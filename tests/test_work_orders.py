"""工单版本条件、幂等重试、冲突留痕与并发/重启一致性测试。"""
from __future__ import annotations
import os, tempfile, threading, unittest
from urban_network.errors import Conflict, ValidationFailed
from urban_network.models import Reading, Segment
from urban_network.service import NetworkService


def _open_work_order(service, token, segment="S1", reading="R1", assignee="crew"):
    service.register_segment(token, Segment(segment, "east", "gas", 100, 4))
    alert = service.ingest_reading(
        token, Reading(reading, segment, "sensor", 160, 230, 88, "2026-01-01T00:00:00+00:00")
    )["alert_id"]
    return service.create_work_order(token, segment, alert, assignee)


class WorkOrderVersionTests(unittest.TestCase):
    def setUp(self):
        self.service = NetworkService(); self.service.bootstrap()
        self.token = self.service.auth.login("admin", "network-admin")
        self.order = _open_work_order(self.service, self.token)
        self.wid = self.order["work_order_id"]

    def test_version_starts_at_one_and_applied_transition_bumps_it(self):
        self.assertEqual(self.order["version"], 1)
        result = self.service.transition_work_order(self.token, self.wid, "assigned", "accept", 1)
        self.assertEqual(result["outcome"], "applied")
        self.assertEqual(result["resulting_version"], 2)
        self.assertEqual(result["work_order"]["version"], 2)
        self.assertEqual(result["work_order"]["status"], "assigned")

    def test_stale_version_is_conflict_and_does_not_mutate_state(self):
        self.service.transition_work_order(self.token, self.wid, "assigned", "accept", 1)
        with self.assertRaises(Conflict) as caught:
            self.service.transition_work_order(self.token, self.wid, "cancelled", "stale", 1)
        body = caught.exception.body
        self.assertEqual(body["outcome"], "conflicted")
        self.assertEqual(body["current_version"], 2)
        self.assertEqual(body["current_status"], "assigned")
        self.assertEqual(body["winner"]["target"], "assigned")
        self.assertEqual(body["submission"]["target"], "cancelled")
        order = self.service.work_order(self.token, self.wid)
        self.assertEqual((order["status"], order["version"]), ("assigned", 2))

    def test_same_previous_version_succeeds_at_most_once(self):
        first = self.service.transition_work_order(self.token, self.wid, "assigned", "accept", 1)
        self.assertEqual(first["outcome"], "applied")
        for target in ("assigned", "cancelled"):
            with self.assertRaises(Conflict):
                self.service.transition_work_order(self.token, self.wid, target, "again", 1)
        self.assertEqual(self.service.work_order(self.token, self.wid)["version"], 2)

    def test_identical_retry_returns_original_applied_decision(self):
        first = self.service.transition_work_order(self.token, self.wid, "assigned", "accept", 1)
        self.assertFalse(first["replayed"])
        second = self.service.transition_work_order(self.token, self.wid, "assigned", "accept", 1)
        self.assertEqual(second["decision_id"], first["decision_id"])
        self.assertTrue(second["replayed"])
        self.assertEqual(second["resulting_version"], 2)
        self.assertEqual(second["work_order"]["version"], 2)
        rows = self.service.work_order_decisions(self.token, self.wid)
        self.assertEqual([r["outcome"] for r in rows].count("applied"), 1)

    def test_identical_retry_of_conflict_replays_same_conflict(self):
        self.service.transition_work_order(self.token, self.wid, "assigned", "accept", 1)
        with self.assertRaises(Conflict) as first:
            self.service.transition_work_order(self.token, self.wid, "cancelled", "stale", 1)
        with self.assertRaises(Conflict) as second:
            self.service.transition_work_order(self.token, self.wid, "cancelled", "stale", 1)
        self.assertEqual(second.exception.body["decision_id"], first.exception.body["decision_id"])
        rows = self.service.work_order_decisions(self.token, self.wid)
        # 重试不产生新的裁决行，也不产生新的审计事件。
        self.assertEqual(len(rows), 2)
        events = self.service.audit_events(self.token, "work_order", self.wid)
        self.assertEqual([e["action"] for e in events].count("transition_conflicted"), 1)

    def test_different_submission_on_same_version_is_separate_conflict(self):
        self.service.transition_work_order(self.token, self.wid, "assigned", "accept", 1)
        with self.assertRaises(Conflict):
            self.service.transition_work_order(self.token, self.wid, "cancelled", "reason one", 1)
        with self.assertRaises(Conflict):
            self.service.transition_work_order(self.token, self.wid, "cancelled", "reason two", 1)
        rows = self.service.work_order_decisions(self.token, self.wid)
        self.assertEqual(len([r for r in rows if r["outcome"] == "conflicted"]), 2)

    def test_completed_order_cannot_be_reopened_by_old_request(self):
        self.service.transition_work_order(self.token, self.wid, "assigned", "go", 1)
        self.service.transition_work_order(self.token, self.wid, "in_progress", "go", 2)
        self.service.transition_work_order(self.token, self.wid, "completed", "done", 3)
        with self.assertRaises(Conflict) as caught:
            self.service.transition_work_order(self.token, self.wid, "blocked", "late block", 3)
        self.assertIn("completed", caught.exception.body["detail"])
        self.assertEqual(self.service.work_order(self.token, self.wid)["status"], "completed")

    def test_cancelled_order_cannot_be_reopened(self):
        self.service.transition_work_order(self.token, self.wid, "cancelled", "abort", 1)
        with self.assertRaises(Conflict) as caught:
            self.service.transition_work_order(self.token, self.wid, "assigned", "late", 1)
        self.assertIn("cancelled", caught.exception.body["detail"])
        self.assertEqual(self.service.work_order(self.token, self.wid)["status"], "cancelled")

    def test_invalid_expected_version_is_rejected_before_lookup(self):
        for bad in (0, -1, "1", 1.0, True, None):
            with self.assertRaises(ValidationFailed):
                self.service.transition_work_order(self.token, self.wid, "assigned", "x", bad)

    def test_audit_events_follow_apply_and_conflict_order(self):
        self.service.transition_work_order(self.token, self.wid, "assigned", "accept", 1)
        with self.assertRaises(Conflict):
            self.service.transition_work_order(self.token, self.wid, "cancelled", "stale", 1)
        actions = [e["action"] for e in self.service.audit_events(self.token, "work_order", self.wid)]
        self.assertEqual(actions, ["created", "transition_applied", "transition_conflicted"])
        # 审计事件严格按 event_id 单调排列（重启后仍可核对）。
        events = self.service.audit_events(self.token, "work_order", self.wid)
        ids = [e["event_id"] for e in events]
        self.assertEqual(ids, sorted(ids))

    def test_decisions_keep_both_sides_for_dispatcher(self):
        self.service.transition_work_order(self.token, self.wid, "assigned", "crew A accepts", 1)
        with self.assertRaises(Conflict):
            self.service.transition_work_order(self.token, self.wid, "cancelled", "crew B blocks", 1)
        decisions = self.service.work_order_decisions(self.token, self.wid)
        self.assertEqual([d["outcome"] for d in decisions], ["applied", "conflicted"])
        winner, loser = decisions
        self.assertEqual(winner["target"], "assigned")
        self.assertEqual(loser["target"], "cancelled")
        self.assertEqual(loser["reason"], "crew B blocks")
        self.assertIsNone(loser["resulting_version"])
        self.assertEqual(loser["from_version"], 2)


class WorkOrderConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.directory.name, "network.sqlite3")
        first = NetworkService(self.path); first.bootstrap()
        token = first.auth.login("admin", "network-admin")
        self.order = _open_work_order(first, token)
        self.wid = self.order["work_order_id"]
        first.db.close()

    def tearDown(self):
        self.directory.cleanup()

    def test_parallel_same_version_requests_have_single_winner(self):
        services = [NetworkService(self.path) for _ in range(6)]
        tokens = [s.auth.login("admin", "network-admin") for s in services]
        outcomes = []
        barrier = threading.Barrier(len(services))

        def fire(index, service, token):
            barrier.wait()
            try:
                result = service.transition_work_order(token, self.wid, "assigned", f"crew {index}", 1)
                outcomes.append(("applied", result["decision_id"]))
            except Conflict as exc:
                outcomes.append(("conflict", exc.body["detail"]))

        threads = [threading.Thread(target=fire, args=(i, s, t)) for i, (s, t) in enumerate(zip(services, tokens))]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        for s in services: s.db.close()

        self.assertEqual(len([o for o in outcomes if o[0] == "applied"]), 1)
        self.assertEqual(len([o for o in outcomes if o[0] == "conflict"]), 5)
        winner = NetworkService(self.path)
        wt = winner.auth.login("admin", "network-admin")
        order = winner.work_order(wt, self.wid)
        self.assertEqual((order["status"], order["version"]), ("assigned", 2))
        applied = [d for d in winner.work_order_decisions(wt, self.wid) if d["outcome"] == "applied"]
        self.assertEqual(len(applied), 1)
        self.assertEqual(len(winner.work_order_decisions(wt, self.wid)), 6)
        winner.db.close()

    def test_identical_concurrent_submissions_collapse_to_one_decision(self):
        # 同一抢修队断线重连后，多个连接并发提交完全相同的内容：全部命中同一条原决定。
        count = 8
        services = [NetworkService(self.path) for _ in range(count)]
        tokens = [s.auth.login("admin", "network-admin") for s in services]
        outcomes, errors = [], []
        barrier = threading.Barrier(count)

        def fire(index, service, token):
            barrier.wait()
            try:
                result = service.transition_work_order(token, self.wid, "assigned", "same reason", 1)
                outcomes.append(("applied", result["decision_id"]))
            except Conflict as exc:
                outcomes.append(("conflict", exc.body["decision_id"]))
            except Exception as exc:  # noqa: BLE001 - 任何异常都视为实现缺陷
                errors.append(repr(exc))

        threads = [threading.Thread(target=fire, args=(i, s, t)) for i, (s, t) in enumerate(zip(services, tokens))]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        for s in services: s.db.close()

        self.assertEqual(errors, [])
        # 完全相同的并发提交共享同一条 applied 决定，版本只前进一次。
        self.assertEqual({decision_id for _, decision_id in outcomes}, {outcomes[0][1]})
        self.assertTrue(all(kind == "applied" for kind, _ in outcomes))
        check = NetworkService(self.path)
        token = check.auth.login("admin", "network-admin")
        self.assertEqual(check.work_order(token, self.wid)["version"], 2)
        self.assertEqual(len(check.work_order_decisions(token, self.wid)), 1)
        check.db.close()

    def test_conflict_retry_across_connections_replays_original(self):
        service = NetworkService(self.path)
        token = service.auth.login("admin", "network-admin")
        service.transition_work_order(token, self.wid, "assigned", "accept", 1)
        with self.assertRaises(Conflict) as first:
            service.transition_work_order(token, self.wid, "cancelled", "stale", 1)
        service.db.close()

        # 换一个连接（模拟重连）提交完全相同的请求，返回原冲突决定且不新增行。
        other = NetworkService(self.path)
        token = other.auth.login("admin", "network-admin")
        with self.assertRaises(Conflict) as second:
            other.transition_work_order(token, self.wid, "cancelled", "stale", 1)
        self.assertEqual(second.exception.body["decision_id"], first.exception.body["decision_id"])
        self.assertEqual(len(other.work_order_decisions(token, self.wid)), 2)
        other.db.close()

    def test_state_and_audit_survive_restart(self):
        service = NetworkService(self.path)
        token = service.auth.login("admin", "network-admin")
        service.transition_work_order(token, self.wid, "assigned", "accept", 1)
        with self.assertRaises(Conflict):
            service.transition_work_order(token, self.wid, "cancelled", "stale", 1)
        service.db.close()

        restarted = NetworkService(self.path)
        token = restarted.auth.login("admin", "network-admin")
        order = restarted.work_order(token, self.wid)
        self.assertEqual((order["status"], order["version"]), ("assigned", 2))
        # 重启后旧请求依旧不能重开，且返回的是原决定。
        with self.assertRaises(Conflict) as caught:
            restarted.transition_work_order(token, self.wid, "cancelled", "stale", 1)
        self.assertIn("current version is 2", caught.exception.body["detail"])
        decisions = restarted.work_order_decisions(token, self.wid)
        self.assertEqual([d["outcome"] for d in decisions], ["applied", "conflicted"])
        actions = [e["action"] for e in restarted.audit_events(token, "work_order", self.wid)]
        self.assertEqual(actions, ["created", "transition_applied", "transition_conflicted"])
        restarted.db.close()


if __name__ == "__main__":
    unittest.main()
