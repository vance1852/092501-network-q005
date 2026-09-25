"""工单流转的版本条件、幂等重试、冲突留痕、并发与重启一致性测试。"""
from __future__ import annotations

import http.client
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from urban_network.api import Handler
from urban_network.errors import Conflict, InvalidState, NotFound, ValidationFailed
from urban_network.models import Reading, Segment
from urban_network.service import NetworkService
from http.server import ThreadingHTTPServer

LEGACY_SCHEMA = """
CREATE TABLE users(user_id TEXT PRIMARY KEY,role TEXT NOT NULL,salt TEXT NOT NULL,password_hash TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 1,created_at TEXT NOT NULL);
CREATE TABLE sessions(token TEXT PRIMARY KEY,user_id TEXT NOT NULL,expires_at TEXT NOT NULL,active INTEGER NOT NULL DEFAULT 1);
CREATE TABLE segments(segment_id TEXT PRIMARY KEY,district TEXT NOT NULL,network_type TEXT NOT NULL,length_m REAL NOT NULL,criticality INTEGER NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE readings(reading_id TEXT PRIMARY KEY,segment_id TEXT NOT NULL REFERENCES segments(segment_id),sensor_id TEXT NOT NULL,pressure_kpa REAL NOT NULL,flow_lps REAL NOT NULL,acoustic_db REAL NOT NULL,observed_at TEXT NOT NULL,UNIQUE(segment_id,sensor_id,observed_at));
CREATE TABLE alerts(alert_id TEXT PRIMARY KEY,segment_id TEXT NOT NULL REFERENCES segments(segment_id),fingerprint TEXT NOT NULL UNIQUE,severity TEXT NOT NULL,score REAL NOT NULL,status TEXT NOT NULL,created_at TEXT NOT NULL,resolved_at TEXT);
CREATE TABLE work_orders(work_order_id TEXT PRIMARY KEY,segment_id TEXT NOT NULL,alert_id TEXT NOT NULL,assignee TEXT NOT NULL,status TEXT NOT NULL,priority INTEGER NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE resources(resource_id TEXT PRIMARY KEY,kind TEXT NOT NULL,district TEXT NOT NULL,capacity INTEGER NOT NULL,available INTEGER NOT NULL);
CREATE TABLE allocations(allocation_id TEXT PRIMARY KEY,resource_id TEXT NOT NULL,work_order_id TEXT NOT NULL,quantity INTEGER NOT NULL,created_at TEXT NOT NULL,UNIQUE(resource_id,work_order_id));
CREATE TABLE audit_events(event_id INTEGER PRIMARY KEY AUTOINCREMENT,entity_type TEXT NOT NULL,entity_id TEXT NOT NULL,action TEXT NOT NULL,actor TEXT NOT NULL,payload TEXT NOT NULL,created_at TEXT NOT NULL);
"""


class TransitionTestBase(unittest.TestCase):
    """在文件数据库上准备一个 in_progress、version=3 的工单。"""

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.db_path = str(Path(self.directory.name) / "network.sqlite3")
        self.service = NetworkService(self.db_path)
        self.service.bootstrap()
        self.token = self.service.auth.login("admin", "network-admin")
        self.service.register_segment(self.token, Segment("S1", "east", "drainage", 100, 4))
        reading = self.service.ingest_reading(
            self.token, Reading("R1", "S1", "sensor", 100, 250, 90, "2026-01-01T00:00:00+00:00")
        )
        self.work_order_id = self.service.create_work_order(self.token, "S1", reading["alert_id"], "crew")["work_order_id"]
        self.service.transition_work_order(self.token, self.work_order_id, "assigned", "crew accepted", 1)
        self.service.transition_work_order(self.token, self.work_order_id, "in_progress", "crew on site", 2)

    def events(self, service=None):
        return (service or self.service).audit_events(self.token, "work_order", self.work_order_id)


class VersionConditionTests(TransitionTestBase):
    def test_same_predecessor_version_succeeds_at_most_once(self):
        applied = self.service.transition_work_order(
            self.token, self.work_order_id, "completed", "修复完成", 3, request_id="crew-a-1"
        )
        self.assertFalse(applied["replayed"])
        self.assertEqual(applied["status"], "completed")
        self.assertEqual(applied["version"], 4)
        with self.assertRaises(Conflict) as raised:
            self.service.transition_work_order(
                self.token, self.work_order_id, "blocked", "缺少配件", 3, request_id="crew-b-1"
            )
        conflict = raised.exception.details["conflict"]
        self.assertEqual(conflict["expected_version"], 3)
        self.assertEqual(conflict["current_version"], 4)
        self.assertEqual(conflict["current_status"], "completed")
        self.assertEqual(conflict["submission"]["target"], "blocked")
        self.assertEqual(conflict["submission"]["request_id"], "crew-b-1")
        self.assertEqual(conflict["winning"]["to_status"], "completed")
        self.assertEqual(conflict["winning"]["request_id"], "crew-a-1")
        order = self.service.work_order(self.token, self.work_order_id)
        self.assertEqual((order["status"], order["version"]), ("completed", 4))

    def test_conflict_preserves_both_submissions_for_dispatcher(self):
        self.service.transition_work_order(self.token, self.work_order_id, "completed", "修复完成", 3, request_id="crew-a-1")
        with self.assertRaises(Conflict):
            self.service.transition_work_order(self.token, self.work_order_id, "blocked", "缺少配件", 3, request_id="crew-b-1")
        view = self.service.work_order_conflicts(self.token, self.work_order_id)
        self.assertEqual(view["status"], "completed")
        self.assertEqual(len(view["transitions"]), 3)
        self.assertEqual(len(view["conflicts"]), 1)
        conflict = view["conflicts"][0]
        self.assertEqual(conflict["submission"]["reason"], "缺少配件")
        self.assertEqual(conflict["winning"]["reason"], "修复完成")
        self.assertEqual(conflict["winning"]["transition_id"], view["transitions"][-1]["transition_id"])

    def test_identical_retry_returns_original_decision(self):
        first = self.service.transition_work_order(
            self.token, self.work_order_id, "completed", "修复完成", 3, request_id="crew-a-1"
        )
        second = self.service.transition_work_order(
            self.token, self.work_order_id, "completed", "修复完成", 3, request_id="crew-a-1"
        )
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(second["transition"]["transition_id"], first["transition"]["transition_id"])
        self.assertEqual(second["version"], first["version"])
        actions = [event["action"] for event in self.events()]
        self.assertEqual(actions, ["created", "transition", "transition", "transition"])
        view = self.service.work_order_conflicts(self.token, self.work_order_id)
        self.assertEqual(len(view["transitions"]), 3)
        self.assertEqual(view["conflicts"], [])

    def test_identical_retry_without_request_id_is_replayed(self):
        first = self.service.transition_work_order(self.token, self.work_order_id, "blocked", "等待配件", 3)
        retry = self.service.transition_work_order(self.token, self.work_order_id, "blocked", "等待配件", 3)
        self.assertTrue(retry["replayed"])
        self.assertEqual(retry["transition"]["transition_id"], first["transition"]["transition_id"])

    def test_conflicted_retry_returns_same_conflict_decision(self):
        self.service.transition_work_order(self.token, self.work_order_id, "completed", "修复完成", 3, request_id="crew-a-1")
        with self.assertRaises(Conflict) as first:
            self.service.transition_work_order(self.token, self.work_order_id, "blocked", "缺少配件", 3, request_id="crew-b-1")
        with self.assertRaises(Conflict) as second:
            self.service.transition_work_order(self.token, self.work_order_id, "blocked", "缺少配件", 3, request_id="crew-b-1")
        self.assertEqual(
            first.exception.details["conflict"]["conflict_id"],
            second.exception.details["conflict"]["conflict_id"],
        )
        view = self.service.work_order_conflicts(self.token, self.work_order_id)
        self.assertEqual(len(view["conflicts"]), 1)
        actions = [event["action"] for event in self.events()]
        self.assertEqual(actions.count("transition_conflict"), 1)

    def test_request_id_reuse_with_different_content_conflicts(self):
        self.service.transition_work_order(self.token, self.work_order_id, "blocked", "等待配件", 3, request_id="crew-a-1")
        with self.assertRaises(Conflict) as raised:
            self.service.transition_work_order(self.token, self.work_order_id, "completed", "修复完成", 3, request_id="crew-a-1")
        self.assertIn("请求标识", str(raised.exception))
        order = self.service.work_order(self.token, self.work_order_id)
        self.assertEqual((order["status"], order["version"]), ("blocked", 4))

    def test_completed_order_cannot_be_reopened(self):
        self.service.transition_work_order(self.token, self.work_order_id, "completed", "修复完成", 3)
        with self.assertRaises(Conflict):
            self.service.transition_work_order(self.token, self.work_order_id, "blocked", "旧离线页面提交", 3)
        current = self.service.work_order(self.token, self.work_order_id)
        with self.assertRaises(InvalidState):
            self.service.transition_work_order(self.token, self.work_order_id, "in_progress", "试图重开", current["version"])
        order = self.service.work_order(self.token, self.work_order_id)
        self.assertEqual(order["status"], "completed")

    def test_cancelled_order_cannot_be_reopened(self):
        reading = self.service.ingest_reading(
            self.token, Reading("R2", "S1", "sensor", 100, 250, 90, "2026-01-02T00:00:00+00:00")
        )
        order = self.service.create_work_order(self.token, "S1", reading["alert_id"], "crew")
        self.service.transition_work_order(self.token, order["work_order_id"], "cancelled", "误报", order["version"])
        with self.assertRaises(Conflict):
            self.service.transition_work_order(self.token, order["work_order_id"], "assigned", "离线重试", 1)
        with self.assertRaises(InvalidState):
            self.service.transition_work_order(self.token, order["work_order_id"], "assigned", "试图重开", 2)
        self.assertEqual(self.service.work_order(self.token, order["work_order_id"])["status"], "cancelled")

    def test_validation_errors(self):
        with self.assertRaises(ValidationFailed):
            self.service.transition_work_order(self.token, self.work_order_id, "unknown", "x", 3)
        with self.assertRaises(ValidationFailed):
            self.service.transition_work_order(self.token, self.work_order_id, "completed", " ", 3)
        with self.assertRaises(ValidationFailed):
            self.service.transition_work_order(self.token, self.work_order_id, "completed", "x", None)
        with self.assertRaises(ValidationFailed):
            self.service.transition_work_order(self.token, self.work_order_id, "completed", "x", 0)
        with self.assertRaises(NotFound):
            self.service.transition_work_order(self.token, "wo-missing", "completed", "x", 1)
        with self.assertRaises(InvalidState):
            self.service.transition_work_order(self.token, self.work_order_id, "assigned", "不允许的回退", 3)


class ConcurrencyTests(TransitionTestBase):
    def _race(self, submissions):
        barrier = threading.Barrier(len(submissions))
        outcomes = []

        def submit(target, reason, request_id):
            barrier.wait(timeout=10)
            try:
                result = self.service.transition_work_order(
                    self.token, self.work_order_id, target, reason, 3, request_id=request_id
                )
                outcomes.append(("applied", result))
            except Conflict as exc:
                outcomes.append(("conflict", exc.details["conflict"]))

        threads = [threading.Thread(target=submit, args=item) for item in submissions]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        return outcomes

    def test_concurrent_reports_have_exactly_one_winner(self):
        submissions = [
            ("completed" if index % 2 == 0 else "blocked", f"回传-{index}", f"req-{index}")
            for index in range(8)
        ]
        outcomes = self._race(submissions)
        applied = [payload for kind, payload in outcomes if kind == "applied"]
        conflicts = [payload for kind, payload in outcomes if kind == "conflict"]
        self.assertEqual(len(applied), 1)
        self.assertEqual(len(conflicts), 7)
        order = self.service.work_order(self.token, self.work_order_id)
        self.assertEqual(order["version"], 4)
        self.assertEqual(order["status"], applied[0]["status"])
        view = self.service.work_order_conflicts(self.token, self.work_order_id)
        self.assertEqual(len(view["conflicts"]), 7)
        self.assertEqual(len(view["transitions"]), 3)
        winning_id = view["transitions"][-1]["transition_id"]
        for conflict in view["conflicts"]:
            self.assertEqual(conflict["expected_version"], 3)
            self.assertEqual(conflict["current_version"], 4)
            self.assertEqual(conflict["winning"]["transition_id"], winning_id)
        actions = [event["action"] for event in self.events()]
        self.assertEqual(actions, ["created", "transition", "transition", "transition"] + ["transition_conflict"] * 7)

    def test_concurrent_identical_retries_share_one_decision(self):
        submissions = [("completed", "修复完成", "crew-a-1")] * 4
        outcomes = self._race(submissions)
        self.assertEqual(len(outcomes), 4)
        results = [payload for kind, payload in outcomes if kind == "applied"]
        self.assertEqual(len(results), 4)
        self.assertEqual(sum(1 for result in results if not result["replayed"]), 1)
        self.assertEqual(sum(1 for result in results if result["replayed"]), 3)
        self.assertEqual({result["transition"]["transition_id"] for result in results}, {results[0]["transition"]["transition_id"]})
        order = self.service.work_order(self.token, self.work_order_id)
        self.assertEqual((order["status"], order["version"]), ("completed", 4))
        view = self.service.work_order_conflicts(self.token, self.work_order_id)
        self.assertEqual(len(view["transitions"]), 3)
        self.assertEqual(view["conflicts"], [])


class RestartTests(TransitionTestBase):
    def test_restart_preserves_state_conflicts_and_audit_order(self):
        self.service.transition_work_order(self.token, self.work_order_id, "completed", "修复完成", 3, request_id="crew-a-1")
        with self.assertRaises(Conflict):
            self.service.transition_work_order(self.token, self.work_order_id, "blocked", "缺少配件", 3, request_id="crew-b-1")
        before = self.events()
        self.service.db.close()
        reopened = NetworkService(self.db_path)
        token = reopened.auth.login("admin", "network-admin")
        order = reopened.work_order(token, self.work_order_id)
        self.assertEqual((order["status"], order["version"]), ("completed", 4))
        view = reopened.work_order_conflicts(token, self.work_order_id)
        self.assertEqual(len(view["transitions"]), 3)
        self.assertEqual(len(view["conflicts"]), 1)
        self.assertEqual(view["conflicts"][0]["winning"]["to_status"], "completed")
        self.assertEqual(order["version"], 1 + len(view["transitions"]))
        after = reopened.audit_events(token, "work_order", self.work_order_id)
        self.assertEqual([event["event_id"] for event in after], [event["event_id"] for event in before])
        self.assertEqual(
            [event["action"] for event in after],
            ["created", "transition", "transition", "transition", "transition_conflict"],
        )
        replayed = reopened.transition_work_order(token, self.work_order_id, "completed", "修复完成", 3, request_id="crew-a-1")
        self.assertTrue(replayed["replayed"])
        self.assertEqual(reopened.work_order(token, self.work_order_id)["version"], 4)
        reopened.db.close()

    def test_legacy_database_gains_version_column(self):
        legacy_path = str(Path(self.directory.name) / "legacy.sqlite3")
        legacy = sqlite3.connect(legacy_path)
        legacy.executescript(LEGACY_SCHEMA)
        legacy.execute(
            "INSERT INTO work_orders VALUES(?,?,?,?,?,?,?,?)",
            ("wo-legacy", "S1", "alert-1", "crew", "open", 3, "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00"),
        )
        legacy.commit()
        legacy.close()
        service = NetworkService(legacy_path)
        service.bootstrap()
        token = service.auth.login("admin", "network-admin")
        columns = {row[1] for row in service.db.execute("PRAGMA table_info(work_orders)")}
        self.assertIn("version", columns)
        order = service.work_order(token, "wo-legacy")
        self.assertEqual(order["version"], 1)
        applied = service.transition_work_order(token, "wo-legacy", "assigned", "接管", 1)
        self.assertEqual(applied["version"], 2)
        service.db.close()


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        db_path = str(Path(self.directory.name) / "api.sqlite3")
        self.service = NetworkService(db_path)
        self.service.bootstrap()
        Handler.service = self.service
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.addCleanup(self.server.server_close)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.shutdown)
        self.port = self.server.server_address[1]
        _, body = self.request("POST", "/login", {"user_id": "admin", "password": "network-admin"})
        self.token = body["token"]
        self.request("POST", "/segments", {"segment_id": "S1", "district": "east", "network_type": "drainage", "length_m": 100, "criticality": 4}, self.token)
        status, reading = self.request(
            "POST", "/segments/S1/readings",
            {"reading_id": "R1", "sensor_id": "sensor", "pressure_kpa": 100, "flow_lps": 250, "acoustic_db": 90, "observed_at": "2026-01-01T00:00:00+00:00"},
            self.token,
        )
        self.assertEqual(status, 201)
        status, order = self.request("POST", "/segments/S1/work-orders", {"alert_id": reading["alert_id"], "assignee": "crew"}, self.token)
        self.assertEqual(status, 201)
        self.work_order_id = order["work_order_id"]
        self.request("POST", f"/work-orders/{self.work_order_id}/transitions", {"target": "assigned", "reason": "crew accepted", "expected_version": 1}, self.token)
        self.request("POST", f"/work-orders/{self.work_order_id}/transitions", {"target": "in_progress", "reason": "crew on site", "expected_version": 2}, self.token)

    def request(self, method, path, payload=None, token=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = "Bearer " + token
        connection.request(method, path, None if payload is None else json.dumps(payload), headers)
        response = connection.getresponse()
        body = json.loads(response.read() or b"{}")
        connection.close()
        return response.status, body

    def test_transition_endpoint_status_codes(self):
        status, applied = self.request(
            "POST", f"/work-orders/{self.work_order_id}/transitions",
            {"target": "completed", "reason": "修复完成", "expected_version": 3, "request_id": "crew-a-1"}, self.token,
        )
        self.assertEqual(status, 200)
        self.assertFalse(applied["replayed"])
        self.assertEqual(applied["version"], 4)
        status, replay = self.request(
            "POST", f"/work-orders/{self.work_order_id}/transitions",
            {"target": "completed", "reason": "修复完成", "expected_version": 3, "request_id": "crew-a-1"}, self.token,
        )
        self.assertEqual(status, 200)
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["transition"]["transition_id"], applied["transition"]["transition_id"])
        status, conflict = self.request(
            "POST", f"/work-orders/{self.work_order_id}/transitions",
            {"target": "blocked", "reason": "缺少配件", "expected_version": 3, "request_id": "crew-b-1"}, self.token,
        )
        self.assertEqual(status, 409)
        self.assertEqual(conflict["error"]["code"], "conflict")
        self.assertEqual(conflict["conflict"]["submission"]["target"], "blocked")
        self.assertEqual(conflict["conflict"]["winning"]["to_status"], "completed")
        status, invalid = self.request(
            "POST", f"/work-orders/{self.work_order_id}/transitions",
            {"target": "in_progress", "reason": "试图重开", "expected_version": 4}, self.token,
        )
        self.assertEqual(status, 409)
        self.assertEqual(invalid["error"]["code"], "invalid_state")
        status, missing_version = self.request(
            "POST", f"/work-orders/{self.work_order_id}/transitions", {"target": "blocked", "reason": "x"}, self.token
        )
        self.assertEqual(status, 422)
        self.assertEqual(missing_version["error"]["code"], "validation_failed")
        status, _ = self.request(
            "POST", "/work-orders/wo-missing/transitions", {"target": "blocked", "reason": "x", "expected_version": 1}, self.token
        )
        self.assertEqual(status, 404)
        status, _ = self.request(
            "POST", f"/work-orders/{self.work_order_id}/transitions", {"target": "blocked", "reason": "x", "expected_version": 4}
        )
        self.assertEqual(status, 403)
        status, view = self.request("GET", f"/work-orders/{self.work_order_id}/conflicts", token=self.token)
        self.assertEqual(status, 200)
        self.assertEqual(view["status"], "completed")
        self.assertEqual(len(view["conflicts"]), 1)
        self.assertEqual(view["conflicts"][0]["submission"]["reason"], "缺少配件")
        status, order = self.request("GET", f"/work-orders/{self.work_order_id}", token=self.token)
        self.assertEqual(status, 200)
        self.assertEqual((order["status"], order["version"]), ("completed", 4))

    def test_concurrent_http_transitions_have_one_winner(self):
        barrier = threading.Barrier(2)
        results = []

        def post(target, request_id):
            barrier.wait(timeout=10)
            results.append(self.request(
                "POST", f"/work-orders/{self.work_order_id}/transitions",
                {"target": target, "reason": f"回传-{request_id}", "expected_version": 3, "request_id": request_id},
                self.token,
            ))

        threads = [
            threading.Thread(target=post, args=("completed", "crew-a-1")),
            threading.Thread(target=post, args=("blocked", "crew-b-1")),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        statuses = sorted(status for status, _ in results)
        self.assertEqual(statuses, [200, 409])
        conflict_body = next(body for status, body in results if status == 409)
        self.assertEqual(conflict_body["error"]["code"], "conflict")
        status, view = self.request("GET", f"/work-orders/{self.work_order_id}/conflicts", token=self.token)
        self.assertEqual(status, 200)
        self.assertEqual(view["version"], 4)
        self.assertEqual(len(view["conflicts"]), 1)
        self.assertEqual(len(view["transitions"]), 3)
        events = self.service.audit_events(self.token, "work_order", self.work_order_id)
        self.assertEqual(
            [event["action"] for event in events],
            ["created", "transition", "transition", "transition", "transition_conflict"],
        )


if __name__ == "__main__":
    unittest.main()
