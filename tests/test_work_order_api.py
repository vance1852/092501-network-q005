"""工单流转 HTTP 接口的状态码、并发裁决与重启查询测试。"""
from __future__ import annotations
import json, os, tempfile, threading, unittest
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from urllib.parse import quote

from urban_network import api as api_module
from urban_network.models import Reading, Segment
from urban_network.service import NetworkService


def _request(host, port, method, path, token=None, payload=None):
    conn = HTTPConnection(host, port, timeout=10)
    headers = {"Content-Type": "application/json"}
    if token: headers["Authorization"] = f"Bearer {token}"
    body = json.dumps(payload).encode() if payload is not None else None
    conn.request(method, path, body=body, headers=headers)
    response = conn.getresponse()
    data = json.loads(response.read().decode() or "{}")
    conn.close()
    return response.status, data


class WorkOrderHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.path = os.path.join(cls.directory.name, "network.sqlite3")
        cls.host = "127.0.0.1"
        service = NetworkService(cls.path); service.bootstrap()
        cls.admin = service.auth.login("admin", "network-admin")
        service.register_segment(cls.admin, Segment("S1", "east", "gas", 100, 4))
        alert = service.ingest_reading(
            cls.admin, Reading("R1", "S1", "sensor", 160, 230, 88, "2026-01-01T00:00:00+00:00")
        )["alert_id"]
        cls.alert_id = alert
        service.db.close()

        api_module.Handler.service = NetworkService(cls.path)
        cls.server = ThreadingHTTPServer((cls.host, 0), api_module.Handler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        # 每个连接独立 token（会话存于数据库，可跨连接共享）。
        status, data = _request(cls.host, cls.port, "POST", "/login",
                                payload={"user_id": "admin", "password": "network-admin"})
        cls.token = data["token"]

    def setUp(self):
        # 每个用例使用独立工单，避免用例间的版本状态相互干扰。
        status, order = _request(self.host, self.port, "POST", "/segments/S1/work-orders",
                                 token=self.token,
                                 payload={"alert_id": self.alert_id, "assignee": "crew"})
        self.assertEqual(status, 201)
        self.wid = order["work_order_id"]

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown(); cls.server.server_close(); cls.thread.join()
        cls.directory.cleanup()

    def _transition(self, target, reason, expected_version):
        return _request(self.host, self.port, "POST", f"/work-orders/{self.wid}/transitions",
                        token=self.token,
                        payload={"target": target, "reason": reason, "expected_version": expected_version})

    def test_health(self):
        status, data = _request(self.host, self.port, "GET", "/health")
        self.assertEqual(status, 200); self.assertEqual(data["status"], "ok")

    def test_work_order_exposes_version(self):
        status, data = _request(self.host, self.port, "GET", f"/work-orders/{self.wid}", token=self.token)
        self.assertEqual(status, 200); self.assertEqual(data["version"], 1)

    def test_transition_returns_200_and_conflict_returns_409(self):
        status, data = self._transition("assigned", "crew accepts", 1)
        self.assertEqual(status, 200)
        self.assertEqual(data["outcome"], "applied")
        self.assertEqual(data["resulting_version"], 2)
        status, data = self._transition("cancelled", "stale cancel", 1)
        self.assertEqual(status, 409)
        self.assertEqual(data["error"]["code"], "conflict")
        self.assertEqual(data["decision"]["outcome"], "conflicted")
        self.assertEqual(data["decision"]["winner"]["target"], "assigned")

    def test_identical_retry_replays_original_409(self):
        self.assertEqual(self._transition("assigned", "assigned once", 1)[0], 200)
        s1, d1 = self._transition("cancelled", "same stale cancel", 1)
        s2, d2 = self._transition("cancelled", "same stale cancel", 1)
        self.assertEqual((s1, s2), (409, 409))
        self.assertEqual(d1["decision"]["decision_id"], d2["decision"]["decision_id"])

    def test_missing_expected_version_is_422_and_unknown_order_is_404(self):
        status, data = _request(self.host, self.port, "POST", f"/work-orders/{self.wid}/transitions",
                                token=self.token, payload={"target": "assigned", "reason": "x"})
        self.assertEqual(status, 422); self.assertEqual(data["error"]["code"], "validation_failed")
        status, data = _request(self.host, self.port, "POST", "/work-orders/missing/transitions",
                                token=self.token, payload={"target": "assigned", "reason": "x", "expected_version": 1})
        self.assertEqual(status, 404); self.assertEqual(data["error"]["code"], "not_found")

    def test_decisions_endpoint_lists_both_sides(self):
        self.assertEqual(self._transition("assigned", "assigned once", 1)[0], 200)
        self.assertEqual(self._transition("cancelled", "stale cancel", 1)[0], 409)
        status, data = _request(self.host, self.port, "GET", f"/work-orders/{self.wid}/decisions", token=self.token)
        self.assertEqual(status, 200)
        outcomes = [d["outcome"] for d in data["decisions"]]
        self.assertIn("applied", outcomes)
        self.assertEqual(outcomes.count("applied"), 1)
        self.assertEqual(outcomes.count("conflicted"), 1)

    def test_concurrent_identical_version_requests_have_consistent_codes(self):
        # 新建工单并推进到 in_progress，随后两支抢修队基于同一版本并发提交
        # completed 与 blocked（信号恢复后同时回传的真实场景）。
        alert_id = api_module.Handler.service.work_order(self.token, self.wid)["alert_id"]
        status, order = _request(self.host, self.port, "POST", "/segments/S1/work-orders", token=self.token,
                                 payload={"alert_id": alert_id, "assignee": "crew-2"})
        self.assertEqual(status, 201)
        wid = order["work_order_id"]
        for target, reason, version in (("assigned", "a", 1), ("in_progress", "b", 2)):
            status, _ = self._request_to(wid, target, reason, version)
            self.assertEqual(status, 200)
        results = []
        barrier = threading.Barrier(2)

        def fire(target, reason):
            barrier.wait()
            results.append(self._request_to(wid, target, reason, 3))

        threads = [threading.Thread(target=fire, args=("completed", "crew A finished")),
                   threading.Thread(target=fire, args=("blocked", "crew B blocked"))]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        statuses = sorted(status for status, _ in results)
        self.assertEqual(statuses, [200, 409])
        applied = next(d for s, d in results if s == 200)
        conflict = next(d for s, d in results if s == 409)
        winning_target = applied["work_order"]["status"]
        self.assertEqual(conflict["decision"]["winner"]["target"], winning_target)
        self.assertIn(winning_target, {"completed", "blocked"})
        # 双方提交都能被调度员看到（按落库顺序）。
        _, data = _request(self.host, self.port, "GET", f"/work-orders/{wid}/decisions", token=self.token)
        final_pair = sorted(d["target"] for d in data["decisions"] if d["expected_version"] == 3)
        self.assertEqual(final_pair, ["blocked", "completed"])

    def _request_to(self, wid, target, reason, expected_version):
        return _request(self.host, self.port, "POST", f"/work-orders/{quote(wid)}/transitions",
                        token=self.token,
                        payload={"target": target, "reason": reason, "expected_version": expected_version})


if __name__ == "__main__":
    unittest.main()
