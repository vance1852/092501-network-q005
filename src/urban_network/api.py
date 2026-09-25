"""依赖标准库的 JSON HTTP API。"""
from __future__ import annotations
import argparse,json
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from .errors import ServiceError
from .models import Reading,Segment
from .service import NetworkService
class Handler(BaseHTTPRequestHandler):
    service=NetworkService()
    def _send(self,status,payload):
        data=json.dumps(payload,ensure_ascii=False).encode(); self.send_response(status); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(data))); self.end_headers(); self.wfile.write(data)
    def _error(self,exc):
        if isinstance(exc,ServiceError):return self._send(exc.status,{"error":{"code":exc.code,"message":str(exc)},**exc.details})
        if isinstance(exc,PermissionError):return self._send(403,{"error":{"code":"forbidden","message":str(exc)}})
        if isinstance(exc,ValueError):return self._send(422,{"error":{"code":"validation_failed","message":str(exc)}})
        return self._send(400,{"error":{"code":"bad_request","message":str(exc)}})
    def _token(self):return self.headers.get("Authorization","").removeprefix("Bearer ")
    def log_message(self,*args):return
    def do_GET(self):
        try:
            if self.path=="/health":return self._send(200,{"status":"ok","service":"urban-network"})
            parts=self.path.split("/")
            if self.path.startswith("/segments/") and self.path.endswith("/risk"):return self._send(200,self.service.risk_report(self._token(),parts[2]))
            if self.path.startswith("/segments/"):return self._send(200,self.service.segment(self._token(),self.path.split("/",2)[2]))
            if len(parts)==3 and parts[1]=="work-orders":return self._send(200,self.service.work_order(self._token(),parts[2]))
            if len(parts)==4 and parts[1]=="work-orders" and parts[3]=="conflicts":return self._send(200,self.service.work_order_conflicts(self._token(),parts[2]))
            return self._send(404,{"error":{"code":"route_not_found","message":"not found"}})
        except Exception as e:return self._error(e)
    def do_POST(self):
        try:
            body=json.loads(self.rfile.read(int(self.headers.get("Content-Length","0"))) or b"{}")
            if self.path=="/login":return self._send(200,{"token":self.service.auth.login(body["user_id"],body["password"])})
            token=self._token(); parts=self.path.split("/")
            if self.path=="/segments":return self._send(201,self.service.register_segment(token,Segment(body["segment_id"],body["district"],body["network_type"],body["length_m"],body["criticality"])))
            if self.path.startswith("/segments/") and self.path.endswith("/readings"):
                sid=parts[2]; r=Reading(body["reading_id"],sid,body["sensor_id"],body["pressure_kpa"],body["flow_lps"],body["acoustic_db"],body["observed_at"]); return self._send(201,self.service.ingest_reading(token,r))
            if self.path.startswith("/segments/") and self.path.endswith("/work-orders"):
                return self._send(201,self.service.create_work_order(token,parts[2],body["alert_id"],body["assignee"],body.get("priority",3)))
            if len(parts)==4 and parts[1]=="work-orders" and parts[3]=="transitions":
                return self._send(200,self.service.transition_work_order(token,parts[2],body.get("target"),body.get("reason"),body.get("expected_version"),body.get("request_id")))
            return self._send(404,{"error":{"code":"route_not_found","message":"not found"}})
        except Exception as e:return self._error(e)
def main():
    p=argparse.ArgumentParser(); p.add_argument("--database",default=":memory:"); p.add_argument("--host",default="127.0.0.1"); p.add_argument("--port",type=int,default=8080); a=p.parse_args(); Handler.service=NetworkService(a.database); Handler.service.bootstrap(); ThreadingHTTPServer((a.host,a.port),Handler).serve_forever()
if __name__=="__main__":main()
