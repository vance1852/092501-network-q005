"""依赖标准库的 JSON HTTP API。"""
from __future__ import annotations
import argparse,json
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from .errors import NetworkError
from .models import Reading,Segment
from .service import NetworkService
class Handler(BaseHTTPRequestHandler):
    service=NetworkService()
    def log_message(self,format,*args):return
    def _send(self,status,payload):
        data=json.dumps(payload,ensure_ascii=False).encode(); self.send_response(status); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(data))); self.end_headers(); self.wfile.write(data)
    def _token(self):return self.headers.get("Authorization","").removeprefix("Bearer ")
    def _body(self):
        raw=self.rfile.read(int(self.headers.get("Content-Length","0"))) or b"{}"
        try: value=json.loads(raw)
        except json.JSONDecodeError as exc: raise NetworkError("request body must be a JSON object") from exc
        if not isinstance(value,dict): raise NetworkError("request body must be a JSON object")
        return value
    def _fail(self,exc):
        if isinstance(exc,NetworkError):
            body={"error":{"code":exc.code,"message":str(exc)}}
            if exc.body: body["decision"]=exc.body
            return self._send(exc.status,body)
        if isinstance(exc,PermissionError):return self._send(403,{"error":{"code":"forbidden","message":str(exc)}})
        if isinstance(exc,KeyError):return self._send(404,{"error":{"code":"not_found","message":str(exc).strip('"')}})
        return self._send(400,{"error":{"code":"bad_request","message":str(exc)}})
    def do_GET(self):
        try:
            if self.path=="/health":return self._send(200,{"status":"ok","service":"urban-network"})
            if self.path.startswith("/segments/") and self.path.endswith("/risk"):return self._send(200,self.service.risk_report(self._token(),self.path.split("/")[2]))
            if self.path.startswith("/segments/"):return self._send(200,self.service.segment(self._token(),self.path.split("/",2)[2]))
            if self.path.startswith("/work-orders/") and self.path.endswith("/decisions"):return self._send(200,{"decisions":self.service.work_order_decisions(self._token(),self.path.split("/")[2])})
            if self.path.startswith("/work-orders/"):return self._send(200,self.service.work_order(self._token(),self.path.split("/",2)[2]))
            return self._send(404,{"error":{"code":"not_found","message":"route not found"}})
        except Exception as e:return self._fail(e)
    def do_POST(self):
        try:
            body=self._body()
            if self.path=="/login":return self._send(200,{"token":self.service.auth.login(body["user_id"],body["password"])})
            token=self._token()
            if self.path=="/segments":return self._send(201,self.service.register_segment(token,Segment(body["segment_id"],body["district"],body["network_type"],body["length_m"],body["criticality"])))
            if self.path.startswith("/segments/") and self.path.endswith("/readings"):
                sid=self.path.split("/")[2]; r=Reading(body["reading_id"],sid,body["sensor_id"],body["pressure_kpa"],body["flow_lps"],body["acoustic_db"],body["observed_at"]); return self._send(201,self.service.ingest_reading(token,r))
            if self.path.startswith("/segments/") and self.path.endswith("/work-orders"):
                return self._send(201,self.service.create_work_order(token,self.path.split("/")[2],body["alert_id"],body["assignee"],body.get("priority",3)))
            if self.path.startswith("/work-orders/") and self.path.endswith("/transitions"):
                if "expected_version" not in body:return self._send(422,{"error":{"code":"validation_failed","message":"expected_version is required"}})
                return self._send(200,self.service.transition_work_order(token,self.path.split("/")[2],body.get("target",""),body.get("reason",""),body["expected_version"]))
            return self._send(404,{"error":{"code":"not_found","message":"route not found"}})
        except Exception as e:return self._fail(e)
def main():
    p=argparse.ArgumentParser(); p.add_argument("--database",default=":memory:"); p.add_argument("--host",default="127.0.0.1"); p.add_argument("--port",type=int,default=8080); a=p.parse_args(); Handler.service=NetworkService(a.database); Handler.service.bootstrap(); ThreadingHTTPServer((a.host,a.port),Handler).serve_forever()
if __name__=="__main__":main()
