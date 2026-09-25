"""协调管网监测、告警、工单和应急资源分配的应用服务。"""
from __future__ import annotations
import hashlib,json,sqlite3,threading,uuid
from .auth import Auth
from .errors import Conflict,ValidationFailed
from .models import Reading,Segment,as_dict,utcnow
from .risk import leak_probability,score_reading
from .storage import audit,connect,rows,transaction
WORK_ORDER_TRANSITIONS={"open":{"assigned","cancelled"},"assigned":{"in_progress","cancelled"},"in_progress":{"completed","blocked"},"blocked":{"in_progress","cancelled"},"completed":set(),"cancelled":set()}
TERMINAL_STATUSES={"completed","cancelled"}
def _decision_digest(work_order_id,actor,expected_version,target,reason):
    return hashlib.sha256(f"{work_order_id}|{actor}|{expected_version}|{target}|{reason}".encode()).hexdigest()
class NetworkService:
    def __init__(self,database=":memory:"): self.db=connect(database); self.auth=Auth(self.db); self._lock=threading.RLock()
    def bootstrap(self):
        for uid,pwd,role in (("admin","network-admin","admin"),("operator","network-operator","operator")):
            try:self.auth.create_user(uid,pwd,role)
            except Exception:pass
    def register_segment(self,token,segment):
        actor=self.auth.require(token,"admin"); segment.validate(); now=utcnow()
        with transaction(self.db):
            self.db.execute("INSERT INTO segments VALUES(?,?,?,?,?,?,?,?)",(segment.segment_id,segment.district,segment.network_type,segment.length_m,segment.criticality,segment.status,now,now)); audit(self.db,"segment",segment.segment_id,"created",actor.user_id,as_dict(segment))
        return self.segment(token,segment.segment_id)
    def segment(self,token,segment_id):
        self.auth.require(token,"read"); row=self.db.execute("SELECT * FROM segments WHERE segment_id=?",(segment_id,)).fetchone()
        if not row:raise KeyError(segment_id)
        return dict(row)
    def ingest_reading(self,token,reading):
        actor=self.auth.require(token,"measure"); reading.validate(); seg=self.db.execute("SELECT criticality FROM segments WHERE segment_id=?",(reading.segment_id,)).fetchone()
        if not seg:raise KeyError(reading.segment_id)
        risk=score_reading(reading.pressure_kpa,reading.flow_lps,reading.acoustic_db,seg[0]); fingerprint=hashlib.sha256(f"{reading.segment_id}|{reading.sensor_id}|{reading.observed_at}".encode()).hexdigest()
        with transaction(self.db):
            if self.db.execute("SELECT reading_id FROM readings WHERE reading_id=?",(reading.reading_id,)).fetchone(): return {"reading_id":reading.reading_id,"duplicate":True,"risk":as_dict(risk)}
            self.db.execute("INSERT INTO readings VALUES(?,?,?,?,?,?,?)",(reading.reading_id,reading.segment_id,reading.sensor_id,reading.pressure_kpa,reading.flow_lps,reading.acoustic_db,reading.observed_at)); alert_id=None
            if risk.severity in {"high","critical"}:
                alert_id="alert-"+fingerprint[:18]; self.db.execute("INSERT OR IGNORE INTO alerts VALUES(?,?,?,?,?,?,?,?)",(alert_id,reading.segment_id,fingerprint,risk.severity,risk.score,"open",utcnow(),None))
            audit(self.db,"reading",reading.reading_id,"ingested",actor.user_id,{"risk":as_dict(risk),"alert_id":alert_id})
        return {"reading_id":reading.reading_id,"duplicate":False,"risk":as_dict(risk),"alert_id":alert_id}
    def risk_report(self,token,segment_id):
        self.auth.require(token,"analyze"); readings=rows(self.db,"SELECT * FROM readings WHERE segment_id=? ORDER BY observed_at",(segment_id,)); alerts=rows(self.db,"SELECT * FROM alerts WHERE segment_id=? ORDER BY created_at",(segment_id,)); return {"segment_id":segment_id,"readings":len(readings),"alerts":alerts,"leak_probability":leak_probability(alerts)}
    def create_work_order(self,token,segment_id,alert_id,assignee,priority=3):
        actor=self.auth.require(token,"work_order")
        if not assignee.strip() or not 1<=priority<=5:raise ValueError("assignee and priority are invalid")
        if not self.db.execute("SELECT 1 FROM alerts WHERE alert_id=? AND segment_id=?",(alert_id,segment_id)).fetchone():raise KeyError(alert_id)
        wid="wo-"+uuid.uuid4().hex[:16]
        with transaction(self.db): self.db.execute("INSERT INTO work_orders(work_order_id,segment_id,alert_id,assignee,status,priority,created_at,updated_at,version) VALUES(?,?,?,?,?,?,?,?,1)",(wid,segment_id,alert_id,assignee,"open",priority,utcnow(),utcnow())); audit(self.db,"work_order",wid,"created",actor.user_id,{"segment_id":segment_id,"alert_id":alert_id})
        return self.work_order(token,wid)
    def work_order(self,token,work_order_id):
        self.auth.require(token,"read"); row=self.db.execute("SELECT * FROM work_orders WHERE work_order_id=?",(work_order_id,)).fetchone()
        if not row:raise KeyError(work_order_id)
        return dict(row)
    def _decision_body(self,row):
        return json.loads(row["response_json"])
    def _insert_decision(self,now,work_order_id,expected_version,actor,target,reason,digest,outcome,from_status,from_version,resulting_version,body):
        self.db.execute("INSERT INTO work_order_decisions(decision_id,work_order_id,expected_version,actor,target,reason,request_sha256,outcome,from_status,from_version,resulting_version,response_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",(body["decision_id"],work_order_id,expected_version,actor,target,reason,digest,outcome,from_status,from_version,resulting_version,json.dumps(body,ensure_ascii=False,sort_keys=True),now))
    def _conflict_body(self,now,work_order_id,expected_version,actor,target,reason,detail):
        order=self.db.execute("SELECT status,version FROM work_orders WHERE work_order_id=?",(work_order_id,)).fetchone()
        winner=self.db.execute("SELECT actor,target,reason,resulting_version FROM work_order_decisions WHERE work_order_id=? AND expected_version=? AND outcome='applied'",(work_order_id,expected_version)).fetchone()
        body={"decision_id":"dec-"+uuid.uuid4().hex[:16],"outcome":"conflicted","work_order_id":work_order_id,"expected_version":expected_version,"detail":detail,"current_status":order["status"],"current_version":order["version"],"submission":{"actor":actor,"target":target,"reason":reason},"replayed":False}
        if winner:body["winner"]={"actor":winner["actor"],"target":winner["target"],"reason":winner["reason"],"resulting_version":winner["resulting_version"]}
        return order,body
    @staticmethod
    def _audit_conflict(body):
        payload={k:body[k] for k in ("decision_id","expected_version","detail","current_status","current_version")}
        payload["submission"]=dict(body["submission"])
        if "winner" in body: payload["winner"]=body["winner"]
        return payload
    def _persist_conflict(self,now,work_order_id,expected_version,actor_id,target,reason,digest,detail):
        order,body=self._conflict_body(now,work_order_id,expected_version,actor_id,target,reason,detail)
        self._insert_decision(now,work_order_id,expected_version,actor_id,target,reason,digest,"conflicted",order["status"],order["version"],None,body)
        audit(self.db,"work_order",work_order_id,"transition_conflicted",actor_id,self._audit_conflict(body))
        return body
    def _replay_or_raise(self,prior):
        replay=self._decision_body(prior); replay["replayed"]=True
        if prior["outcome"]=="conflicted": raise Conflict("work order transition conflict",replay)
        return replay
    def _resolve_after_losing_race(self,work_order_id,actor_id,target,reason,expected_version,digest):
        # 先到者已占据该前置版本或相同请求摘要：读取并重放原决定；若本方请求尚未落库则登记为冲突。
        for _ in range(100):
            try:
                with transaction(self.db):
                    prior=self.db.execute("SELECT response_json,outcome FROM work_order_decisions WHERE work_order_id=? AND request_sha256=?",(work_order_id,digest)).fetchone()
                    if prior is not None: return self._replay_or_raise(prior)
                    order=self.db.execute("SELECT status,version FROM work_orders WHERE work_order_id=?",(work_order_id,)).fetchone()
                    detail="work order is already "+order["status"] if order["status"] in TERMINAL_STATUSES else f"expected version {expected_version} but current version is {order['version']}"
                    return self._persist_conflict(utcnow(),work_order_id,expected_version,actor_id,target,reason,digest,detail)
            except sqlite3.IntegrityError:
                continue  # 另一连接刚登记了相同请求，回滚后下一轮重放其决定。
        raise RuntimeError("could not settle work order transition after repeated races")
    def transition_work_order(self,token,work_order_id,target,reason,expected_version):
        actor=self.auth.require(token,"work_order")
        if not isinstance(expected_version,int) or isinstance(expected_version,bool) or expected_version<1:raise ValidationFailed("expected_version must be a positive integer")
        if not isinstance(target,str) or not target.strip():raise ValidationFailed("transition target is required")
        if not isinstance(reason,str) or not reason.strip():raise ValidationFailed("transition reason is required")
        digest=_decision_digest(work_order_id,actor.user_id,expected_version,target,reason)
        # 同一连接被 HTTP 线程共享，锁与 BEGIN IMMEDIATE 共同保证请求按到达顺序串行裁决。
        with self._lock:
            try:
                with transaction(self.db):
                    row=self.db.execute("SELECT status,version FROM work_orders WHERE work_order_id=?",(work_order_id,)).fetchone()
                    if not row:raise KeyError(work_order_id)
                    prior=self.db.execute("SELECT response_json,outcome FROM work_order_decisions WHERE work_order_id=? AND request_sha256=?",(work_order_id,digest)).fetchone()
                    if prior is not None:
                        return self._replay_or_raise(prior)
                    now=utcnow(); detail=None
                    if row["status"] in TERMINAL_STATUSES: detail="work order is already "+row["status"]
                    elif row["version"]!=expected_version: detail=f"expected version {expected_version} but current version is {row['version']}"
                    elif target not in WORK_ORDER_TRANSITIONS.get(row["status"],set()): detail=f"invalid work order transition from {row['status']} to {target}"
                    if detail is not None:
                        body=self._persist_conflict(now,work_order_id,expected_version,actor.user_id,target,reason,digest,detail)
                    else:
                        updated=self.db.execute("UPDATE work_orders SET status=?,version=version+1,updated_at=? WHERE work_order_id=? AND version=?",(target,now,work_order_id,expected_version)).rowcount
                        if updated!=1:
                            body=self._persist_conflict(now,work_order_id,expected_version,actor.user_id,target,reason,digest,f"expected version {expected_version} but current version is {row['version']}")
                        else:
                            current=dict(self.db.execute("SELECT * FROM work_orders WHERE work_order_id=?",(work_order_id,)).fetchone())
                            body={"decision_id":"dec-"+uuid.uuid4().hex[:16],"outcome":"applied","work_order_id":work_order_id,"expected_version":expected_version,"resulting_version":expected_version+1,"replayed":False,"work_order":current}
                            self._insert_decision(now,work_order_id,expected_version,actor.user_id,target,reason,digest,"applied",row["status"],row["version"],expected_version+1,body)
                            audit(self.db,"work_order",work_order_id,"transition_applied",actor.user_id,{"decision_id":body["decision_id"],"from":row["status"],"to":target,"reason":reason,"expected_version":expected_version,"resulting_version":expected_version+1})
            except sqlite3.IntegrityError:
                # 多连接/多进程并发：同一前置版本的 applied 行或相同请求摘要已由先到者落库。
                body=self._resolve_after_losing_race(work_order_id,actor.user_id,target,reason,expected_version,digest)
        if body["outcome"]=="conflicted": raise Conflict("work order transition conflict",body)
        return body
    def work_order_decisions(self,token,work_order_id):
        self.auth.require(token,"read")
        if not self.db.execute("SELECT 1 FROM work_orders WHERE work_order_id=?",(work_order_id,)).fetchone():raise KeyError(work_order_id)
        return rows(self.db,"SELECT * FROM work_order_decisions WHERE work_order_id=? ORDER BY rowid",(work_order_id,))
    def add_resource(self,token,resource_id,kind,district,capacity):
        actor=self.auth.require(token,"admin")
        if capacity<=0 or not kind.strip() or not district.strip():raise ValueError("resource fields are invalid")
        with transaction(self.db):self.db.execute("INSERT INTO resources VALUES(?,?,?,?,?)",(resource_id,kind,district,capacity,capacity)); audit(self.db,"resource",resource_id,"created",actor.user_id,{"kind":kind,"district":district,"capacity":capacity})
        return self.resource(token,resource_id)
    def resource(self,token,resource_id):
        self.auth.require(token,"read"); row=self.db.execute("SELECT * FROM resources WHERE resource_id=?",(resource_id,)).fetchone()
        if not row:raise KeyError(resource_id)
        return dict(row)
    def allocate(self,token,resource_id,work_order_id,quantity):
        actor=self.auth.require(token,"allocate")
        if quantity<=0:raise ValueError("quantity must be positive")
        aid="alloc-"+uuid.uuid4().hex[:16]
        with transaction(self.db):
            resource=self.db.execute("SELECT available FROM resources WHERE resource_id=?",(resource_id,)).fetchone()
            if not resource:raise KeyError(resource_id)
            if not self.db.execute("SELECT 1 FROM work_orders WHERE work_order_id=?",(work_order_id,)).fetchone():raise KeyError(work_order_id)
            if resource[0]<quantity:raise ValueError("resource capacity exceeded")
            old=self.db.execute("SELECT allocation_id FROM allocations WHERE resource_id=? AND work_order_id=?",(resource_id,work_order_id)).fetchone()
            if old:return {"allocation_id":old[0],"duplicate":True}
            self.db.execute("INSERT INTO allocations VALUES(?,?,?,?,?)",(aid,resource_id,work_order_id,quantity,utcnow())); self.db.execute("UPDATE resources SET available=available-? WHERE resource_id=?",(quantity,resource_id)); audit(self.db,"resource",resource_id,"allocated",actor.user_id,{"work_order_id":work_order_id,"quantity":quantity})
        return {"allocation_id":aid,"duplicate":False,"resource_id":resource_id,"quantity":quantity}
    def audit_events(self,token,entity_type,entity_id): self.auth.require(token,"read"); return rows(self.db,"SELECT * FROM audit_events WHERE entity_type=? AND entity_id=? ORDER BY event_id",(entity_type,entity_id))
