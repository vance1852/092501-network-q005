"""协调管网监测、告警、工单和应急资源分配的应用服务。"""
from __future__ import annotations
import hashlib,json,threading,uuid
from .auth import Auth
from .errors import Conflict,InvalidState,NotFound,ValidationFailed
from .models import Reading,Segment,as_dict,utcnow
from .risk import leak_probability,score_reading
from .storage import audit,connect,rows,transaction
TERMINAL_STATUSES={"completed","cancelled"}
ALLOWED_TRANSITIONS={"open":{"assigned","cancelled"},"assigned":{"in_progress","cancelled"},"in_progress":{"completed","blocked"},"blocked":{"in_progress","cancelled"},"completed":set(),"cancelled":set()}
KNOWN_STATUSES=frozenset(ALLOWED_TRANSITIONS)
def _transition_digest(work_order_id,actor,expected_version,target,reason,request_id):
    canonical={"work_order_id":work_order_id,"actor":actor,"expected_version":expected_version,"target":target,"reason":reason,"request_id":request_id}
    return hashlib.sha256(json.dumps(canonical,ensure_ascii=False,sort_keys=True).encode()).hexdigest()
class NetworkService:
    def __init__(self,database=":memory:"):
        self._lock=threading.RLock(); self.db=connect(database); self.auth=Auth(self.db,lock=self._lock)
    def bootstrap(self):
        for uid,pwd,role in (("admin","network-admin","admin"),("operator","network-operator","operator")):
            try:self.auth.create_user(uid,pwd,role)
            except Exception:pass
    def register_segment(self,token,segment):
        with self._lock:
            actor=self.auth.require(token,"admin"); segment.validate(); now=utcnow()
            with transaction(self.db):
                self.db.execute("INSERT INTO segments VALUES(?,?,?,?,?,?,?,?)",(segment.segment_id,segment.district,segment.network_type,segment.length_m,segment.criticality,segment.status,now,now)); audit(self.db,"segment",segment.segment_id,"created",actor.user_id,as_dict(segment))
            return self.segment(token,segment.segment_id)
    def segment(self,token,segment_id):
        with self._lock:
            self.auth.require(token,"read"); row=self.db.execute("SELECT * FROM segments WHERE segment_id=?",(segment_id,)).fetchone()
            if not row:raise NotFound(segment_id)
            return dict(row)
    def ingest_reading(self,token,reading):
        with self._lock:
            actor=self.auth.require(token,"measure"); reading.validate(); seg=self.db.execute("SELECT criticality FROM segments WHERE segment_id=?",(reading.segment_id,)).fetchone()
            if not seg:raise NotFound(reading.segment_id)
            risk=score_reading(reading.pressure_kpa,reading.flow_lps,reading.acoustic_db,seg[0]); fingerprint=hashlib.sha256(f"{reading.segment_id}|{reading.sensor_id}|{reading.observed_at}".encode()).hexdigest()
            with transaction(self.db):
                if self.db.execute("SELECT reading_id FROM readings WHERE reading_id=?",(reading.reading_id,)).fetchone(): return {"reading_id":reading.reading_id,"duplicate":True,"risk":as_dict(risk)}
                self.db.execute("INSERT INTO readings VALUES(?,?,?,?,?,?,?)",(reading.reading_id,reading.segment_id,reading.sensor_id,reading.pressure_kpa,reading.flow_lps,reading.acoustic_db,reading.observed_at)); alert_id=None
                if risk.severity in {"high","critical"}:
                    alert_id="alert-"+fingerprint[:18]; self.db.execute("INSERT OR IGNORE INTO alerts VALUES(?,?,?,?,?,?,?,?)",(alert_id,reading.segment_id,fingerprint,risk.severity,risk.score,"open",utcnow(),None))
                audit(self.db,"reading",reading.reading_id,"ingested",actor.user_id,{"risk":as_dict(risk),"alert_id":alert_id})
            return {"reading_id":reading.reading_id,"duplicate":False,"risk":as_dict(risk),"alert_id":alert_id}
    def risk_report(self,token,segment_id):
        with self._lock:
            self.auth.require(token,"analyze"); readings=rows(self.db,"SELECT * FROM readings WHERE segment_id=? ORDER BY observed_at",(segment_id,)); alerts=rows(self.db,"SELECT * FROM alerts WHERE segment_id=? ORDER BY created_at",(segment_id,)); return {"segment_id":segment_id,"readings":len(readings),"alerts":alerts,"leak_probability":leak_probability(alerts)}
    def create_work_order(self,token,segment_id,alert_id,assignee,priority=3):
        with self._lock:
            actor=self.auth.require(token,"work_order")
            if not assignee.strip() or not 1<=priority<=5:raise ValueError("assignee and priority are invalid")
            if not self.db.execute("SELECT 1 FROM alerts WHERE alert_id=? AND segment_id=?",(alert_id,segment_id)).fetchone():raise NotFound(alert_id)
            wid="wo-"+uuid.uuid4().hex[:16]; now=utcnow()
            with transaction(self.db): self.db.execute("INSERT INTO work_orders(work_order_id,segment_id,alert_id,assignee,status,priority,version,created_at,updated_at) VALUES(?,?,?,?,?,?,1,?,?)",(wid,segment_id,alert_id,assignee,"open",priority,now,now)); audit(self.db,"work_order",wid,"created",actor.user_id,{"segment_id":segment_id,"alert_id":alert_id})
            return self.work_order(token,wid)
    def work_order(self,token,work_order_id):
        with self._lock:
            self.auth.require(token,"read"); row=self.db.execute("SELECT * FROM work_orders WHERE work_order_id=?",(work_order_id,)).fetchone()
            if not row:raise NotFound(work_order_id)
            return dict(row)
    def transition_work_order(self,token,work_order_id,target,reason,expected_version,request_id=None):
        """按前置版本流转工单：同一版本最多成功一次，相同重试返回原决定，冲突双方留痕。"""
        with self._lock:
            actor=self.auth.require(token,"work_order")
            if target not in KNOWN_STATUSES:raise ValidationFailed("unknown work order status")
            if not isinstance(reason,str) or not reason.strip():raise ValidationFailed("transition reason is required")
            if isinstance(expected_version,bool) or not isinstance(expected_version,int) or expected_version<1:raise ValidationFailed("expected_version must be a positive integer")
            if request_id is not None:
                request_id=str(request_id).strip()
                if not request_id:raise ValidationFailed("request_id cannot be blank")
            digest=_transition_digest(work_order_id,actor.user_id,expected_version,target,reason,request_id); conflict=None; response=None
            with transaction(self.db):
                row=self.db.execute("SELECT * FROM work_orders WHERE work_order_id=?",(work_order_id,)).fetchone()
                if not row:raise NotFound(work_order_id)
                if request_id is not None:
                    prior=self.db.execute("SELECT * FROM work_order_transitions WHERE work_order_id=? AND request_id=?",(work_order_id,request_id)).fetchone()
                    if prior:
                        if prior["request_sha256"]!=digest:raise Conflict("同一请求标识对应了不同的流转内容")
                        response=self._replay(prior)
                    if response is None:
                        rejected=self.db.execute("SELECT * FROM work_order_conflicts WHERE work_order_id=? AND request_id=?",(work_order_id,request_id)).fetchone()
                        if rejected:
                            if rejected["request_sha256"]!=digest:raise Conflict("同一请求标识对应了不同的流转内容")
                            conflict=self._conflict_view(rejected)
                if response is None and conflict is None:
                    prior=self.db.execute("SELECT * FROM work_order_transitions WHERE work_order_id=? AND from_version=? AND request_sha256=?",(work_order_id,expected_version,digest)).fetchone()
                    if prior:response=self._replay(prior)
                if response is None and conflict is None:
                    if row["version"]!=expected_version:
                        conflict=self._record_conflict(row,actor,target,reason,expected_version,request_id,digest)
                    else:
                        if row["status"] in TERMINAL_STATUSES:raise InvalidState(f"work order is already {row['status']} and cannot be reopened")
                        if target not in ALLOWED_TRANSITIONS[row["status"]]:raise InvalidState("invalid work order transition")
                        response=self._apply_transition(row,actor,target,reason,request_id,digest)
            if conflict is not None:raise Conflict("工单版本已变化，冲突提交已保留供调度员核对",details={"conflict":conflict})
            return response
    def _replay(self,prior):
        response=json.loads(prior["response_json"]); response["replayed"]=True; return response
    def _apply_transition(self,row,actor,target,reason,request_id,digest):
        wid=row["work_order_id"]; from_version=row["version"]; to_version=from_version+1; now=utcnow()
        cursor=self.db.execute("UPDATE work_orders SET status=?,version=version+1,updated_at=? WHERE work_order_id=? AND version=?",(target,now,wid,from_version))
        if cursor.rowcount!=1:raise Conflict("工单版本已变化，请重新读取后提交")
        cursor=self.db.execute("INSERT INTO work_order_transitions(work_order_id,request_id,actor,from_status,to_status,from_version,to_version,reason,request_sha256,response_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",(wid,request_id,actor.user_id,row["status"],target,from_version,to_version,reason,digest,"{}",now))
        transition_id=cursor.lastrowid
        response={"work_order_id":wid,"segment_id":row["segment_id"],"alert_id":row["alert_id"],"assignee":row["assignee"],"status":target,"priority":row["priority"],"version":to_version,"created_at":row["created_at"],"updated_at":now,"transition":{"transition_id":transition_id,"from_status":row["status"],"to_status":target,"from_version":from_version,"to_version":to_version,"reason":reason,"actor":actor.user_id,"request_id":request_id,"created_at":now},"replayed":False}
        self.db.execute("UPDATE work_order_transitions SET response_json=? WHERE transition_id=?",(json.dumps(response,ensure_ascii=False,sort_keys=True),transition_id))
        audit(self.db,"work_order",wid,"transition",actor.user_id,{"from":row["status"],"to":target,"from_version":from_version,"to_version":to_version,"reason":reason,"request_id":request_id,"transition_id":transition_id})
        return response
    def _record_conflict(self,row,actor,target,reason,expected_version,request_id,digest):
        wid=row["work_order_id"]
        existing=self.db.execute("SELECT * FROM work_order_conflicts WHERE work_order_id=? AND request_sha256=?",(wid,digest)).fetchone()
        if existing:return self._conflict_view(existing)
        winning=self.db.execute("SELECT * FROM work_order_transitions WHERE work_order_id=? AND from_version=?",(wid,expected_version)).fetchone(); now=utcnow()
        cursor=self.db.execute("INSERT INTO work_order_conflicts(work_order_id,actor,target_status,reason,request_id,expected_version,current_version,current_status,winning_transition_id,request_sha256,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",(wid,actor.user_id,target,reason,request_id,expected_version,row["version"],row["status"],None if winning is None else winning["transition_id"],digest,now))
        conflict_id=cursor.lastrowid
        audit(self.db,"work_order",wid,"transition_conflict",actor.user_id,{"conflict_id":conflict_id,"expected_version":expected_version,"current_version":row["version"],"current_status":row["status"],"target":target,"reason":reason,"request_id":request_id,"winning_transition_id":None if winning is None else winning["transition_id"]})
        return self._conflict_view(self.db.execute("SELECT * FROM work_order_conflicts WHERE conflict_id=?",(conflict_id,)).fetchone())
    def _conflict_view(self,conflict):
        winning=None
        if conflict["winning_transition_id"] is not None:
            w=self.db.execute("SELECT * FROM work_order_transitions WHERE transition_id=?",(conflict["winning_transition_id"],)).fetchone()
            if w:winning={"transition_id":w["transition_id"],"actor":w["actor"],"from_status":w["from_status"],"to_status":w["to_status"],"from_version":w["from_version"],"to_version":w["to_version"],"reason":w["reason"],"request_id":w["request_id"],"created_at":w["created_at"]}
        return {"conflict_id":conflict["conflict_id"],"work_order_id":conflict["work_order_id"],"expected_version":conflict["expected_version"],"current_version":conflict["current_version"],"current_status":conflict["current_status"],"submission":{"actor":conflict["actor"],"target":conflict["target_status"],"reason":conflict["reason"],"request_id":conflict["request_id"]},"winning":winning,"created_at":conflict["created_at"]}
    def work_order_conflicts(self,token,work_order_id):
        """调度员视图：已生效的流转决定与因版本冲突被保留的提交。"""
        with self._lock:
            self.auth.require(token,"read"); order=self.db.execute("SELECT * FROM work_orders WHERE work_order_id=?",(work_order_id,)).fetchone()
            if not order:raise NotFound(work_order_id)
            transitions=rows(self.db,"SELECT transition_id,work_order_id,request_id,actor,from_status,to_status,from_version,to_version,reason,request_sha256,created_at FROM work_order_transitions WHERE work_order_id=? ORDER BY transition_id",(work_order_id,))
            conflicts=[self._conflict_view(c) for c in self.db.execute("SELECT * FROM work_order_conflicts WHERE work_order_id=? ORDER BY conflict_id",(work_order_id,)).fetchall()]
            return {"work_order_id":work_order_id,"status":order["status"],"version":order["version"],"transitions":transitions,"conflicts":conflicts}
    def add_resource(self,token,resource_id,kind,district,capacity):
        with self._lock:
            actor=self.auth.require(token,"admin")
            if capacity<=0 or not kind.strip() or not district.strip():raise ValueError("resource fields are invalid")
            with transaction(self.db):self.db.execute("INSERT INTO resources VALUES(?,?,?,?,?)",(resource_id,kind,district,capacity,capacity)); audit(self.db,"resource",resource_id,"created",actor.user_id,{"kind":kind,"district":district,"capacity":capacity})
            return self.resource(token,resource_id)
    def resource(self,token,resource_id):
        with self._lock:
            self.auth.require(token,"read"); row=self.db.execute("SELECT * FROM resources WHERE resource_id=?",(resource_id,)).fetchone()
            if not row:raise NotFound(resource_id)
            return dict(row)
    def allocate(self,token,resource_id,work_order_id,quantity):
        with self._lock:
            actor=self.auth.require(token,"allocate")
            if quantity<=0:raise ValueError("quantity must be positive")
            aid="alloc-"+uuid.uuid4().hex[:16]
            with transaction(self.db):
                resource=self.db.execute("SELECT available FROM resources WHERE resource_id=?",(resource_id,)).fetchone()
                if not resource:raise NotFound(resource_id)
                if not self.db.execute("SELECT 1 FROM work_orders WHERE work_order_id=?",(work_order_id,)).fetchone():raise NotFound(work_order_id)
                if resource[0]<quantity:raise ValueError("resource capacity exceeded")
                old=self.db.execute("SELECT allocation_id FROM allocations WHERE resource_id=? AND work_order_id=?",(resource_id,work_order_id)).fetchone()
                if old:return {"allocation_id":old[0],"duplicate":True}
                self.db.execute("INSERT INTO allocations VALUES(?,?,?,?,?)",(aid,resource_id,work_order_id,quantity,utcnow())); self.db.execute("UPDATE resources SET available=available-? WHERE resource_id=?",(quantity,resource_id)); audit(self.db,"resource",resource_id,"allocated",actor.user_id,{"work_order_id":work_order_id,"quantity":quantity})
            return {"allocation_id":aid,"duplicate":False,"resource_id":resource_id,"quantity":quantity}
    def audit_events(self,token,entity_type,entity_id):
        with self._lock: self.auth.require(token,"read"); return rows(self.db,"SELECT * FROM audit_events WHERE entity_type=? AND entity_id=? ORDER BY event_id",(entity_type,entity_id))
