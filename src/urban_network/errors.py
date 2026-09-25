"""工单流转等服务的可观察错误类型，携带 HTTP 状态码与结构化细节。"""
from __future__ import annotations


class ServiceError(RuntimeError):
    code = "service_error"
    status = 400

    def __init__(self, message, *, details=None):
        super().__init__(message)
        self.details = dict(details or {})

    def __str__(self):
        return str(self.args[0]) if self.args else ""


class NotFound(ServiceError, KeyError):
    code = "not_found"
    status = 404


class Conflict(ServiceError):
    """版本条件未满足或幂等键被不同内容复用。"""
    code = "conflict"
    status = 409


class InvalidState(ServiceError, ValueError):
    """当前状态不允许该流转（例如工单已完成或已取消）。"""
    code = "invalid_state"
    status = 409


class ValidationFailed(ServiceError, ValueError):
    code = "validation_failed"
    status = 422
