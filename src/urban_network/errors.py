"""管网服务向 API 暴露的稳定错误。"""
from __future__ import annotations
from typing import Any


class NetworkError(RuntimeError):
    code = "network_error"
    status = 400

    def __init__(self, message: str, body: dict[str, Any] | None = None):
        super().__init__(message)
        self.body = body


class ValidationFailed(NetworkError):
    code = "validation_failed"
    status = 422


class Conflict(NetworkError):
    code = "conflict"
    status = 409
