"""
提供 Brain 服务 API 元信息。
"""
from typing import Dict, Any
from datetime import datetime


def get_api_info() -> Dict[str, Any]:
    """返回 API 基本信息，用于 /api/v1/info。

    返回:
        包含服务名称、版本、时间戳、说明的字典
    """
    return {
        "service": "brain-service",
        "version": "1.0.0",
        "description": "Central coordinator for AutoTM services (dataflow, scheduling, routing).",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "endpoints": [
            {"method": "GET", "path": "/health", "desc": "Basic health check"},
            {"method": "GET", "path": "/api/v1/status", "desc": "Detailed component status"},
            {"method": "GET", "path": "/api/v1/info", "desc": "API metadata"},
        ],
    }

