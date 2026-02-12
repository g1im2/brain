import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import auth_service as auth_module
from auth_service import AuthService


class _FakeSystemApi:
    def __init__(self, admin_user):
        self._admin_user = admin_user
        self.calls = []

    def ensure_system_schema(self):
        self.calls.append(("ensure_system_schema",))

    def seed_defaults(self):
        self.calls.append(("seed_defaults",))

    def get_user_by_username(self, username):
        self.calls.append(("get_user_by_username", username))
        if username == "admin":
            return dict(self._admin_user) if self._admin_user else None
        return None

    def set_user_password(self, user_id, password_hash, actor_id):
        self.calls.append(("set_user_password", user_id, actor_id, bool(password_hash)))

    def reset_user_auth_state(self, user_id, actor_id, reason):
        self.calls.append(("reset_user_auth_state", user_id, actor_id, reason))

    def record_login_success(self, user_id, ip_address):
        self.calls.append(("record_login_success", user_id, ip_address))

    def append_audit_log(self, audit):
        self.calls.append(("append_audit_log", getattr(audit, "action", "")))


def _build_service(monkeypatch, api):
    monkeypatch.setattr(auth_module, "create_database_manager", lambda: object())
    monkeypatch.setattr(auth_module, "UISystemDataAPI", lambda _db: api)
    config = SimpleNamespace(
        service=SimpleNamespace(
            auth_issuer="autotm-brain",
            auth_jwt_secret="unit-test-secret",
            auth_access_token_ttl_seconds=900,
            auth_refresh_token_ttl_seconds=604800,
            auth_admin_default_password="admin123!",
        )
    )
    return AuthService(config=config, redis_client=None)


@pytest.mark.asyncio
async def test_initialize_bootstrap_heals_admin_when_password_hash_missing(monkeypatch):
    api = _FakeSystemApi(
        {
            "id": "user_admin",
            "username": "admin",
            "password_hash": "",
            "failed_login_count": 5,
            "locked_until": "2099-01-01T00:00:00+00:00",
        }
    )
    service = _build_service(monkeypatch, api)

    await service.initialize()

    assert any(item[0] == "set_user_password" and item[1] == "user_admin" for item in api.calls)
    assert any(
        item[0] == "reset_user_auth_state"
        and item[1] == "user_admin"
        and item[3] == "bootstrap_missing_password_hash"
        for item in api.calls
    )


@pytest.mark.asyncio
async def test_initialize_does_not_reset_admin_when_password_hash_present(monkeypatch):
    api = _FakeSystemApi(
        {
            "id": "user_admin",
            "username": "admin",
            "password_hash": "pbkdf2_sha256$210000$salt$digest",
            "failed_login_count": 5,
            "locked_until": "2099-01-01T00:00:00+00:00",
        }
    )
    service = _build_service(monkeypatch, api)

    await service.initialize()

    assert not any(item[0] == "set_user_password" for item in api.calls)
    assert not any(item[0] == "reset_user_auth_state" for item in api.calls)


@pytest.mark.asyncio
async def test_initialize_no_admin_user_no_password_rewrite(monkeypatch):
    api = _FakeSystemApi(None)
    service = _build_service(monkeypatch, api)

    await service.initialize()

    assert not any(item[0] == "set_user_password" for item in api.calls)
    assert not any(item[0] == "reset_user_auth_state" for item in api.calls)
