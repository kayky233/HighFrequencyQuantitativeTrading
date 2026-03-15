from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class SignedEnvelope(BaseModel):
    algorithm: str = Field(default="ed25519")
    payload: dict[str, Any]
    signature: str
    key_id: str | None = None


class LicensePayload(BaseModel):
    license_id: str
    customer_id: str | None = None
    product: str
    edition: str = Field(default="professional")
    machine_binding_mode: str = Field(default="single")
    machine_hash: str | None = None
    expire_at: datetime
    features: list[str] = Field(default_factory=list)
    limits: dict[str, Any] = Field(default_factory=dict)
    offline_grace_hours: int = Field(default=72)
    min_client_version: str | None = None
    max_client_version: str | None = None
    allowed_model_version: str | None = None
    allowed_engine_version: str | None = None


class SessionTokenPayload(BaseModel):
    sub: str | None = None
    license_id: str
    machine_hash: str | None = None
    issued_at: datetime
    expire_at: datetime
    session_rights: list[str] = Field(default_factory=list)
    rev: int = Field(default=1)


class AuthorizationStatus(BaseModel):
    enabled: bool = False
    mode: str = Field(default="off")
    product: str = Field(default="HFQT")
    client_version: str = Field(default="0.4.0")
    machine_hash_short: str | None = None
    license_path: str
    token_cache_path: str
    public_key_path: str
    public_key_present: bool = False
    license_present: bool = False
    license_valid: bool = False
    token_present: bool = False
    token_valid: bool = False
    machine_match: bool = False
    in_offline_grace: bool = False
    can_run: bool = True
    reason: str | None = None
    edition: str | None = None
    license_id: str | None = None
    features: list[str] = Field(default_factory=list)
    limits: dict[str, Any] = Field(default_factory=dict)
    expires_at: datetime | None = None
    token_expires_at: datetime | None = None
    offline_grace_deadline: datetime | None = None
