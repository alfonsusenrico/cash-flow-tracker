"""Symmetric encryption for API keys at rest (CPU-only, sync — fast)."""
from __future__ import annotations

from cryptography.fernet import Fernet


class Crypto:
    def __init__(self, secret: str) -> None:
        self._f = Fernet(secret.encode())

    def encrypt(self, plaintext: str) -> bytes:
        return self._f.encrypt(plaintext.encode())

    def decrypt(self, token: bytes) -> str:
        return self._f.decrypt(bytes(token)).decode()
