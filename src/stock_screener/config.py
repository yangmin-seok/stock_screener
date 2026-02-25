from __future__ import annotations

import logging
import os
from pathlib import Path


logger = logging.getLogger(__name__)


def load_env_file(path: str | Path = ".env") -> None:
    """Load key=value pairs from a local .env file into process environment.

    Existing environment values are preserved.
    """

    env_path = Path(path)
    if not env_path.exists() or not env_path.is_file():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def mask_secret(secret: str, prefix: int = 4, suffix: int = 4) -> str:
    if not secret:
        return ""
    if len(secret) <= prefix + suffix:
        return "*" * len(secret)
    return f"{secret[:prefix]}{'*' * (len(secret) - (prefix + suffix))}{secret[-suffix:]}"


def get_required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(
            f"필수 환경변수 '{name}'가 설정되지 않았습니다. "
            "배치 실행 전에 프로젝트 루트 .env 또는 실행 환경에 값을 설정하세요."
        )
    logger.info("Loaded required environment variable: %s=%s", name, mask_secret(value))
    return value
