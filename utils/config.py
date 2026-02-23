"""Shared configuration loader for corpus data utilities.

Loads config.ini for paths/settings and .env for secrets.
All paths default to relative locations under the repository root.
"""

import configparser
import os
from pathlib import Path

# Repository root = parent of utils/
REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_env():
    """Load .env file into environment (does not overwrite existing vars)."""
    env_file = REPO_ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                key, val = key.strip(), val.strip()
                if key and key not in os.environ:
                    os.environ[key] = val


def _resolve_path(p: str) -> Path:
    """Resolve a path: absolute stays absolute, relative is from REPO_ROOT."""
    path = Path(p)
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def load_config() -> configparser.ConfigParser:
    """Load config.ini with sensible defaults."""
    _load_env()

    config = configparser.ConfigParser()
    config["paths"] = {
        "storage_dir": str(REPO_ROOT / "storage"),
        "log_dir": str(REPO_ROOT / "logs"),
    }
    config["postgres"] = {
        "host": "localhost",
        "port": "5432",
        "database": "courtlistener",
        "user": "postgres",
        "sslmode": "require",
    }

    config_file = REPO_ROOT / "config.ini"
    if config_file.exists():
        config.read(config_file)

    return config


_config = None


def get_config() -> configparser.ConfigParser:
    global _config
    if _config is None:
        _config = load_config()
    return _config


def storage_dir(source: str = "") -> Path:
    """Get storage directory, optionally for a specific source. Creates it."""
    base = _resolve_path(get_config()["paths"]["storage_dir"])
    p = base / source if source else base
    p.mkdir(parents=True, exist_ok=True)
    return p


def log_dir() -> Path:
    """Get log directory. Creates it."""
    p = _resolve_path(get_config()["paths"]["log_dir"])
    p.mkdir(parents=True, exist_ok=True)
    return p


def postgres_config() -> dict:
    """Get postgres connection config with password from .env."""
    cfg = get_config()["postgres"]
    return {
        "host": cfg["host"],
        "port": int(cfg["port"]),
        "database": cfg["database"],
        "user": cfg["user"],
        "password": os.environ.get("POSTGRES_PASSWORD", ""),
        "sslmode": cfg.get("sslmode", "require"),
    }
