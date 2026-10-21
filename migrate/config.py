import os
from dataclasses import dataclass


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class Config:
    old_dsn: str                 # admin connection to the legacy database (from where this tool runs)
    new_dsn: str                 # admin connection to the new database
    old_from_new: str            # how the NEW database reaches the OLD one (replicator role)
    new_from_old: str            # how the OLD database reaches the NEW one (used for the rollback stream)
    proxy_host: str
    proxy_port: int              # HAProxy runtime API
    legacy_url: str              # apps, direct (for shadow comparison)
    modern_url: str
    app_role: str = "app"
    replicator_role: str = "replicator"
    canary_steps: tuple = (5, 25, 50, 100)
    observe_seconds: float = 20.0
    max_error_rate: float = 0.01
    min_requests: int = 10
    max_lag_bytes: int = 1_000_000
    sync_timeout: float = 600.0
    drain_timeout: float = 20.0
    catchup_timeout: float = 30.0
    settle_seconds: float = 2.0
    state_dir: str = ".state"


REQUIRED = {
    "OLD_DSN": "old_dsn", "NEW_DSN": "new_dsn", "OLD_DSN_FROM_NEW": "old_from_new", "NEW_DSN_FROM_OLD": "new_from_old",
    "PROXY_ADMIN": None, "LEGACY_URL": "legacy_url", "MODERN_URL": "modern_url",
}


def from_env(env=None):
    env = os.environ if env is None else env
    missing = [k for k in REQUIRED if not env.get(k)]
    if missing:
        raise ConfigError("missing environment variables: " + ", ".join(missing) + " (source .env, see STEPS.md)")
    host, _, port = env["PROXY_ADMIN"].rpartition(":")
    if not host or not port.isdigit():
        raise ConfigError("PROXY_ADMIN must look like 127.0.0.1:9999")
    kw = {}
    if env.get("CANARY_STEPS"):
        try:
            kw["canary_steps"] = tuple(int(x) for x in env["CANARY_STEPS"].split(","))
        except ValueError:
            raise ConfigError("CANARY_STEPS must be comma-separated integers, e.g. 5,25,50,100")
    for var, field, cast in (("OBSERVE_SECONDS", "observe_seconds", float), ("MAX_ERROR_RATE", "max_error_rate", float),
                             ("MIN_REQUESTS", "min_requests", int), ("MAX_LAG_BYTES", "max_lag_bytes", int),
                             ("STATE_DIR", "state_dir", str), ("APP_ROLE", "app_role", str)):
        if env.get(var):
            try:
                kw[field] = cast(env[var])
            except ValueError:
                raise ConfigError(f"{var} has an invalid value: {env[var]!r}")
    cfg = Config(env["OLD_DSN"], env["NEW_DSN"], env["OLD_DSN_FROM_NEW"], env["NEW_DSN_FROM_OLD"], host, int(port),
                 env["LEGACY_URL"].rstrip("/"), env["MODERN_URL"].rstrip("/"), **kw)
    validate(cfg)
    return cfg


def validate(cfg):
    steps = cfg.canary_steps
    if not steps or any(not 0 < s <= 100 for s in steps) or list(steps) != sorted(set(steps)):
        raise ConfigError("canary steps must be strictly increasing integers between 1 and 100")
    if steps[-1] != 100:
        raise ConfigError("the last canary step must be 100 so reads end up fully on the new stack")
    if not 0 <= cfg.max_error_rate < 1:
        raise ConfigError("max_error_rate must be between 0 and 1")
