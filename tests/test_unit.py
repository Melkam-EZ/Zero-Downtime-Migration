"""Tests that need no Postgres or HAProxy."""
import pytest

from migrate import pg
from migrate.config import Config, ConfigError, from_env, validate
from migrate.plan import Abort, Migration
from migrate.proxy import Proxy, ProxyError, parse_stats
from probe.probe import Prober, format_report

ENV = {"OLD_DSN": "o", "NEW_DSN": "n", "OLD_DSN_FROM_NEW": "a", "NEW_DSN_FROM_OLD": "b", "PROXY_ADMIN": "127.0.0.1:9999",
       "LEGACY_URL": "http://l/", "MODERN_URL": "http://m"}


def test_config_from_env_defaults_and_trailing_slash():
    cfg = from_env(ENV)
    assert cfg.proxy_port == 9999 and cfg.legacy_url == "http://l" and cfg.canary_steps[-1] == 100


def test_config_reports_every_missing_variable():
    with pytest.raises(ConfigError, match="OLD_DSN.*NEW_DSN"):
        from_env({"PROXY_ADMIN": "127.0.0.1:1"})


@pytest.mark.parametrize("bad", [{"PROXY_ADMIN": "nope"}, {"CANARY_STEPS": "5,x"}, {"CANARY_STEPS": "50,25,100"}, {"CANARY_STEPS": "5,50"},
                                 {"CANARY_STEPS": "0,100"}, {"MAX_ERROR_RATE": "2"}, {"OBSERVE_SECONDS": "soon"}])
def test_config_rejects_bad_values(bad):
    with pytest.raises(ConfigError):
        from_env({**ENV, **bad})


STATS = ("# pxname,svname,qcur,type,status,weight,stot,hrsp_5xx,\n"
         "readers,FRONTEND,,0,OPEN,,5,0,\n"
         "readers,legacy,0,2,UP,100,40,1,\n"
         "readers,modern,0,2,DOWN,0,0,0,\n")


def test_parse_stats_keeps_only_server_rows():
    rows = parse_stats(STATS)
    assert set(rows) == {("readers", "legacy"), ("readers", "modern")}
    assert rows[("readers", "legacy")]["weight"] == "100" and rows[("readers", "modern")]["status"] == "DOWN"


def test_parse_stats_rejects_garbage():
    with pytest.raises(ProxyError):
        parse_stats("Unknown command")


def test_set_weights_validates_and_surfaces_haproxy_errors(monkeypatch):
    px, sent = Proxy("127.0.0.1", 1), []
    monkeypatch.setattr(px, "command", lambda c: sent.append(c) or "")
    px.set_weights("writers", {"legacy": 0, "modern": 100})
    assert sent == ["set weight writers/legacy 0", "set weight writers/modern 100"]
    with pytest.raises(ProxyError, match="out of range"):
        px.set_weights("writers", {"legacy": 999})
    monkeypatch.setattr(px, "command", lambda c: "No such server.")
    with pytest.raises(ProxyError, match="No such server"):
        px.set_weights("writers", {"ghost": 1})


def test_unreachable_proxy_gives_a_clear_error():
    with pytest.raises(ProxyError, match="cannot reach the HAProxy runtime API"):
        Proxy("127.0.0.1", 1, timeout=0.2).command("show stat")


def make_cfg(tmp_path):
    return Config("o", "n", "a", "b", "127.0.0.1", 1, "http://l", "http://m", state_dir=str(tmp_path / "state"))


def test_steps_refuse_to_run_out_of_order(tmp_path):
    mig = Migration(make_cfg(tmp_path), log=lambda *_: None)
    for step in (mig.cutover, mig.rollback, mig.finalize, mig.canary):
        with pytest.raises(Abort, match="needs phase"):
            step()


def test_state_survives_a_new_process(tmp_path):
    a = Migration(make_cfg(tmp_path), log=lambda *_: None)
    a._set(phase="synced")
    assert Migration(make_cfg(tmp_path), log=lambda *_: None).phase == "synced"


def test_reset_refuses_after_cutover_without_force(tmp_path):
    mig = Migration(make_cfg(tmp_path), log=lambda *_: None)
    mig._set(phase="cutover_done")
    with pytest.raises(Abort, match="refusing to reset"):
        mig.reset()


def test_fault_injection_only_fires_at_its_checkpoint(tmp_path):
    mig = Migration(make_cfg(tmp_path), log=lambda *_: None, fault="drained")
    mig._cp("frozen")
    with pytest.raises(Abort, match="injected fault"):
        mig._cp("drained")


def test_wait_times_out_with_a_clear_message(tmp_path):
    t = iter(range(100))
    mig = Migration(make_cfg(tmp_path), log=lambda *_: None, sleep=lambda s: None, clock=lambda: next(t))
    with pytest.raises(Abort, match="timed out after 3s waiting for the thing"):
        mig._wait(lambda: False, 3, "the thing")


def rec(kind="read", ok=True, stale=False, attempts=1, version="v1", ms=5.0, t=0.0):
    return {"t": t, "kind": kind, "ok": ok, "attempts": attempts, "status": 200 if ok else 503, "ms": ms, "version": version, "stale": stale}


def prober_with(records):
    p = Prober("http://127.0.0.1:1")
    p.started, p.records = 0.0, records
    return p


def test_report_passes_only_when_nothing_failed_and_nothing_lost():
    ok = prober_with([rec(), rec("write", version="v2", t=6.0)])
    assert ok.report({"acknowledged_writes": 1, "lost_writes": 0, "id_mismatches": 0})["pass"]
    assert not prober_with([rec(ok=False)]).report()["pass"]
    assert not prober_with([rec(stale=True)]).report()["pass"]
    assert not ok.report({"acknowledged_writes": 1, "lost_writes": 1, "id_mismatches": 0})["pass"]
    assert not ok.report({"acknowledged_writes": 1, "lost_writes": 0, "id_mismatches": 1})["pass"]


def test_report_buckets_traffic_by_version_and_flags_slow_writes():
    rep = prober_with([rec(version="v1", t=1), rec(version="v2", t=7), rec("write", ms=900, attempts=3, t=2)]).report()
    assert rep["served_by_version_per_5s"] == {"0": {"v1": 2}, "5": {"v2": 1}}
    assert rep["slowest_writes"][0]["ms"] == 900 and rep["retried"] == 1
    assert "RESULT: PASS" in format_report(rep)
    assert "RESULT: FAIL" in format_report(prober_with([rec(ok=False)]).report())


def test_major_version_parsing():
    class Fake:
        def __init__(self, v): self.v = v
        def execute(self, *a, **k):
            class R:
                def fetchone(s): return (self.v,)
            return R()
    assert pg.major_version(Fake("16.4 (Debian 16.4-1.pgdg120+1)")) == 16
    assert pg.major_version(Fake("15.8")) == 15
