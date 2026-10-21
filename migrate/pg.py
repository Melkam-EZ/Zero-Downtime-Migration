"""Postgres helpers for logical-replication based migrations. All connections are autocommit."""
import time

import psycopg
from psycopg import sql


def connect(dsn):
    conn = psycopg.connect(dsn, autocommit=True, connect_timeout=5)
    conn.execute("SET TIME ZONE 'UTC'")
    return conn


def one(conn, query, params=()):
    row = conn.execute(query, params).fetchone()
    return row[0] if row else None


def info(conn):
    return {
        "version": one(conn, "SHOW server_version"),
        "wal_level": one(conn, "SHOW wal_level"),
        "slots_free": int(one(conn, "SHOW max_replication_slots")) - one(conn, "SELECT count(*) FROM pg_replication_slots"),
        "senders_free": int(one(conn, "SHOW max_wal_senders")) - one(conn, "SELECT count(*) FROM pg_stat_replication"),
    }


def tables(conn):
    return [r[0] for r in conn.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY 1")]


def columns(conn, table):
    return [r[0] for r in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position", (table,))]


def pk_columns(conn, table):
    return [r[0] for r in conn.execute(
        "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
        "WHERE i.indrelid = %s::regclass AND i.indisprimary ORDER BY array_position(i.indkey, a.attnum)", (f'public."{table}"',))]


def row_count(conn, table):
    return one(conn, sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table)))


def fingerprint(conn, table, cols, pk):
    """(row count, md5 over every row's text in primary-key order), for the given columns."""
    row = sql.SQL("ROW({})::text").format(sql.SQL(", ").join(sql.Identifier(c) for c in cols))
    order = sql.SQL(", ").join(sql.Identifier(c) for c in pk)
    q = sql.SQL("SELECT count(*), coalesce(md5(string_agg(md5({row}), '' ORDER BY {order})), '') FROM {t}").format(
        row=row, order=order, t=sql.Identifier(table))
    return tuple(conn.execute(q).fetchone())


def sequences(conn):
    return {r[0]: r[1] for r in conn.execute("SELECT sequencename, last_value FROM pg_sequences WHERE schemaname = 'public'")}


def set_sequence(conn, name, last_value):
    if last_value is not None:
        conn.execute("SELECT setval(%s::regclass, %s, true)", (f'public."{name}"', last_value))


def resync_serial_sequences(conn):
    """Rows that arrive by replication do not advance the destination's sequences. Move every serial sequence up to
    the highest value already in its column, so the next insert can never reuse an existing id."""
    for table in tables(conn):
        for col in columns(conn, table):
            seq = one(conn, "SELECT pg_get_serial_sequence(%s, %s)", (f'public."{table}"', col))
            if not seq:
                continue
            top = one(conn, sql.SQL("SELECT max({}) FROM {}").format(sql.Identifier(col), sql.Identifier(table)))
            if top is not None:
                conn.execute("SELECT setval(%s::regclass, greatest(%s::bigint, (SELECT coalesce(max(last_value), 0) FROM pg_sequences "
                             "WHERE format('%%I.%%I', schemaname, sequencename) = %s OR sequencename = %s)), true)",
                             (seq, top, seq, seq.split(".")[-1].strip('"')))


def current_lsn(conn):
    """The INSERT position, not the write position: with asynchronous commit a committed transaction can sit beyond
    pg_current_wal_lsn(), and waiting for the lower number would declare 'caught up' too early."""
    return one(conn, "SELECT pg_current_wal_insert_lsn()")


def slot_lag_bytes(conn, slot):
    return one(conn, "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint FROM pg_replication_slots WHERE slot_name = %s", (slot,))


def wait_slot_caught_up(conn, slot, target_lsn, timeout, sleep=time.sleep):
    """Wait until the subscriber has confirmed everything up to target_lsn."""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        ok = one(conn, "SELECT coalesce(pg_wal_lsn_diff(confirmed_flush_lsn, %s::pg_lsn) >= 0, false) FROM pg_replication_slots WHERE slot_name = %s",
                 (target_lsn, slot))
        if ok:
            return True
        sleep(0.1)
    return False


def publication_exists(conn, name):
    return bool(one(conn, "SELECT count(*) FROM pg_publication WHERE pubname = %s", (name,)))


def create_publication(conn, name, columns=None):
    """columns=None publishes every table in full. columns={table: [cols]} publishes only those columns (PostgreSQL 15+):
    used for the rollback stream, because the old schema cannot accept columns added by the expand step."""
    if publication_exists(conn, name):
        return
    if columns is None:
        conn.execute(sql.SQL("CREATE PUBLICATION {} FOR ALL TABLES").format(sql.Identifier(name)))
        return
    parts = [sql.SQL("{} ({})").format(sql.Identifier(t), sql.SQL(", ").join(sql.Identifier(c) for c in cols)) for t, cols in columns.items()]
    conn.execute(sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(sql.Identifier(name), sql.SQL(", ").join(parts)))


def major_version(conn):
    return int(str(one(conn, "SHOW server_version")).split(".")[0].split()[0])


def drop_publication(conn, name):
    conn.execute(sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(name)))


def subscription_exists(conn, name):
    return bool(one(conn, "SELECT count(*) FROM pg_subscription WHERE subname = %s", (name,)))


def create_subscription(conn, name, conninfo, publication, copy_data):
    """The slot is created on the publisher with the same name as the subscription."""
    conn.execute(sql.SQL("CREATE SUBSCRIPTION {} CONNECTION {} PUBLICATION {} WITH (copy_data = {}, slot_name = {})").format(
        sql.Identifier(name), sql.Literal(conninfo), sql.Identifier(publication), sql.SQL("true" if copy_data else "false"), sql.Literal(name)))


def drop_subscription(conn, name):
    if subscription_exists(conn, name):
        conn.execute(sql.SQL("ALTER SUBSCRIPTION {} DISABLE").format(sql.Identifier(name)))
        conn.execute(sql.SQL("DROP SUBSCRIPTION {}").format(sql.Identifier(name)))     # also drops the slot on the publisher


def sync_states(conn, name):
    """{'r': tables fully copied and streaming, 'd'/'f'/'s'/'i': still copying}"""
    return {r[0]: r[1] for r in conn.execute(
        "SELECT r.srsubstate, count(*) FROM pg_subscription_rel r JOIN pg_subscription s ON s.oid = r.srsubid WHERE s.subname = %s GROUP BY 1", (name,))}


def set_role_readonly(conn, role, on):
    """Per-role default so only the application role is blocked; replication workers keep writing."""
    if on:
        conn.execute(sql.SQL("ALTER ROLE {} SET default_transaction_read_only = on").format(sql.Identifier(role)))
        conn.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = %s AND pid <> pg_backend_pid()", (role,))
    else:
        conn.execute(sql.SQL("ALTER ROLE {} RESET default_transaction_read_only").format(sql.Identifier(role)))


def busy_sessions(conn, role):
    return one(conn, "SELECT count(*) FROM pg_stat_activity WHERE usename = %s AND state <> 'idle' AND pid <> pg_backend_pid()", (role,))


def grant_select(conn, role):
    conn.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}").format(sql.Identifier(role)))


def truncate_all(conn, tbls):
    if tbls:
        conn.execute(sql.SQL("TRUNCATE {}").format(sql.SQL(", ").join(sql.Identifier(t) for t in tbls)))
