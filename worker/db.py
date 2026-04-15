import json
import logging
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)


def connect(database_url: str):
    return psycopg2.connect(database_url)


# ── oauth state ───────────────────────────────────────────────────────────────

def get_oauth_state(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT access_token, refresh_token, expires_at FROM crm.oauth_state WHERE id = 1"
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError("crm.oauth_state is empty — run bootstrap_oauth.py first")
    return {"access_token": row[0], "refresh_token": row[1], "expires_at": row[2]}


def update_oauth_state(conn, access_token: str, refresh_token: str, expires_at: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE crm.oauth_state
               SET access_token = %s,
                   refresh_token = %s,
                   expires_at = %s,
                   updated_at = NOW()
             WHERE id = 1
            """,
            (access_token, refresh_token, expires_at),
        )
    conn.commit()


# ── sync log ──────────────────────────────────────────────────────────────────

def get_last_sync(conn, source: str) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_synced_at FROM crm.sync_log
             WHERE source = %s AND status = 'ok'
             ORDER BY last_synced_at DESC
             LIMIT 1
            """,
            (source,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def log_sync(conn, source: str, synced_at: datetime, status: str, error: str | None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO crm.sync_log (source, last_synced_at, status, error_message) VALUES (%s, %s, %s, %s)",
            (source, synced_at, status, error),
        )
    conn.commit()


# ── raw upserts ───────────────────────────────────────────────────────────────

def _owner_id(d: dict) -> str | None:
    return (
        d.get("user_id")
        or (d.get("user") or {}).get("id")
        or (d.get("owner") or {}).get("id")
    )


def _pipeline_id(d: dict) -> str | None:
    return (
        d.get("pipeline_id")
        or (d.get("deal_pipeline") or {}).get("id")
        or (d.get("pipeline") or {}).get("id")
    )


def _stage_id(d: dict) -> str | None:
    return (
        d.get("stage_id")
        or (d.get("deal_stage") or {}).get("id")
        or d.get("deal_stage_id")
    )


def upsert_raw_deals(conn, deals: list[dict], synced_at: datetime) -> int:
    if not deals:
        return 0
    rows = [
        (
            d["id"],
            json.dumps(d),
            synced_at,
            d.get("updated_at"),
            d.get("status"),
            _pipeline_id(d),
            _stage_id(d),
            _owner_id(d),
        )
        for d in deals
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO crm.raw_deals
                (id, payload, synced_at, updated_at, status, pipeline_id, stage_id, owner_id)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                payload     = EXCLUDED.payload,
                synced_at   = EXCLUDED.synced_at,
                updated_at  = EXCLUDED.updated_at,
                status      = EXCLUDED.status,
                pipeline_id = EXCLUDED.pipeline_id,
                stage_id    = EXCLUDED.stage_id,
                owner_id    = EXCLUDED.owner_id
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def upsert_raw_users(conn, users: list[dict], synced_at: datetime) -> int:
    if not users:
        return 0
    rows = [(u["id"], json.dumps(u), synced_at, u.get("updated_at")) for u in users]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO crm.raw_users (id, payload, synced_at, updated_at)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                payload    = EXCLUDED.payload,
                synced_at  = EXCLUDED.synced_at,
                updated_at = EXCLUDED.updated_at
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def upsert_raw_products(conn, products: list[dict], synced_at: datetime) -> int:
    if not products:
        return 0
    rows = [(p["id"], json.dumps(p), synced_at, p.get("updated_at")) for p in products]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO crm.raw_products (id, payload, synced_at, updated_at)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                payload    = EXCLUDED.payload,
                synced_at  = EXCLUDED.synced_at,
                updated_at = EXCLUDED.updated_at
            """,
            rows,
        )
    conn.commit()
    return len(rows)


def upsert_raw_pipeline(conn, pipeline: dict, synced_at: datetime) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO crm.raw_pipelines (id, payload, synced_at, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                payload    = EXCLUDED.payload,
                synced_at  = EXCLUDED.synced_at,
                updated_at = EXCLUDED.updated_at
            """,
            (pipeline["id"], json.dumps(pipeline), synced_at, pipeline.get("updated_at")),
        )
    conn.commit()


def upsert_raw_pipeline_stages(conn, stages: list[dict], pipeline_id: str, synced_at: datetime) -> int:
    if not stages:
        return 0
    rows = [
        (s["id"], json.dumps(s), synced_at, s.get("updated_at"), pipeline_id)
        for s in stages
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO crm.raw_pipeline_stages (id, payload, synced_at, updated_at, pipeline_id)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                payload     = EXCLUDED.payload,
                synced_at   = EXCLUDED.synced_at,
                updated_at  = EXCLUDED.updated_at,
                pipeline_id = EXCLUDED.pipeline_id
            """,
            rows,
        )
    conn.commit()
    return len(rows)


# ── normalization (raw → relational) ─────────────────────────────────────────
#
# Each function runs a single SQL upsert from the corresponding raw_ table.
# They must be called in dependency order:
#   users → products → pipelines → pipeline_stages → deals → deal_products

def normalize_users(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO crm.users (id, name, is_active)
            SELECT
                id,
                payload->>'name',
                COALESCE(
                    (payload->>'active')::boolean,
                    (payload->>'is_active')::boolean,
                    true
                )
            FROM crm.raw_users
            ON CONFLICT (id) DO UPDATE SET
                name      = EXCLUDED.name,
                is_active = EXCLUDED.is_active
            """
        )
    conn.commit()


def normalize_products(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO crm.products (id, name)
            SELECT id, payload->>'name'
            FROM crm.raw_products
            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
            """
        )
    conn.commit()


def normalize_pipelines(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO crm.pipelines (id, name, position)
            SELECT
                id,
                payload->>'name',
                COALESCE((payload->>'order')::int, 0)
            FROM crm.raw_pipelines
            ON CONFLICT (id) DO UPDATE SET
                name     = EXCLUDED.name,
                position = EXCLUDED.position
            """
        )
    conn.commit()


def normalize_pipeline_stages(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO crm.pipeline_stages (id, pipeline_id, name, position)
            SELECT
                id,
                pipeline_id,
                payload->>'name',
                COALESCE(
                    (payload->>'order')::int,
                    (payload->>'step_id')::int,
                    (payload->>'index')::int,
                    0
                )
            FROM crm.raw_pipeline_stages
            WHERE pipeline_id IS NOT NULL
              AND pipeline_id IN (SELECT id FROM crm.pipelines)
            ON CONFLICT (id) DO UPDATE SET
                pipeline_id = EXCLUDED.pipeline_id,
                name        = EXCLUDED.name,
                position    = EXCLUDED.position
            """
        )
    conn.commit()


def normalize_deals(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO crm.deals
                (id, name, status, pipeline_id, stage_id, owner_id,
                 won_at, created_at, updated_at, amount)
            SELECT
                id,
                payload->>'name',
                payload->>'status',
                COALESCE(
                    payload->>'pipeline_id',
                    payload->'deal_pipeline'->>'id',
                    payload->'pipeline'->>'id'
                ) AS pipeline_id,
                COALESCE(
                    payload->>'stage_id',
                    payload->'deal_stage'->>'id',
                    payload->>'deal_stage_id'
                ) AS stage_id,
                -- set owner_id to NULL if the user isn't in crm.users yet
                CASE WHEN COALESCE(payload->>'owner_id', payload->>'user_id', payload->'user'->>'id', payload->'owner'->>'id')
                              IN (SELECT id FROM crm.users)
                     THEN COALESCE(payload->>'owner_id', payload->>'user_id', payload->'user'->>'id', payload->'owner'->>'id')
                     ELSE NULL
                END AS owner_id,
                NULLIF(COALESCE(payload->>'closed_at', payload->>'win_time'), '')::timestamptz AS won_at,
                NULLIF(payload->>'created_at', '')::timestamptz AS created_at,
                NULLIF(payload->>'updated_at', '')::timestamptz AS updated_at,
                NULLIF(COALESCE(payload->>'total_price', payload->>'amount'), '')::numeric AS amount
            FROM crm.raw_deals
            WHERE
                -- skip deals whose pipeline or stage isn't loaded yet
                COALESCE(payload->>'pipeline_id', payload->'deal_pipeline'->>'id') IS NOT NULL
                AND COALESCE(payload->>'stage_id', payload->'deal_stage'->>'id') IS NOT NULL
                AND COALESCE(payload->>'created_at', '') != ''
                AND COALESCE(payload->>'updated_at', '') != ''
                AND EXISTS (
                    SELECT 1 FROM crm.pipeline_stages ps
                     WHERE ps.id = COALESCE(payload->>'stage_id', payload->'deal_stage'->>'id')
                       AND ps.pipeline_id = COALESCE(payload->>'pipeline_id', payload->'deal_pipeline'->>'id')
                )
            ON CONFLICT (id) DO UPDATE SET
                name        = EXCLUDED.name,
                status      = EXCLUDED.status,
                pipeline_id = EXCLUDED.pipeline_id,
                stage_id    = EXCLUDED.stage_id,
                owner_id    = EXCLUDED.owner_id,
                won_at      = EXCLUDED.won_at,
                created_at  = EXCLUDED.created_at,
                updated_at  = EXCLUDED.updated_at,
                amount      = EXCLUDED.amount
            """
        )
    conn.commit()


def get_all_product_ids(conn) -> list:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM crm.products")
        return [row[0] for row in cur.fetchall()]


def upsert_raw_deal_product_associations(conn, product_id: str, deal_ids: list, now) -> None:
    if not deal_ids:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO crm.raw_deal_products (deal_id, product_id, synced_at)
            VALUES %s
            ON CONFLICT (deal_id, product_id) DO UPDATE SET synced_at = EXCLUDED.synced_at
            """,
            [(deal_id, product_id, now) for deal_id in deal_ids],
        )
    conn.commit()


def normalize_deal_products(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO crm.deal_products (deal_id, product_id)
            SELECT rdp.deal_id, rdp.product_id
            FROM crm.raw_deal_products AS rdp
            WHERE rdp.deal_id   IN (SELECT id FROM crm.deals)
              AND rdp.product_id IN (SELECT id FROM crm.products)
            ON CONFLICT (deal_id, product_id) DO NOTHING
            """
        )
    conn.commit()


def refresh_deal_metrics(conn) -> None:
    with conn.cursor() as cur:
        try:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY derived.deal_metrics")
            conn.commit()
            log.info("Refreshed deal_metrics (concurrent)")
        except Exception as exc:
            conn.rollback()
            log.warning("CONCURRENTLY refresh failed (%s), falling back to non-concurrent", exc)
            cur.execute("REFRESH MATERIALIZED VIEW derived.deal_metrics")
            conn.commit()
            log.info("Refreshed deal_metrics")
