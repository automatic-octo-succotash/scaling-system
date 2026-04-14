import logging
from datetime import datetime, timedelta, timezone

from worker import db
from worker.client import RDClient

log = logging.getLogger(__name__)

# Fetch deals closed within this many days of today (slightly over 12 months
# to avoid missing deals right on the boundary).
_ROLLING_DAYS = 366


def sync_users(client: RDClient, conn, now: datetime) -> None:
    log.info("Syncing users...")
    users = list(client.paginate("/crm/v2/users"))
    count = db.upsert_raw_users(conn, users, now)
    db.normalize_users(conn)
    log.info("Users: %d upserted", count)


def sync_products(client: RDClient, conn, now: datetime) -> None:
    log.info("Syncing products...")
    products = list(client.paginate("/crm/v2/products"))
    count = db.upsert_raw_products(conn, products, now)
    db.normalize_products(conn)
    log.info("Products: %d upserted", count)


def sync_deals(client: RDClient, conn, now: datetime, last_sync: datetime | None) -> None:
    cutoff = (now - timedelta(days=_ROLLING_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Won deals — rolling 12-month window.
    # On subsequent runs we also filter by updated_at to avoid re-fetching the
    # entire history. A 1-hour buffer guards against clock skew.
    won_filter = f"status:won AND closed_at>={cutoff}"
    if last_sync:
        since = (last_sync - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        won_filter += f" AND updated_at>={since}"

    log.info("Syncing won deals (filter: %s)...", won_filter)
    won = list(client.paginate("/crm/v2/deals", {"filter": won_filter, "order": "updated_at:asc"}))
    count = db.upsert_raw_deals(conn, won, now)
    log.info("Won deals: %d upserted", count)

    # Ongoing deals — always a full refresh because a deal's status can flip
    # from ongoing to won/lost between runs.
    log.info("Syncing ongoing deals...")
    ongoing = list(client.paginate("/crm/v2/deals", {"filter": "status:ongoing", "order": "updated_at:asc"}))
    count = db.upsert_raw_deals(conn, ongoing, now)
    log.info("Ongoing deals: %d upserted", count)


def sync_pipelines(client: RDClient, conn, now: datetime) -> None:
    # Discover pipeline IDs from whatever is in raw_deals — no list-all endpoint
    # exists in the RD Station CRM v2 API.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT pipeline_id FROM crm.raw_deals WHERE pipeline_id IS NOT NULL"
        )
        pipeline_ids = [row[0] for row in cur.fetchall()]

    log.info("Syncing %d pipeline(s)...", len(pipeline_ids))

    for pid in pipeline_ids:
        try:
            pipeline, _ = client.get(f"/crm/v2/pipelines/{pid}")
            db.upsert_raw_pipeline(conn, pipeline, now)
            db.normalize_pipelines(conn)

            # Stages may be embedded in the pipeline response (preferred) or
            # available at a sub-endpoint. Try both.
            stages = (
                pipeline.get("stages")
                or pipeline.get("deal_stages")
                or []
            )
            if not stages:
                try:
                    body, _ = client.get(f"/crm/v2/pipelines/{pid}/stages")
                    stages = body if isinstance(body, list) else body.get("stages", [])
                except Exception as exc:
                    log.warning("Could not fetch stages for pipeline %s via sub-endpoint: %s", pid, exc)

            db.upsert_raw_pipeline_stages(conn, stages, pid, now)
            db.normalize_pipeline_stages(conn)
            log.info("Pipeline %s: %d stage(s)", pid, len(stages))

        except Exception as exc:
            log.error("Failed to sync pipeline %s: %s", pid, exc)


def run(client: RDClient, conn) -> None:
    now = datetime.now(timezone.utc)
    last_sync = db.get_last_sync(conn, "worker")

    try:
        sync_users(client, conn, now)
        sync_products(client, conn, now)
        sync_deals(client, conn, now, last_sync)
        sync_pipelines(client, conn, now)

        # Normalize deals and their products after all reference data is loaded.
        log.info("Normalizing deals...")
        db.normalize_deals(conn)
        db.normalize_deal_products(conn)

        db.refresh_deal_metrics(conn)
        db.log_sync(conn, "worker", now, "ok", None)
        log.info("Sync complete")

    except Exception as exc:
        log.exception("Sync failed: %s", exc)
        db.log_sync(conn, "worker", now, "error", str(exc))
        raise
