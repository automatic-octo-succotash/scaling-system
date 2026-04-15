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
    # Date filters use bracket params (closed_at[gte], updated_at[gte]) not
    # inline AND syntax, which the API does not support.
    won_params: dict = {"filter": "status:won", "closed_at[gte]": cutoff, "order": "updated_at:asc"}
    if last_sync:
        since = (last_sync - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        won_params["updated_at[gte]"] = since

    log.info("Syncing won deals (closed_at >= %s)...", cutoff)
    won = list(client.paginate("/crm/v2/deals", won_params))
    count = db.upsert_raw_deals(conn, won, now)
    log.info("Won deals: %d upserted", count)

    # Ongoing deals — always a full refresh because a deal's status can flip
    # from ongoing to won/lost between runs.
    log.info("Syncing ongoing deals...")
    ongoing = list(client.paginate("/crm/v2/deals", {"filter": "status:ongoing", "order": "updated_at:asc"}))
    count = db.upsert_raw_deals(conn, ongoing, now)
    log.info("Ongoing deals: %d upserted", count)


def sync_pipelines(client: RDClient, conn, now: datetime) -> None:
    pipelines = list(client.paginate("/crm/v2/pipelines"))
    log.info("Syncing %d pipeline(s)...", len(pipelines))

    for pipeline in pipelines:
        pid = pipeline["id"]
        try:
            db.upsert_raw_pipeline(conn, pipeline, now)

            stages = list(client.paginate(f"/crm/v2/pipelines/{pid}/stages"))
            db.upsert_raw_pipeline_stages(conn, stages, pid, now)
            log.info("Pipeline %s: %d stage(s)", pid, len(stages))

        except Exception as exc:
            log.error("Failed to sync pipeline %s: %s", pid, exc)

    db.normalize_pipelines(conn)
    db.normalize_pipeline_stages(conn)


def sync_deal_products(client: RDClient, conn, now: datetime) -> None:
    product_ids = db.get_all_product_ids(conn)
    log.info("Syncing deal-product associations for %d product(s)...", len(product_ids))
    total = 0
    for product_id in product_ids:
        try:
            deals = list(client.paginate("/crm/v2/deals", {"product_ids": product_id}))
            deal_ids = [d["id"] for d in deals if "id" in d]
            db.upsert_raw_deal_product_associations(conn, product_id, deal_ids, now)
            log.info("Product %s: %d deal(s)", product_id, len(deal_ids))
            total += len(deal_ids)
        except Exception as exc:
            log.warning("Failed to fetch deals for product %s: %s", product_id, exc)
    log.info("Deal-product associations: %d total", total)


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
        sync_deal_products(client, conn, now)
        db.normalize_deal_products(conn)

        db.refresh_deal_metrics(conn)
        db.log_sync(conn, "worker", now, "ok", None)
        log.info("Sync complete")

    except Exception as exc:
        log.exception("Sync failed: %s", exc)
        db.log_sync(conn, "worker", now, "error", str(exc))
        raise
