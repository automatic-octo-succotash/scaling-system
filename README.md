# scaling-system

RD Station CRM ETL worker for MLC Logística. Fetches deals, users, products, and pipelines from the [RD Station CRM v2 API](https://developers.rdstation.com/reference/visao-geral-crm) and loads them into PostgreSQL, then refreshes the `derived.deal_metrics` materialized view.

Deployed as a Kubernetes CronJob (every 5 minutes) via [cuddly-carnival](https://github.com/automatic-octo-succotash/cuddly-carnival).

## What it does

Each run:

1. Reads OAuth2 tokens from `crm.oauth_state` in PostgreSQL (refreshes proactively if expiry is within 5 minutes)
2. Syncs users, products, won deals (rolling 12 months), ongoing deals, and pipelines/stages from the RD Station API
3. Upserts raw JSON into `crm.raw_*` tables
4. Normalizes into `crm.deals`, `crm.users`, `crm.products`, `crm.pipelines`, `crm.pipeline_stages`
5. Refreshes `derived.deal_metrics`
6. Logs the sync result to `crm.sync_log`

## Project layout

```
worker/
  main.py      Entry point — reads env, boots client, runs sync
  client.py    RDClient: HTTP + pagination + token refresh
  sync.py      Per-resource sync logic
  db.py        PostgreSQL helpers — upserts, normalization, OAuth state
bootstrap_oauth.py   One-time OAuth2 token seeding script (run locally)
```

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `DATABASE_URL` | yes | PostgreSQL connection string |
| `RD_CLIENT_ID` | yes | RD Station app client ID |
| `RD_CLIENT_SECRET` | yes | RD Station app client secret |

## Token seeding (first-time setup)

OAuth2 tokens must be seeded once into `crm.oauth_state` before the worker can run. Use `bootstrap_oauth.py`:

```bash
# Optional: set these to skip interactive prompts
export RD_CLIENT_ID=<your-client-id>
export RD_CLIENT_SECRET=<your-client-secret>
export KUBECONFIG=~/.kube/config   # path to cluster kubeconfig

python bootstrap_oauth.py
```

The script prints an authorization URL. Open it in a browser, grant access, then paste the `code` from the redirect URL. It exchanges the code for tokens and inserts them into the database via `kubectl exec`.

After seeding, the worker handles all subsequent token refreshes automatically.

## CI / image builds

Pushes to `main` trigger the [publish-image](.github/workflows/publish-image.yml) workflow, which builds a multi-arch image (`linux/amd64`, `linux/arm64`) and pushes to GHCR:

```
ghcr.io/automatic-octo-succotash/scaling-system:sha-<commit>
```

To deploy a new version, update the image tag in [cuddly-carnival/k8s/base/worker/cronjob.yaml](https://github.com/automatic-octo-succotash/cuddly-carnival/blob/main/k8s/base/worker/cronjob.yaml).

## Local development

```bash
pip install -r requirements.txt

DATABASE_URL=postgres://... \
RD_CLIENT_ID=... \
RD_CLIENT_SECRET=... \
python -m worker.main
```
