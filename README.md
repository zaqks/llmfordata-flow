# llmfordata-flow

**Scrape, analyze, report, and notify on LLM-for-data trends.**

This is a lightweight, end-to-end workflow that scrapes multiple data sources, analyzes items with an LLM, generates reports and charts, and sends browser notifications to subscribed users. It orchestrates several pipelines working together: ingestion, LLM analysis, report generation, and notifications.

---
## Key Features

* **Multi-source ingestion** – arXiv, Hugging Face, PapersWithCode, Databricks, Snowflake, NVIDIA, SemanticScholar.
* **LLM-powered analysis** – Extracts topics, keywords, summaries, and emerging algorithms (using OpenRouter for LLM access).
* **Report generation** – Produces markdown reports, charts, and stores them in the database.
* **Push notifications** – Sends browser notifications via VAPID keys and `pywebpush`.

---

## Screenshots

![alt text](docs/Screenshot_20260301_022337.png)
![alt text](docs/Screenshot_20260301_022352.png)
![alt text](docs/Screenshot_20260301_022324.png)


---

## Architecture overview
```txt
        ┌───────────────────────────────┐
        │          Data Sources         │
        │   arXiv, HuggingFace,         │
        │ PapersWithCode, Databricks,   │
        │ Snowflake, NVIDIA, etc.       │
        └───────────────┬───────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │           Ingestion           │
        │           (plombery_1)        │
        │   Scrape, normalize, store    │
        └───────────────┬───────────────┘
                        │
                        ▼
        ┌─────────────────────────────────┐
        │         LLM Analysis            │
        │           (plombery_2)          │
        │  Extract topics, keywords,      │
        │  summaries, emerging algorithms │
        └───────────────┬─────────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │       Report Generation       │
        │      Markdown + Charts        │
        │      Save artifacts to DB     │
        └───────────────┬───────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │        Notifications          │
        │         Browser Push          │
        │     Alert subscribed users    │
        └───────────────────────────────┘
```

---

## How It Works

1. **Ingestion** – Pipelines scrape external sources and insert normalized rows into the local datasink.
2. **Analysis** – Newly ingested rows trigger the LLM analysis pipeline (`datasource_analysis_llm`) to generate structured insights.
3. **Persistence** – Analysis results (topics, keywords, summaries, impact) are saved in the database.
4. **Report Generation** – Aggregates analyzed data, creates markdown reports + charts, stores artifacts, and marks rows as exported.
5. **Notifications** – The API service sends browser push notifications to subscribed clients.

---

## Repository Layout

```text
docker-compose.yml          # Service composition: postgres, api, plombery_1, plombery_2
services/api/               # FastAPI app, DB models, push endpoints, static frontend
services/plombery_1/        # Ingestion flows: src/flows/ingestion/*
services/plombery_2/        # Analysis & report flows: src/flows/analysis, src/flows/gen
```

---

## Getting Started

### Prerequisites

* Docker & Docker Compose (`docker compose`)
* `.env` file (copy from `.env.example`)
* Python (optional, for local development without Docker)

### Setup

1. **Copy and configure environment variables**

```bash
cp .env.example .env
# Edit `.env` to set DATABASE_URL, VAPID paths, LLM keys, HOST variables
```

2. **Generate VAPID keys for push notifications**

```bash
python services/api/generate_vapid_keys.py
# Keep or move vapid_private.pem & vapid_public.txt
# Ensure .env points to correct paths
```

3. **Start services with Docker Compose**

```bash
docker compose up --build
```

Service URLs:

* API: `http://localhost:8000`
* Plombery ingestion (plombery_1): `http://localhost:8001`
* Plombery analysis/reporting (plombery_2): `http://localhost:8002`

---

## Important Environment Variables

* `DATABASE_URL` – SQLAlchemy DB URL (postgresql://user:pass@postgres:5432/dbname)
* `VAPID_PRIVATE_PATH` / `VAPID_PUBLIC_PATH` – Paths to VAPID keys for browser push
* `VAPID_CLAIM_EMAIL` – Contact email for VAPID claims
* `OPENROUTER_MODEL`, `OPENROUTER_API_KEY` – LLM configuration. This project uses
        **OpenRouter** as the LLM gateway; set `OPENROUTER_MODEL` to the model id and
        `OPENROUTER_API_KEY` to your OpenRouter API key. See `services/plombery_2`
        utilities for how the key/model are consumed by the analysis and report
        pipelines.
* `HOST1`, `HOST2` – Host URLs used for pipeline triggers

See `.env.example` for full configuration.

---

## Plombery Pipelines Examples

* **`arxiv_ingestion`** – Scrapes arXiv and normalizes rows.
* **`datasource_analysis_llm`** – Calls LLM to generate structured insights (topics, keywords, summaries, emerging algorithms).
* **`report_generation`** – Aggregates analyses, creates markdown reports + charts, stores artifacts, and triggers notifications.

Pipelines are defined under `services/plombery_*/src/flows` using Plombery decorators (`@task`, `register_pipeline`) with HTTP helpers to trigger downstream services.

---

## Customization: prompts and templates

- The analysis and report-generation prompts are editable templates used by the
        pipelines. You can customize how the LLM is prompted by changing the files:

        - `services/plombery_2/src/flows/analysis/prompt.txt` — template used by the
                `datasource_analysis_llm` pipeline.
        - `services/plombery_2/src/flows/gen/prompt_markdown.txt` — template used by
                the `report_generation` pipeline when creating markdown reports.

- Both pipelines also accept a `PROMPT` parameter (see each flow's
        `InputParams`) so you can override the built-in template at runtime when
        invoking the pipeline (or via environment-aware configuration). Edit the
        template files for persistent changes or pass a `PROMPT` parameter for ad‑hoc
        variations (useful for A/B testing prompts or experimenting with different
        instruction styles).

