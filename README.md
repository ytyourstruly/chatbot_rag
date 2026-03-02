# Website Chatbot — MVP

A streaming analytics chatbot built with LangChain, Chainlit, asyncpg, and OpenAI.

```
docs/                  ← Put your .md / .html / .txt documentation here
app/
  config.py            ← Settings (loads .env)
  database.py          ← asyncpg pool + hardcoded SQL queries
  cache.py             ← In-memory TTL cache
  analytics.py         ← Intent detection + DB query execution
  vector_store.py      ← document loader
  llm.py               ← Streaming OpenAI chat
  chatbot.py           ← Orchestration pipeline
scripts/
  ingest.py            ← One-time doc ingestion → FAISS index
chainlit_app.py        ← Chainlit UI frontend
```

---

## Prerequisites

- Python 3.11
- PostgreSQL with a **read-only** role
- An OpenAI API key

---

## Setup

### 1. Clone & install dependencies

```bash
git clone <repo>
cd chatbot
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env and fill in:
#   OPENAI_API_KEY
#   DATABASE_URL   (use the read-only PostgreSQL user)
```

### 3. Create the read-only PostgreSQL role (run as superuser)

```sql
CREATE ROLE readonly_user WITH LOGIN PASSWORD 'strongpassword';
GRANT CONNECT ON DATABASE your_database TO readonly_user;
GRANT USAGE ON SCHEMA public TO readonly_user;
GRANT SELECT ON contracts TO readonly_user;
-- No INSERT / UPDATE / DELETE / DDL privileges
```

### 4. Add documentation files

Place your website documentation (`.md`, `.html`, or `.txt` files) in the
`docs/` folder. A sample file is already included.

### 5. Ingest documents

```bash
python scripts/ingest.py
```

Run this whenever docs change to refresh the document store used by the app.

---

## Running

### Option A — Chainlit UI (recommended for end users)

```bash
chainlit run chainlit_app.py --port 8001
```

Open http://localhost:8001

<!-- FastAPI/headless mode removed from this README -->

---

## Decision Flow

```
User question
  │
  ▼
Keyword analytics detection
  │
  ├─ "total amount" / "total revenue" ──► SUM(amount) from DB
  ├─ "total ports" / "how many ports"  ──► SUM(total_ports_count) from DB
  ├─ other analytics signal            ──► "Only 2 queries supported"
  │
  └─ not analytical ──────────────────► Stream LLM (general answer)
```

---

## Security Notes

- PostgreSQL role has **SELECT-only** access on `contracts` table.
- Only **two hardcoded SQL statements** are ever executed — no dynamic SQL.
- Zero user input is passed into any query.
- 30-second query timeout enforced via `asyncpg`.
- Analytics results are cached for 5 minutes (configurable via `CACHE_TTL_SECONDS`).

---

## Scaling Notes (for future)

| Concern | Current (MVP) | Next step |
|---|---|---|
| Vector index | In-process, in-memory | Migrate to Pinecone / pgvector |
| Analytics cache | In-memory dict | Redis |
| DB connections | asyncpg pool | PgBouncer in front |
| LLM | OpenAI API | Add fallback model |
| Deployment | Single process | Docker + Gunicorn workers |
