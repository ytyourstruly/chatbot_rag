# Database Configuration Guide

## Overview

This application uses **asyncpg** to connect to PostgreSQL asynchronously. All database configuration is centralized in the `.env` file and validated on startup.

## Configuration Files

### 1. `.env` (Environment Variables)
**File**: [.env](.env)

Required variables:
```bash
DATABASE_URL=postgresql://enki_viewer:PASSWORD@host:port/contractor-service
OPENAI_API_KEY=sk-...
```

The `DATABASE_URL` must follow this format:
```
postgresql://[user]:[password]@[host]:[port]/[database]
```

### 2. `app/config.py` (Settings Validation)
**File**: [app/config.py](app/config.py)

- Loads all environment variables from `.env`
- Validates `DATABASE_URL` and `OPENAI_API_KEY` formats
- Exposes pool configuration:
  - `db_pool_min_size`: Minimum idle connections (default: 1)
  - `db_pool_max_size`: Maximum total connections (default: 10)
  - `db_connection_timeout`: Connection attempt timeout in seconds (default: 10)
  - `db_statement_timeout`: SQL execution timeout in seconds (default: 30)

### 3. `app/database.py` (Connection Pool)
**File**: [app/database.py](app/database.py)

Manages the global asyncpg connection pool with:
- **Startup**: `create_pool()` called in `@cl.on_chat_start()`
- **Shutdown**: `close_pool()` called in `@cl.on_chat_end()`
- **Error Handling**: Connection failures logged but don't crash the app

## Connection Flow

```
┌─────────────────────────────────────────┐
│  .env file                              │
│  DATABASE_URL=postgresql://...          │
└──────────────┬──────────────────────────┘
               │
               v
┌─────────────────────────────────────────┐
│  app/config.py                          │
│  Settings.database_url (with validation)│
└──────────────┬──────────────────────────┘
               │
               v
┌─────────────────────────────────────────┐
│  app/database.py                        │
│  create_pool() → asyncpg.create_pool()  │
│  _pool: Optional[asyncpg.Pool]          │
└──────────────┬──────────────────────────┘
               │
               v
┌─────────────────────────────────────────┐
│  app/analytics.py                       │
│  get_pool() → execute SQL queries       │
└─────────────────────────────────────────┘
```

## Startup Sequence

1. **Load .env**: `load_dotenv()` in `chainlit_app.py`
2. **Validate Settings**: `Settings()` initialized in `app/config.py`
3. **Create Pool**: `await create_pool()` in `@cl.on_chat_start()`
4. **Use Pool**: Analytics queries call `get_pool()` and execute SQL

## Error Handling

### Invalid DATABASE_URL
```
✗ Invalid DATABASE_URL format
Expected: postgresql://user:pass@host:port/db
```
→ Verify format and credentials in `.env`

### Connection Failed
```
✗ PostgreSQL connection failed — analytics disabled.
Check: DATABASE_URL, host/port reachable, credentials valid, database exists.
```
→ Verify:
- PostgreSQL server is running
- Host and port are correct
- Credentials (user/password) are valid
- Database exists and user has access
- Network connectivity to the host

### Pool Not Available
```
❌ Database pool not available.
Check: DATABASE_URL in .env, PostgreSQL running, credentials valid.
```
→ Triggered when `get_pool()` is called before `create_pool()` completes

## Features

✅ **Validation**: DATABASE_URL and API keys validated on startup  
✅ **Type Safety**: Settings use Pydantic for type hints and validation  
✅ **Async**: Non-blocking asyncpg connection pool  
✅ **Resilient**: Connection failures don't crash the app  
✅ **Configurable**: All pool parameters in one place  
✅ **Secure**: Read-only PostgreSQL role (`enki_viewer`)  
✅ **Instrumented**: Detailed logging at each step  

## Production Checklist

- [ ] DATABASE_URL in `.env` uses strong passwords
- [ ] Database user has minimal required permissions (read-only)
- [ ] PostgreSQL server is running and accessible
- [ ] Connection timeout is appropriate for your network
- [ ] Pool size matches expected concurrent usage
- [ ] Logs are monitored for connection failures
- [ ] `.env` is never committed to version control

## Testing the Connection

To test if the database connection is working:

```python
from app.config import settings
from app.database import create_pool, is_db_available

# Test 1: Settings loaded correctly
print(f"DATABASE_URL: {settings.database_url}")

# Test 2: Pool creation
await create_pool()
print(f"Pool available: {is_db_available()}")

# Test 3: Execute a query
from app.database import get_pool
pool = get_pool()
result = await pool.fetchval("SELECT 1")
print(f"Query result: {result}")
```

## Security Notes

- **Read-Only Role**: Database connections use `enki_viewer` (read-only role)
- **No Parameter Injection**: All SQL queries are hardcoded, no user input in queries
- **Statement Cache Disabled**: `statement_cache_size=0` prevents prepared statement attacks
- **Environment Variables**: Never hardcode credentials; always use `.env`

## References

- [asyncpg Documentation](https://magicstack.github.io/asyncpg/)
- [PostgreSQL Connection Strings](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING)
- [Pydantic Settings](https://docs.pydantic.dev/latest/api/pydantic_settings/)
