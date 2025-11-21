# Silver Layer dbt Transformation

Transforms raw Amplitude event data (JSON) into analytics-ready dimensional tables.

## Project Structure

### Staging Layer (`models/staging/`)
- **`stg_amplitude_events.sql`** - Parses JSON into flat columns (1:1 with source)
- Materialized as views
- Schema: `staging`

### Intermediate Layer (`models/intermediate/`)
Six dimensional/fact tables materialized in the `silver` schema:

1. **`int_ip_addresses`** - Unique IP addresses (table)
2. **`int_users`** - User dimension with latest device/IP (table)
3. **`int_sessions`** - Sessionized events with user journey (incremental)
4. **`int_event_properties`** - Event property combinations (table)
5. **`int_events`** - Deduplicated event fact table (incremental)
6. **`int_companies`** - Companies from email domains (table)

## Data Model

```
stg_amplitude_events (view)
    ↓
int_ip_addresses → int_users → int_companies
    ↓                ↓
int_event_properties  int_events (fact) ← int_sessions
```

**Surrogate Keys:** Generated using `dbt_utils.generate_surrogate_key()` macro

**Incremental Models:** `int_events` and `int_sessions` append new data based on `event_time`

## Running Models

```bash
# Install dependencies first
dbt deps

# Run all models
dbt run

# Full refresh incremental models
dbt run --full-refresh

# Run specific layers
dbt run --select staging
dbt run --select intermediate

# Test data quality
dbt test

# Generate and view documentation
dbt docs generate
dbt docs serve
```

## Development Workflow

1. Make changes to model SQL
2. Run model: `dbt run --select model_name`
3. Test: `dbt test --select model_name`
4. Document in corresponding `.yml` file
5. Commit to version control

## Testing

- **Primary keys:** `unique` + `not_null` tests on all surrogate keys
- **Relationships:** Foreign key integrity between tables
- **Custom test:** `assert_no_duplicate_events` checks UUID uniqueness

## Incremental Strategy

**int_events:**
- Filters on `event_time > max(event_time)`
- Unique key: `event_id`

**int_sessions:**
- Filters on `event_time > max(session_end)`
- Unique key: `session_id`

Run `--full-refresh` if schema changes or data issues occur.

## Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt_utils Package](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/)
