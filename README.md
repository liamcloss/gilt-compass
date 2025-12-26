# Gilt Compass

**Gilt Compass** is an incremental, broker‑truth decision‑support system for discovering, tracking, and evaluating investable instruments.

It is **not** an auto‑trading system.  
It is designed to surface *what is worth thinking about*, and *why*, with full provenance.

---

## What this system does (in plain English)

1. **Ingests the full Trading 212 ISA universe** (what is actually allowed)
2. **Tracks lifecycle changes** (new, removed, inactive instruments)
3. **Tags price eligibility** per data source (e.g. Yahoo)
4. **Ingests prices incrementally** (never brute‑force)
5. **Scores instruments incrementally** (only when data changes)
6. **Triggers thesis generation only when something meaningful changes**

Everything is:
- incremental
- idempotent
- auditable

---

## What this system deliberately does NOT do

- ❌ No auto‑trading
- ❌ No daily reprocessing of everything
- ❌ No silent filtering
- ❌ No opinionated “buy/sell” logic

This is **decision support**, not execution.

---

## High‑level architecture

Trading 212  
↓  
Universe (broker truth)  
↓  
Lifecycle management  
↓  
Price eligibility tagging  
↓  
Incremental pricing  
↓  
Incremental scoring  
↓  
Change detection  
↓  
Thesis queue

Each step has **one responsibility**.

---

## Pipelines (this matters)

### Weekly — Universe Pipeline
**Purpose:** What exists?

Run **once per week**.

```
python -m src.run_universe_pipeline
```

Steps:
1. Ingest latest Trading 212 snapshot
2. Diff vs previous snapshot
3. Enqueue new instruments
4. Mark removed instruments inactive
5. Ensure price eligibility tags exist

This pipeline is:
- slow
- structural
- low frequency

---

### Daily — Market Pipeline
**Purpose:** What changed?

Run **daily** (or on demand).

```
python -m src.run_market_pipeline
```

Steps:
1. Incremental price ingest (only new / unpriced instruments)
2. Incremental scoring (only instruments with new prices)
3. Trigger thesis generation for meaningful score changes

This pipeline is:
- fast
- incremental
- safe to re‑run

---

## Key concepts (read this once)

### Universe ≠ Priceable ≠ Actionable

An instrument can be:
- in the broker universe
- but not priceable via Yahoo
- and therefore not scorable yet

This is intentional.

Nothing is deleted.  
State is tracked explicitly.

---

### Flags, not filters

You will see columns like:
- `active`
- `price_eligible`
- `price_source`

These are **facts**, not decisions.

Downstream logic consumes flags — it does not re‑filter ad hoc.

---

### Incremental by default

- Prices are fetched **once**
- Known failures are **never retried**
- Scores are computed **only when inputs change**
- Theses are triggered **only on meaningful deltas**

This keeps the system fast and trustworthy at scale.

---

## Project structure (golden view)

```
src/
├── run_universe_pipeline.py        # weekly job
├── run_market_pipeline.py          # daily job
├── manifest.py                     # run provenance
│
├── ingest_universe_trading212.py
├── ingest_prices_from_universe.py
├── score_incremental.py
│
├── universe/
│   ├── diff_trading212_universe.py
│   ├── enqueue_new_trading212.py
│   ├── mark_inactive_trading212.py
│   ├── tag_price_source.py
│   └── __init__.py
│
├── thesis/
│   ├── trigger_thesis_on_score_change.py
│   └── __init__.py
│
└── __init__.py
```

Anything not in this tree is either:
- deprecated
- experimental
- or safe to delete

---

## Outputs you should care about

```
data/
├── universe/
│   ├── gilt_universe.csv           # authoritative universe
│   └── raw/                        # Trading 212 snapshots
│
├── prices/
│   ├── prices_1y.parquet
│   ├── price_ingest_failures.csv
│   └── price_ingest_attempted.csv
│
outputs/
├── scores_current.parquet
├── scores_previous.parquet
├── thesis_queue.csv
└── manifests/
```

If these look sane, the system is healthy.

---

## How to work with this system safely

### Do
- Run **weekly universe** separately from **daily market**
- Inspect failure logs — they are data
- Treat score deltas as signals, not commands

### Don’t
- Reintroduce batch scoring
- Add ad‑hoc filters
- Delete instruments
- Auto‑trade

---

## Philosophy (this is intentional)

> **Discovery is slow.  
> Pricing is incremental.  
> Thinking is scarce.**

Gilt Compass is designed to respect all three.

---
