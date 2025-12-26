# Gilt Compass — Architecture

This document explains **how Gilt Compass works**, how data flows through the system, and why certain design decisions were made.

It is intentionally opinionated.

---

## System intent

Gilt Compass exists to answer one question well:

> **“What is worth thinking about right now?”**

It separates:
- discovery (what exists)
- measurement (what changed)
- judgement (what deserves attention)

---

## High-level data flow

```
Trading 212
   ↓
Universe snapshot
   ↓
Diff & lifecycle management
   ↓
Gilt universe (authoritative)
   ↓
Price eligibility tagging
   ↓
Incremental pricing
   ↓
Incremental scoring
   ↓
Score change detection
   ↓
Thesis queue
```

Each stage is:
- deterministic
- idempotent
- single-purpose

---

## Pipelines

### 1. Weekly Universe Pipeline

**Purpose:** Structural truth — *what exists?*

```bash
python -m src.run_universe_pipeline
```

Responsibilities:
- Ingest latest Trading 212 snapshot
- Detect new instruments
- Enqueue additions
- Mark removed instruments inactive
- Ensure price eligibility flags exist

Characteristics:
- Slow
- Low frequency
- Safe to re-run

---

### 2. Daily Market Pipeline

**Purpose:** Behavioural truth — *what changed?*

```bash
python -m src.run_market_pipeline
```

Responsibilities:
- Incremental price ingest
- Incremental scoring
- Trigger thesis generation on meaningful score changes

Characteristics:
- Fast
- Incremental
- Quiet when nothing matters

---

## Universe model

The universe is **broker-truth**, not opinionated.

Key columns:
- `instrument_id` – stable identifier
- `active` – lifecycle state
- `price_eligible` – whether a price source can cover it
- `price_source` – which source (e.g. Yahoo)

Nothing is deleted. Instruments only move through states.

---

## Pricing model

- Prices are sourced incrementally
- Known failures are cached and never retried
- Pricing capability is a property of the instrument, not the pipeline

This avoids brute-force ingestion and API abuse.

---

## Scoring model

- Scoring reacts to **new price data only**
- Scores are persisted as state, not recomputed wholesale
- Normalisation happens within the scoring batch

Scoring is **detection**, not prediction.

---

## Thesis triggering

A thesis is generated only when:
- an instrument is newly scored, or
- its score changes materially

This ensures thinking is:
- scarce
- focused
- auditable

The output is a queue, not an action.

---

## What this architecture avoids

- Batch recomputation
- Hidden filters
- Implicit decisions
- Auto-trading

Each of these would reduce trust and increase noise.

---

## Operating principles

> **Discovery is slow.**  
> **Pricing is incremental.**  
> **Thinking is scarce.**

If the system is quiet, it is working correctly.

---

## Extensibility (future-safe)

The architecture supports, but does not require:
- Additional price providers
- Confidence bands on scores
- Manual approval loops
- Rank-crossing triggers
- Thesis expiry / cooldowns

These are deliberate omissions, not limitations.

---

## Final word

Gilt Compass is designed to be boring when nothing matters.

That is the highest compliment an analytical system can earn.

