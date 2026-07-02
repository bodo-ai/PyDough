## Key Facts

- **Source collection:** `nations`
- **Limit:** 1
- **Data filters:** none

## Query Summary

Accesses 'nations', selecting name and computing counting customers records as 'n', keeping the top 1 rows by n desc.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'nations' collection.

- Collection: `nations`

### Step 3 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `name` → reference
  - `n` → COUNT(`customers`) _(implicitly scoped via relationship)_

> Note: 'n' aggregates 'customers' — scoping is implicit via 'customers' relationship navigation, not an explicit filter.

### Step 4 — TopK

Sorts the collection and keeps the top 1 records.

- Limit: 1
- Order by: `n` DESC NULLS LAST

## Schema

- **Source collection:** `nations`
- **Output columns:** `name` (string), `n` (numeric)
- **Ordering:** `n` DESC NULLS LAST
- **Limit:** 1
