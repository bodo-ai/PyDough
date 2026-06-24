## Key Facts

- **Source collection:** `customers`
- **Limit:** none
- **Data filters:** market_segment == 'BUILDING'
- **Post-compute filters:** RANKING(by=(n.ASC(na_pos='first'))) == 1

## Query Summary

Accesses 'customers', filtered to rows where market_segment == 'BUILDING' and RANKING(by=(n.ASC(na_pos='first'))) == 1, selecting name and computing counting orders records as 'n'.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'customers' collection.

- Collection: `customers`

### Step 3 — Where

Filters rows to those matching the given conditions.

- Condition: `market_segment == 'BUILDING'`

### Step 4 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `name` → reference
  - `n` → COUNT(`orders`) _(implicitly scoped via relationship)_

> Note: 'n' aggregates 'orders' — scoping is implicit via 'orders' relationship navigation, not an explicit filter.

### Step 5 — Where

Filters rows to those matching the given conditions.

- Condition: `RANKING(by=(n.ASC(na_pos='first'))) == 1`

> Note: this step uses a window function (e.g. RANKING, PERCENTILE). PyDough window functions commonly use 'per=' to rank within partitions of an ancestor collection rather than globally. The partition scope is resolved at SQL generation time and is NOT shown in the expression text — check the source code for a 'per=' argument before assuming this is a global rank.

### Step 6 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `name` → reference

## Schema

- **Source collection:** `customers`
- **Output columns:** `name` (string)
- **Ordering:** _(none)_
- **Limit:** _(none)_
