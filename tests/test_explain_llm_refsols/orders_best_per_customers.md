## Key Facts

- **Source collection:** `customers`
- **Output collection:** `orders`
- **Limit:** none
- **Data filters:** RANKING(by=(total_price.DESC(na_pos='last')), levels=1, allow_ties=False) == 1

## Query Summary

Accesses 'customers', then subcollection filtered to rows where RANKING(by=(total_price.DESC(na_pos='last')), levels=1, allow_ties=False) == 1, selecting key.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'customers' collection.

- Collection: `customers`

### Step 3 — SubCollection

Traverses the 'orders' relationship from 'customers' to 'orders'.

- `customers` → `orders` via `orders`

### Step 4 — Where

Filters rows to those matching the given conditions.

- Condition: `RANKING(by=(total_price.DESC(na_pos='last')), levels=1, allow_ties=False) == 1`

> Note: this step uses a window function (e.g. RANKING, PERCENTILE). PyDough window functions commonly use 'per=' to rank within partitions of an ancestor collection rather than globally. The partition scope is resolved at SQL generation time and is NOT shown in the expression text — check the source code for a 'per=' argument before assuming this is a global rank.

### Step 5 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `key` → reference

## Schema

- **Source collection:** `customers`
- **Output columns:** `key` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
