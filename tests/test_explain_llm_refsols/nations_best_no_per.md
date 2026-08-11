## Key Facts

- **Source collection:** `nations`
- **Limit:** none
- **Data filters:** RANKING(by=(key.ASC(na_pos='first')), allow_ties=False) == 1

## Query Summary

Accesses 'nations', filtered to rows where RANKING(by=(key.ASC(na_pos='first')), allow_ties=False) == 1.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'nations' collection.

- Collection: `nations`

### Step 3 — Where

Filters rows to those matching the given conditions.

- Condition: `RANKING(by=(key.ASC(na_pos='first')), allow_ties=False) == 1`

> Note: this step uses a window function (e.g. RANKING, PERCENTILE). PyDough window functions commonly use 'per=' to rank within partitions of an ancestor collection rather than globally. The partition scope is resolved at SQL generation time and is NOT shown in the expression text — check the source code for a 'per=' argument before assuming this is a global rank.

## Schema

- **Source collection:** `nations`
- **Output columns:** _(none)_
- **Ordering:** _(none)_
- **Limit:** _(none)_
