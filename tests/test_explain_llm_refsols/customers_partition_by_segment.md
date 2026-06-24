## Key Facts

- **Source collection:** `customers`
- **Limit:** none
- **Data filters:** none

## Query Summary

Accesses 'customers', partitioned by market_segment, selecting market_segment and computing counting customers records per group as 'n'.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'customers' collection.

- Collection: `customers`

### Step 3 — PartitionBy

Partitions the collection by ['market_segment'].

- Keys: `market_segment`
- Child name: `customers`

> The partition key(s) ['market_segment'] are available inside child scope 'customers' but not outside it.

### Step 4 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `market_segment` → reference
  - `n` → COUNT(`customers`)

## Schema

- **Source collection:** `customers`
- **Output columns:** `market_segment` (string), `n` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
