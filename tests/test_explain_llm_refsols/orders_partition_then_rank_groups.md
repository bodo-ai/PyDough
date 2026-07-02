## Key Facts

- **Source collection:** `customers`
- **Output collection:** `orders`
- **Limit:** none
- **Data filters:** none
- **Post-compute filters:** RANKING(by=(n.DESC(na_pos='last'))) == 1

## Query Summary

Accesses 'customers', then subcollection filtered to rows where RANKING(by=(n.DESC(na_pos='last'))) == 1, partitioned by order_status, selecting order_status and computing counting orders records per group as 'n'.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'customers' collection.

- Collection: `customers`

### Step 3 — SubCollection

Traverses the 'orders' relationship from 'customers' to 'orders'.

- `customers` → `orders` via `orders`

### Step 4 — PartitionBy

Partitions the collection by ['order_status'].

- Keys: `order_status`
- Partition name: `g`
- Child name: `orders`

> The partition key(s) ['order_status'] identify each group and are accessible at the group level. Row-level data is accessible via the child collection 'orders'; aggregating over it (e.g. COUNT('orders')) operates on the rows within that group.

### Step 5 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `order_status` → reference
  - `n` → COUNT(`orders`) _(implicitly scoped via relationship)_

> Note: 'n' aggregates 'orders' — scoping is implicit via 'orders' relationship navigation, not an explicit filter.

### Step 6 — Where

Filters rows to those matching the given conditions.

- Condition: `RANKING(by=(n.DESC(na_pos='last'))) == 1`

> Note: this step uses a window function (e.g. RANKING, PERCENTILE). PyDough window functions commonly use 'per=' to rank within partitions of an ancestor collection rather than globally. The partition scope is resolved at SQL generation time and is NOT shown in the expression text — check the source code for a 'per=' argument before assuming this is a global rank.

### Step 7 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `order_status` → reference

## Schema

- **Source collection:** `customers`
- **Output columns:** `order_status` (string)
- **Ordering:** _(none)_
- **Limit:** _(none)_
