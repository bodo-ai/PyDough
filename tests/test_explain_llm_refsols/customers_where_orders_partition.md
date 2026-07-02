## Key Facts

- **Source collection:** `customers`
- **Output collection:** `orders`
- **Limit:** 1
- **Data filters:** market_segment == 'BUILDING'

## Query Summary

Accesses 'customers', filtered to rows where market_segment == 'BUILDING', partitioned by order_status, selecting order_status and computing counting orders records per group as 'n', keeping the top 1 rows by n desc.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'customers' collection.

- Collection: `customers`

### Step 3 — Where

Filters rows to those matching the given conditions.

- Condition: `market_segment == 'BUILDING'`

### Step 4 — SubCollection

Traverses the 'orders' relationship from 'customers' to 'orders'.

- `customers` → `orders` via `orders`

### Step 5 — PartitionBy

Partitions the collection by ['order_status'].

- Keys: `order_status`
- Partition name: `g`
- Child name: `orders`

> The partition key(s) ['order_status'] identify each group and are accessible at the group level. Row-level data is accessible via the child collection 'orders'; aggregating over it (e.g. COUNT('orders')) operates on the rows within that group.

### Step 6 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `order_status` → reference
  - `n` → COUNT(`orders`) _(implicitly scoped via relationship)_

> Note: 'n' aggregates 'orders' — scoping is implicit via 'orders' relationship navigation, not an explicit filter.

### Step 7 — TopK

Sorts the collection and keeps the top 1 records.

- Limit: 1
- Order by: `n` DESC NULLS LAST

### Step 8 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `order_status` → reference

## Schema

- **Source collection:** `customers`
- **Output columns:** `order_status` (string)
- **Ordering:** `n` DESC NULLS LAST
- **Limit:** 1
