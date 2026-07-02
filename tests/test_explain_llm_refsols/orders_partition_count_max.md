## Key Facts

- **Source collection:** `customers`
- **Output collection:** `orders`
- **Limit:** none
- **Data filters:** none

## Query Summary

Accesses 'customers', partitioned by order_status, selecting order_status, max_price (MAX(orders.total_price)) and computing counting orders records per group as 'n'.

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
  - `max_price` → MAX(orders.total_price)

> Note: 'n' aggregates 'orders' — scoping is implicit via 'orders' relationship navigation, not an explicit filter.

## Schema

- **Source collection:** `customers`
- **Output columns:** `order_status` (string), `n` (numeric), `max_price` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
