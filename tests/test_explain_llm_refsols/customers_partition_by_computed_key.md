## Key Facts

- **Source collection:** `customers`
- **Limit:** none
- **Data filters:** none

## Query Summary

Accesses 'customers', partitioned by n_orders, computing counting orders records as 'n_orders', counting customers records as 'n_customers'.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'customers' collection.

- Collection: `customers`

### Step 3 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `n_orders` → COUNT(`orders`) _(implicitly scoped via relationship)_

> Note: 'n_orders' aggregates 'orders' — scoping is implicit via 'orders' relationship navigation, not an explicit filter.

### Step 4 — PartitionBy

Partitions the collection by ['n_orders'].

- Keys: `n_orders`
- Partition name: `g`
- Child name: `customers`

> The partition key(s) ['n_orders'] identify each group and are accessible at the group level. Row-level data is accessible via the child collection 'customers'; aggregating over it (e.g. COUNT('customers')) operates on the rows within that group.

### Step 5 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `n_orders` → reference
  - `n_customers` → COUNT(`customers`)

## Schema

- **Source collection:** `customers`
- **Output columns:** `n_orders` (numeric), `n_customers` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
