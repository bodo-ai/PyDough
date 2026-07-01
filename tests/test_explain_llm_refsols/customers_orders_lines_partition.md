## Key Facts

- **Source collection:** `customers`
- **Output collection:** `lines`
- **Limit:** none
- **Data filters:** market_segment == 'BUILDING'

## Query Summary

Accesses 'customers', filtered to rows where market_segment == 'BUILDING', partitioned by return_flag, selecting return_flag.

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

### Step 5 — SubCollection

Traverses the 'lines' relationship from 'orders' to 'lines'.

- `orders` → `lines` via `lines`

### Step 6 — PartitionBy

Partitions the collection by ['return_flag'].

- Keys: `return_flag`
- Partition name: `g`
- Child name: `lines`

> The partition key(s) ['return_flag'] identify each group and are accessible at the group level. Row-level data is accessible via the child collection 'lines'; aggregating over it (e.g. COUNT('lines')) operates on the rows within that group.

### Step 7 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `return_flag` → reference

## Schema

- **Source collection:** `customers`
- **Output columns:** `return_flag` (string)
- **Ordering:** _(none)_
- **Limit:** _(none)_
