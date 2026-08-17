## Key Facts

- **Source collection:** `suppliers`
- **Output collection:** `supply_records`
- **Limit:** none
- **Data filters:** none

## Query Summary

Pairs every 'g1' row with every 'suppliers' row, then navigates to 'supply_records', partitioned by order_status.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — PartitionBy

Partitions the collection by ['order_status'].

- Keys: `order_status`
- Partition name: `g1`
- Child name: `orders`

> The partition key(s) ['order_status'] identify each group and are accessible at the group level. Row-level data is accessible via the child collection 'orders'; aggregating over it (e.g. COUNT('orders')) operates on the rows within that group.

### Step 3 — Cross

CROSS join: every row of 'g1' paired with every row of 'suppliers'.

- Left: `g1`
- Right: `suppliers`

> Each row now represents a unique combination of 'g1' × 'suppliers'. After CROSS, only 'suppliers' terms are directly accessible as expressions; 'g1' terms were available before the CROSS.

### Step 4 — SubCollection

Traverses the 'supply_records' relationship from 'suppliers' to 'supply_records'.

- `suppliers` → `supply_records` via `supply_records`

### Step 5 — PartitionBy

Partitions the collection by ['supply_cost'].

- Keys: `supply_cost`
- Partition name: `g2`
- Child name: `supply_records`

> The partition key(s) ['supply_cost'] identify each group and are accessible at the group level. Row-level data is accessible via the child collection 'supply_records'; aggregating over it (e.g. COUNT('supply_records')) operates on the rows within that group.

## Schema

- **Source collection:** `suppliers`
- **Output columns:** _(none)_
- **Ordering:** _(none)_
- **Limit:** _(none)_
