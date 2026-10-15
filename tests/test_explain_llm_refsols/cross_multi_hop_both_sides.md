## Key Facts

- **Source collection:** `suppliers`
- **Output collection:** `supply_records`
- **Limit:** none
- **Data filters:** none

## Query Summary

Pairs every 'orders' row with every 'suppliers' row, then navigates to 'orders' then 'supply_records'.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'customers' collection.

- Collection: `customers`

### Step 3 — SubCollection

Traverses the 'orders' relationship from 'customers' to 'orders'.

- `customers` → `orders` via `orders`

### Step 4 — Cross

CROSS join: every row of 'orders' paired with every row of 'suppliers'.

- Left: `orders`
- Right: `suppliers`

> Each row now represents a unique combination of 'orders' × 'suppliers'. After CROSS, only 'suppliers' terms are directly accessible as expressions; 'orders' terms were available before the CROSS.

### Step 5 — SubCollection

Traverses the 'supply_records' relationship from 'suppliers' to 'supply_records'.

- `suppliers` → `supply_records` via `supply_records`

## Schema

- **Source collection:** `suppliers`
- **Output columns:** _(none)_
- **Ordering:** _(none)_
- **Limit:** _(none)_
