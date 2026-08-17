## Key Facts

- **Source collection:** `suppliers`
- **Limit:** none
- **Data filters:** market_segment == 'BUILDING' AND account_balance > 0 AND account_balance > 1000

## Query Summary

Accesses 'customers', filtered to rows where market_segment == 'BUILDING', then pairs every 'customers' row with every 'suppliers' row, then subcollection filtered to rows where account_balance > 0 and account_balance > 1000.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'customers' collection.

- Collection: `customers`

### Step 3 — Where

Filters rows to those matching the given conditions.

- Condition: `market_segment == 'BUILDING'`

### Step 4 — Cross

CROSS join: every row of 'customers' paired with every row of 'suppliers'.

- Left: `customers`
- Right: `suppliers`

> Each row now represents a unique combination of 'customers' × 'suppliers'. After CROSS, only 'suppliers' terms are directly accessible as expressions; 'customers' terms were available before the CROSS.

### Step 5 — Where

Filters rows to those matching the given conditions.

- Condition: `account_balance > 0`

> This condition filters 'suppliers' before it is paired by CROSS — it is part of the right-hand argument, not a filter on the joined result.

### Step 6 — Where

Filters rows to those matching the given conditions.

- Condition: `account_balance > 1000`

## Schema

- **Source collection:** `suppliers`
- **Output columns:** _(none)_
- **Ordering:** _(none)_
- **Limit:** _(none)_
