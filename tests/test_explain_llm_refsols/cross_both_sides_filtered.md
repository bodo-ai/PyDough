## Key Facts

- **Source collection:** `suppliers`
- **Limit:** none
- **Data filters:** account_balance > 0 AND account_balance > 1000

## Query Summary

Pairs every 'customers' row with every 'suppliers' row, then subcollection filtered to rows where account_balance > 0 and account_balance > 1000.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — Cross

CROSS join: every row of 'customers' paired with every row of 'suppliers'.

- Left: `customers`
- Right: `suppliers`

> Each row now represents a unique combination of 'customers' × 'suppliers'. After CROSS, only 'suppliers' terms are directly accessible as expressions; 'customers' terms were available before the CROSS.

### Step 3 — Where

Filters rows to those matching the given conditions.

- Condition: `account_balance > 0`

### Step 4 — Where

Filters rows to those matching the given conditions.

- Condition: `account_balance > 1000`

## Schema

- **Source collection:** `suppliers`
- **Output columns:** _(none)_
- **Ordering:** _(none)_
- **Limit:** _(none)_
