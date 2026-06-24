## Key Facts

- **Source collection:** `regions`
- **Limit:** none
- **Data filters:** none

## Query Summary

Pairs every 'nations' row with every 'regions' row.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — Cross

CROSS join: every row of 'nations' paired with every row of 'regions'.

- Left: `nations`
- Right: `regions`

> Each row now represents a unique combination of 'nations' × 'regions' — both their terms are available downstream.

## Schema

- **Source collection:** `regions`
- **Output columns:** _(none)_
- **Ordering:** _(none)_
- **Limit:** _(none)_
