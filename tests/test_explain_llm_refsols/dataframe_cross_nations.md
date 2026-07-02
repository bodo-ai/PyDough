## Key Facts

- **Source collection:** `nations`
- **Limit:** none
- **Data filters:** none

## Query Summary

Pairs every 'scores' row with every 'nations' row, selecting name.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — Cross

CROSS join: every row of 'scores' paired with every row of 'nations'.

- Left: `scores`
- Right: `nations`

> Each row now represents a unique combination of 'scores' × 'nations'. After CROSS, only 'nations' terms are directly accessible as expressions; 'scores' terms were available before the CROSS.

### Step 3 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `name` → reference

## Schema

- **Source collection:** `nations`
- **Output columns:** `name` (string)
- **Ordering:** _(none)_
- **Limit:** _(none)_
