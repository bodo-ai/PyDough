## Key Facts

- **Source collection:** `nations`
- **Limit:** none
- **Data filters:** none

## Query Summary

Accesses 'nations', computing counting customers records as 'n'.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'nations' collection.

- Collection: `nations`

### Step 3 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `n` → COUNT(`customers`) _(implicitly scoped via relationship)_

> Note: 'n' aggregates 'customers' — scoping is implicit via 'customers' relationship navigation, not an explicit filter.

## Schema

- **Source collection:** `nations`
- **Output columns:** `n` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
