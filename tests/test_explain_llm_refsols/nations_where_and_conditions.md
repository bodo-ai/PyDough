## Key Facts

- **Source collection:** `nations`
- **Limit:** none
- **Data filters:** key > 5 AND key < 20

## Query Summary

Accesses 'nations', filtered to rows where key > 5 and key < 20.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'nations' collection.

- Collection: `nations`

### Step 3 — Where

Filters rows to those matching the given conditions.

- Conditions:
  - `key > 5`
  - `key < 20`

## Schema

- **Source collection:** `nations`
- **Output columns:** _(none)_
- **Ordering:** _(none)_
- **Limit:** _(none)_
