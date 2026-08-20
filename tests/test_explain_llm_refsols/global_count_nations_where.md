## Key Facts

- **Source collection:** `nations`
- **Limit:** none
- **Data filters:** none

## Query Summary

Graph-level aggregation over collection(s): 'nations', computing counting nations records as 'n'.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `n` → COUNT(`nations`) where key > 5

## Schema

- **Source collection:** `nations`
- **Output columns:** `n` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
