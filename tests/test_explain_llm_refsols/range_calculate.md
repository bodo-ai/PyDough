## Key Facts

- **Source collection:** `nums`
- **Limit:** none
- **Data filters:** none

## Query Summary

Accesses user-generated collection 'nums', selecting n.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — UserGeneratedCollection

Accesses user-generated range collection 'nums' (range(1, 6), column 'n').

- Name: `nums`
- Range: `range(1, 6)` in column `n`

### Step 3 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `n` → reference

## Schema

- **Source collection:** `nums`
- **Output columns:** `n` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
