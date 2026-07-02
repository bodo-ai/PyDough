## Key Facts

- **Source collection:** `nums`
- **Limit:** none
- **Data filters:** n > 5

## Query Summary

Accesses user-generated collection 'nums', filtered to rows where n > 5, selecting n.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — UserGeneratedCollection

Accesses user-generated range collection 'nums' (range(0, 10), column 'n').

- Name: `nums`
- Range: `range(0, 10)` in column `n`

### Step 3 — Where

Filters rows to those matching the given conditions.

- Condition: `n > 5`

### Step 4 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `n` → reference

## Schema

- **Source collection:** `nums`
- **Output columns:** `n` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
