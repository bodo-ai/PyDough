## Key Facts

- **Source collection:** `scores`
- **Limit:** none
- **Data filters:** none

## Query Summary

Accesses user-generated collection 'scores', selecting row_id, score.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — UserGeneratedCollection

Accesses user-generated DataFrame collection 'scores' (5 row(s), columns: row_id, score).

- Name: `scores`
- Rows: 5
- Columns: `row_id`, `score`

### Step 3 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `row_id` → reference
  - `score` → reference

## Schema

- **Source collection:** `scores`
- **Output columns:** `row_id` (numeric), `score` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
