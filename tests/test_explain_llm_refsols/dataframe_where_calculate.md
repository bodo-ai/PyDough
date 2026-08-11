## Key Facts

- **Source collection:** `scores`
- **Limit:** none
- **Data filters:** score > 3

## Query Summary

Accesses user-generated collection 'scores', filtered to rows where score > 3, selecting row_id, score.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — UserGeneratedCollection

Accesses user-generated DataFrame collection 'scores' (5 row(s), columns: row_id, score).

- Name: `scores`
- Rows: 5
- Columns: `row_id`, `score`

### Step 3 — Where

Filters rows to those matching the given conditions.

- Condition: `score > 3`

### Step 4 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `row_id` → reference
  - `score` → reference

## Schema

- **Source collection:** `scores`
- **Output columns:** `row_id` (numeric), `score` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
