## Key Facts

- **Source collection:** `scores`
- **Limit:** none
- **Data filters:** none

## Query Summary

Accesses user-generated collection 'scores', ordered by score desc.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — UserGeneratedCollection

Accesses user-generated DataFrame collection 'scores' (5 row(s), columns: row_id, score).

- Name: `scores`
- Rows: 5
- Columns: `row_id`, `score`

### Step 3 — OrderBy

Sorts the collection.

- Order by: `score` DESC NULLS LAST

## Schema

- **Source collection:** `scores`
- **Output columns:** _(none)_
- **Ordering:** `score` DESC NULLS LAST
- **Limit:** _(none)_
