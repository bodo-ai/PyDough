## Key Facts

- **Source collection:** `scores`
- **Limit:** 3
- **Data filters:** none

## Query Summary

Accesses user-generated collection 'scores', keeping the top 3 rows by score desc.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — UserGeneratedCollection

Accesses user-generated DataFrame collection 'scores' (5 row(s), columns: row_id, score).

- Name: `scores`
- Rows: 5
- Columns: `row_id`, `score`

### Step 3 — TopK

Sorts the collection and keeps the top 3 records.

- Limit: 3
- Order by: `score` DESC NULLS LAST

## Schema

- **Source collection:** `scores`
- **Output columns:** _(none)_
- **Ordering:** `score` DESC NULLS LAST
- **Limit:** 3
