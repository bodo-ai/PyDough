## Key Facts

- **Source collection:** `nums`
- **Limit:** 3
- **Data filters:** none

## Query Summary

Accesses user-generated collection 'nums', keeping the top 3 rows by n asc.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — UserGeneratedCollection

Accesses user-generated range collection 'nums' (range(1, 11), column 'n').

- Name: `nums`
- Range: `range(1, 11)` in column `n`

### Step 3 — TopK

Sorts the collection and keeps the top 3 records.

- Limit: 3
- Order by: `n` ASC NULLS FIRST

## Schema

- **Source collection:** `nums`
- **Output columns:** _(none)_
- **Ordering:** `n` ASC NULLS FIRST
- **Limit:** 3
