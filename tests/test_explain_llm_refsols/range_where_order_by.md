## Key Facts

- **Source collection:** `nums`
- **Limit:** none
- **Data filters:** n > 10

## Query Summary

Accesses user-generated collection 'nums', filtered to rows where n > 10, ordered by n desc.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — UserGeneratedCollection

Accesses user-generated range collection 'nums' (range(0, 20), column 'n').

- Name: `nums`
- Range: `range(0, 20)` in column `n`

### Step 3 — Where

Filters rows to those matching the given conditions.

- Condition: `n > 10`

### Step 4 — OrderBy

Sorts the collection.

- Order by: `n` DESC NULLS LAST

## Schema

- **Source collection:** `nums`
- **Output columns:** _(none)_
- **Ordering:** `n` DESC NULLS LAST
- **Limit:** _(none)_
