## Key Facts

- **Source collection:** `nations`
- **Limit:** 5
- **Data filters:** none

## Query Summary

Accesses 'nations', keeping the top 5 rows by name asc.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'nations' collection.

- Collection: `nations`

### Step 3 — TopK

Sorts the collection and keeps the top 5 records.

- Limit: 5
- Order by: `name` ASC NULLS FIRST

## Schema

- **Source collection:** `nations`
- **Output columns:** _(none)_
- **Ordering:** `name` ASC NULLS FIRST
- **Limit:** 5
