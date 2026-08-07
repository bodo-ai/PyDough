## Key Facts

- **Source collection:** `nations`
- **Limit:** none
- **Data filters:** key > 5

## Query Summary

Accesses 'nations', filtered to rows where key > 5, selecting key, name.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'nations' collection.

- Collection: `nations`

### Step 3 — Where

Filters rows to those matching the given conditions.

- Condition: `key > 5`

### Step 4 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `key` → reference
  - `name` → reference

## Schema

- **Source collection:** `nations`
- **Output columns:** `key` (numeric), `name` (string)
- **Ordering:** _(none)_
- **Limit:** _(none)_
