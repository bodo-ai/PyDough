## Key Facts

- **Source collection:** `nations`
- **Limit:** none
- **Data filters:** none

## Query Summary

Accesses 'nations', selecting key, name.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'nations' collection.

- Collection: `nations`

### Step 3 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `key` → reference
  - `name` → reference

## Schema

- **Source collection:** `nations`
- **Output columns:** `key` (numeric), `name` (string)
- **Ordering:** _(none)_
- **Limit:** _(none)_
