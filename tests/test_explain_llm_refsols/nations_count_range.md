## Key Facts

- **Source collection:** `nations`
- **Limit:** none
- **Data filters:** none

## Query Summary

Accesses 'nations', selecting name and computing counting r records as 'n'.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'nations' collection.

- Collection: `nations`

### Step 3 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `name` → reference
  - `n` → COUNT(`r`)

## Schema

- **Source collection:** `nations`
- **Output columns:** `name` (string), `n` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
