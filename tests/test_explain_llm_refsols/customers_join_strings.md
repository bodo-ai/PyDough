## Key Facts

- **Source collection:** `customers`
- **Limit:** none
- **Data filters:** none

## Query Summary

Accesses 'customers', selecting full (JOIN_STRINGS(' ', name, phone)).

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'customers' collection.

- Collection: `customers`

### Step 3 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `full` → JOIN_STRINGS(' ', name, phone)

## Schema

- **Source collection:** `customers`
- **Output columns:** `full` (string)
- **Ordering:** _(none)_
- **Limit:** _(none)_
