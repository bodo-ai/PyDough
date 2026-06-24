## Key Facts

- **Source collection:** `customers`
- **Limit:** none
- **Data filters:** none

## Query Summary

Accesses 'customers', selecting nation_name (nation.SINGULAR().name).

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'customers' collection.

- Collection: `customers`

### Step 3 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `nation_name` → nation.SINGULAR().name

> Note: 'nation_name' accesses 'nation.SINGULAR().name' via implicit singular relationship navigation — scoping to the parent row is enforced by the relationship, not by an explicit filter.

## Schema

- **Source collection:** `customers`
- **Output columns:** `nation_name` (string)
- **Ordering:** _(none)_
- **Limit:** _(none)_
