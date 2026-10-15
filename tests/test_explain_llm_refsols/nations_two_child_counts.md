## Key Facts

- **Source collection:** `nations`
- **Limit:** none
- **Data filters:** none

## Query Summary

Accesses 'nations', selecting name and computing counting customers records as 'n_customers', counting suppliers records as 'n_suppliers'.

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
  - `n_customers` → COUNT(`customers`) _(implicitly scoped via relationship)_
  - `n_suppliers` → COUNT(`suppliers`) _(implicitly scoped via relationship)_

> Note: 'n_customers' aggregates 'customers' — scoping is implicit via 'customers' relationship navigation, not an explicit filter.
> Note: 'n_suppliers' aggregates 'suppliers' — scoping is implicit via 'suppliers' relationship navigation, not an explicit filter.

## Schema

- **Source collection:** `nations`
- **Output columns:** `name` (string), `n_customers` (numeric), `n_suppliers` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
