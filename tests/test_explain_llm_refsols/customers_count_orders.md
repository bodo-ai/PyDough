## Key Facts

- **Source collection:** `customers`
- **Limit:** none
- **Data filters:** none

## Query Summary

Accesses 'customers', selecting key and computing counting orders records as 'n_orders'.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'customers' collection.

- Collection: `customers`

### Step 3 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `key` → reference
  - `n_orders` → COUNT(`orders`) _(implicitly scoped via relationship)_

> Note: 'n_orders' aggregates 'orders' — scoping is implicit via 'orders' relationship navigation, not an explicit filter.

## Schema

- **Source collection:** `customers`
- **Output columns:** `key` (numeric), `n_orders` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
