## Key Facts

- **Source collection:** `customers`
- **Output collection:** `orders`
- **Limit:** none
- **Data filters:** market_segment == 'BUILDING'

## Query Summary

Accesses 'customers', filtered to rows where market_segment == 'BUILDING', selecting key.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'customers' collection.

- Collection: `customers`

### Step 3 — Where

Filters rows to those matching the given conditions.

- Condition: `market_segment == 'BUILDING'`

### Step 4 — SubCollection

Traverses the 'orders' relationship from 'customers' to 'orders'.

- `customers` → `orders` via `orders`

### Step 5 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `key` → reference

## Schema

- **Source collection:** `customers`
- **Output columns:** `key` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
