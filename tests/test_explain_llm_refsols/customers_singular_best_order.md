## Key Facts

- **Source collection:** `customers`
- **Limit:** none
- **Data filters:** none

## Query Summary

Accesses 'customers', selecting best_order_price (computed).

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'customers' collection.

- Collection: `customers`

### Step 3 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `best_order_price` → orders.WHERE(RANKING(by=(total_price.DESC(na_pos='last'))) == 1).SINGULAR().total_price

> Note: 'best_order_price' accesses 'orders.WHERE(RANKING(by=(total_price.DESC(na_pos='last'))) == 1).SINGULAR().total_price' via implicit singular relationship navigation — scoping to the parent row is enforced by the relationship, not by an explicit filter.

## Schema

- **Source collection:** `customers`
- **Output columns:** `best_order_price` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
