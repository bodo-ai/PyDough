## Key Facts

- **Source collection:** `customers`
- **Limit:** none
- **Data filters:** none

## Query Summary

Graph-level aggregation over collection(s): 'customers', computing counting customers records as 'n'.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `n` → COUNT(`customers`) where nation.region.name == 'EUROPE' AND HAS(orders.WHERE(YEAR(order_date) == 1994))

## Schema

- **Source collection:** `customers`
- **Output columns:** `n` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
