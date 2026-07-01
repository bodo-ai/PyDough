## Key Facts

- **Source collection:** `nations`
- **Limit:** none
- **Data filters:** key > 5

## Query Summary

Accesses 'nations', filtered to rows where key > 5, partitioned by name, selecting name.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'nations' collection.

- Collection: `nations`

### Step 3 — Where

Filters rows to those matching the given conditions.

- Condition: `key > 5`

### Step 4 — PartitionBy

Partitions the collection by ['name'].

- Keys: `name`
- Partition name: `g`
- Child name: `nations`

> The partition key(s) ['name'] identify each group and are accessible at the group level. Row-level data is accessible via the child collection 'nations'; aggregating over it (e.g. COUNT('nations')) operates on the rows within that group.

### Step 5 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `name` → reference

## Schema

- **Source collection:** `nations`
- **Output columns:** `name` (string)
- **Ordering:** _(none)_
- **Limit:** _(none)_
