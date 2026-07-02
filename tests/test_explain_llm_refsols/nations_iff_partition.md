## Key Facts

- **Source collection:** `nations`
- **Limit:** 1
- **Data filters:** key > 5

## Query Summary

Accesses 'nations', filtered to rows where key > 5, partitioned by tier, selecting tier and computing counting nations records as 'n', keeping the top 1 rows by n desc.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — TableCollection

Accesses the 'nations' collection.

- Collection: `nations`

### Step 3 — Where

Filters rows to those matching the given conditions.

- Condition: `key > 5`

### Step 4 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `tier` → IFF(key > 15, 'high', 'low')

### Step 5 — PartitionBy

Partitions the collection by ['tier'].

- Keys: `tier`
- Partition name: `g`
- Child name: `nations`

> The partition key(s) ['tier'] identify each group and are accessible at the group level. Row-level data is accessible via the child collection 'nations'; aggregating over it (e.g. COUNT('nations')) operates on the rows within that group.

### Step 6 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `tier` → reference
  - `n` → COUNT(`nations`) where key > 5

### Step 7 — TopK

Sorts the collection and keeps the top 1 records.

- Limit: 1
- Order by: `n` DESC NULLS LAST

### Step 8 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `tier` → reference

## Schema

- **Source collection:** `nations`
- **Output columns:** `tier` (string)
- **Ordering:** `n` DESC NULLS LAST
- **Limit:** 1
