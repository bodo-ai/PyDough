## Key Facts

- **Source collection:** `nums`
- **Limit:** none
- **Data filters:** none

## Query Summary

Accesses user-generated collection 'nums', partitioned by n, selecting n and computing counting nums records per group as 'count'.

## Steps

### Step 1 — GlobalContext

Entry point: the graph-level context.


### Step 2 — UserGeneratedCollection

Accesses user-generated range collection 'nums' (range(0, 3), column 'n').

- Name: `nums`
- Range: `range(0, 3)` in column `n`

### Step 3 — PartitionBy

Partitions the collection by ['n'].

- Keys: `n`
- Partition name: `g`
- Child name: `nums`

> The partition key(s) ['n'] identify each group and are accessible at the group level. Row-level data is accessible via the child collection 'nums'; aggregating over it (e.g. COUNT('nums')) operates on the rows within that group.

### Step 4 — Calculate

Adds computed expressions to the collection.

- Terms:
  - `n` → reference
  - `count` → COUNT(`nums`)

## Schema

- **Source collection:** `nums`
- **Output columns:** `n` (numeric), `count` (numeric)
- **Ordering:** _(none)_
- **Limit:** _(none)_
