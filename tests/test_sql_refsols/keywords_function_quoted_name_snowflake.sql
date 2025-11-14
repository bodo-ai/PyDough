WITH _s0 AS (
  SELECT
    MAX("INTEGER") AS max_integer
  FROM keywords."PARTITION"
)
SELECT
  calculate.".WHERE" AS key,
  calculate."LENGTH" AS len
FROM _s0 AS _s0
JOIN keywords.calculate AS calculate
  ON _s0.max_integer = calculate.".WHERE"
