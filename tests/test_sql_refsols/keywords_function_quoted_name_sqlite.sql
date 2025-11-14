WITH _s0 AS (
  SELECT
    MAX("integer") AS max_integer
  FROM keywords."partition"
)
SELECT
  calculate.".where" AS key,
  calculate."length" AS len
FROM _s0 AS _s0
JOIN keywords.calculate AS calculate
  ON _s0.max_integer = calculate.".where"
