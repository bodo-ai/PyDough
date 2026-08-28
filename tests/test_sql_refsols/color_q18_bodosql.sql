WITH _t0 AS (
  SELECT
    _s0.value AS word
  FROM clrs AS clrs
  CROSS JOIN LATERAL SPLIT_TO_TABLE(clrs.identname, '_') AS _s0
  GROUP BY
    1
  QUALIFY
    RANK() OVER (ORDER BY COUNT(*) DESC) = 1
)
SELECT
  word
FROM _t0
