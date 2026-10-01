WITH _t1 AS (
  SELECT
    _s0.value AS word,
    COUNT(*) AS n_rows
  FROM clrs AS clrs
  CROSS JOIN LATERAL SPLIT_TO_TABLE(clrs.identname, '_') AS _s0
  GROUP BY
    1
), _t0 AS (
  SELECT
    word
  FROM _t1
  QUALIFY
    RANK() OVER (ORDER BY n_rows DESC) = 1
)
SELECT
  word
FROM _t0
