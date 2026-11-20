WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM part AS p
  CROSS JOIN LATERAL SPLIT_TO_TABLE(p.p_name, ' ') AS l
)
SELECT
  _s0.value AS letter,
  COUNT(*) AS n_occur,
  COUNT(DISTINCT _0.color) AS n_colors
FROM _0 AS _0
CROSS JOIN LATERAL FLATTEN(REGEXP_SUBSTR_ALL(_0.color, '.{1}')) AS _s0(seq, key, path, index, value, this)
GROUP BY
  1
