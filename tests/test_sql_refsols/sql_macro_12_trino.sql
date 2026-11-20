WITH _0 AS (
  SELECT DISTINCT
    l.value AS color
  FROM mysql.tpch.part AS p
  CROSS JOIN UNNEST(SPLIT(p.p_name, ' ')) AS l(value)
)
SELECT
  _s0.val AS letter,
  COUNT(*) AS n_occur,
  COUNT(DISTINCT _0.color) AS n_colors
FROM _0 AS _0
CROSS JOIN UNNEST(REGEXP_EXTRACT_ALL(_0.color, '.')) WITH ORDINALITY AS _s0(val, idx)
GROUP BY
  1
