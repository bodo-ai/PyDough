WITH _0 AS (
  SELECT DISTINCT
    UNNEST(STRING_TO_ARRAY(p_name, ' ')) AS color
  FROM tpch.part
)
SELECT
  _s0.val AS letter,
  COUNT(*) AS n_occur,
  COUNT(DISTINCT _0.color) AS n_colors
FROM _0 AS _0
CROSS JOIN LATERAL UNNEST(REGEXP_SPLIT_TO_ARRAY(_0.color, '')) WITH ORDINALITY AS _s0(val, idx)
GROUP BY
  1
