WITH _0 AS (
  SELECT DISTINCT
    UNNEST(STR_SPLIT(p_name, ' ')) AS color
  FROM part
)
SELECT
  _s0.val AS letter,
  COUNT(*) AS n_occur,
  COUNT(DISTINCT _0.color) AS n_colors
FROM _0 AS _0
CROSS JOIN LATERAL (
  SELECT
    UNNEST(REGEXP_SPLIT_TO_ARRAY(_0.color, '')) AS _col_0,
    GENERATE_SUBSCRIPTS(REGEXP_SPLIT_TO_ARRAY(_0.color, ''), 1) - 1 AS _col_1
) AS _s0(val, idx)
GROUP BY
  1
