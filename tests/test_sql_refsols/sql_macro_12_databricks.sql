WITH _0 AS (
  SELECT DISTINCT
    EXPLODE(SPLIT(p_name, ' ')) AS color
  FROM part
)
SELECT
  _s0.val AS letter,
  COUNT(*) AS n_occur,
  COUNT(DISTINCT _0.color) AS n_colors
FROM _0 AS _0, LATERAL POSEXPLODE(SPLIT(_0.color, '\\Q\\E')) AS _s0(idx, val)
WHERE
  _s0.val <> ''
GROUP BY
  1
