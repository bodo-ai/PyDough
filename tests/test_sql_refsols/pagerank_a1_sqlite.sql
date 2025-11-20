WITH _t4 AS (
  SELECT
    s_key
  FROM main.sites
), _s0 AS (
  SELECT
    s_key,
    COUNT(*) OVER () AS n,
    CAST(1.0 AS REAL) / COUNT(*) OVER () AS page_rank
  FROM _t4
), _s1 AS (
  SELECT
    l_source,
    l_target
  FROM main.links
), _s2 AS (
  SELECT
    _s0.s_key,
    MAX(_s0.n) AS anything_n,
    MAX(_s0.page_rank) AS anything_pagerank,
    SUM(IIF(_s1.l_target IS NULL, _s0.n, CAST(_s1.l_source <> _s1.l_target AS INTEGER))) AS sum_ntarget
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.s_key = _s1.l_source
  GROUP BY
    1
), _t1 AS (
  SELECT
    _s3.l_source,
    _s3.l_target,
    _s5.s_key,
    (
      CAST(0.15 AS REAL) / _s2.anything_n
    ) + 0.85 * SUM(
      CAST((
        CAST(_s3.l_source <> _s3.l_target OR _s3.l_target IS NULL AS INTEGER) * _s2.anything_pagerank
      ) AS REAL) / COALESCE(_s2.sum_ntarget, 0)
    ) OVER (PARTITION BY _s5.s_key) AS page_rank
  FROM _s2 AS _s2
  JOIN _s1 AS _s3
    ON _s2.s_key = _s3.l_source
  JOIN _t4 AS _s5
    ON _s3.l_target = _s5.s_key OR _s3.l_target IS NULL
)
SELECT
  s_key AS key,
  ROUND(page_rank, 5) AS page_rank
FROM _t1
WHERE
  NOT l_target IS NULL AND l_source = l_target
ORDER BY
  1
