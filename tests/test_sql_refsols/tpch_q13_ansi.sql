WITH _s3 AS (
  SELECT
    COUNT(*) AS agg_0,
    o_custkey AS customer_key
  FROM tpch.orders
  WHERE
    NOT o_comment LIKE '%special%requests%'
  GROUP BY
    o_custkey
), _t0 AS (
  SELECT
    COUNT(*) AS custdist,
    COALESCE(_s3.agg_0, 0) AS c_count
  FROM tpch.customer AS _s0
  LEFT JOIN _s3 AS _s3
    ON _s0.c_custkey = _s3.customer_key
  GROUP BY
    COALESCE(_s3.agg_0, 0)
)
SELECT
  c_count AS C_COUNT,
  custdist AS CUSTDIST
FROM _t0
ORDER BY
  custdist DESC,
  c_count DESC
LIMIT 10
