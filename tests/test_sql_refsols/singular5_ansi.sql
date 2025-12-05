WITH _t5 AS (
  SELECT
    lineitem.l_shipdate,
    part.p_partkey
  FROM tpch.part AS part
  JOIN tpch.lineitem AS lineitem
    ON lineitem.l_partkey = part.p_partkey
    AND lineitem.l_shipmode = 'RAIL'
    AND lineitem.l_tax = 0
  WHERE
    part.p_brand = 'Brand#13'
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY p_container ORDER BY lineitem.l_extendedprice DESC NULLS FIRST, lineitem.l_shipdate NULLS LAST) = 1
), _s3 AS (
  SELECT
    p_partkey,
    ANY_VALUE(l_shipdate) AS anything_l_shipdate,
    COUNT(*) AS n_rows
  FROM _t5
  GROUP BY
    1
), _t1 AS (
  SELECT
    part.p_container,
    MAX(_s3.anything_l_shipdate) AS max_anything_l_shipdate,
    SUM(_s3.n_rows) AS sum_n_rows
  FROM tpch.part AS part
  LEFT JOIN _s3 AS _s3
    ON _s3.p_partkey = part.p_partkey
  WHERE
    part.p_brand = 'Brand#13'
  GROUP BY
    1
)
SELECT
  p_container AS container,
  max_anything_l_shipdate AS highest_price_ship_date
FROM _t1
WHERE
  sum_n_rows <> 0
ORDER BY
  2,
  1
LIMIT 5
