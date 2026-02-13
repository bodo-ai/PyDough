WITH _t3 AS (
  SELECT
    p_brand,
    p_container,
    p_partkey
  FROM tpch.part
  WHERE
    p_brand = 'Brand#13'
), _t AS (
  SELECT
    lineitem.l_shipdate,
    _t7.p_partkey,
    ROW_NUMBER() OVER (PARTITION BY _t7.p_container ORDER BY lineitem.l_extendedprice DESC, lineitem.l_shipdate) AS _w
  FROM _t3 AS _t7
  JOIN tpch.lineitem AS lineitem
    ON _t7.p_partkey = lineitem.l_partkey
    AND lineitem.l_shipmode = 'RAIL'
    AND lineitem.l_tax = 0
), _s3 AS (
  SELECT
    p_partkey,
    MAX(l_shipdate) AS anything_l_shipdate,
    COUNT(*) AS n_rows
  FROM _t
  WHERE
    _w = 1
  GROUP BY
    1
), _t1 AS (
  SELECT
    _t3.p_container,
    MAX(_s3.anything_l_shipdate) AS max_anything_l_shipdate,
    SUM(_s3.n_rows) AS sum_n_rows
  FROM _t3 AS _t3
  LEFT JOIN _s3 AS _s3
    ON _s3.p_partkey = _t3.p_partkey
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
  2 NULLS FIRST,
  1 NULLS FIRST
LIMIT 5
