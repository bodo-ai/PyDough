WITH _t3 AS (
  SELECT
    p_brand,
    p_container,
    p_partkey
  FROM tpch.PART
  WHERE
    p_brand = 'Brand#13'
), _t AS (
  SELECT
    LINEITEM.l_shipdate,
    _t7.p_partkey,
    ROW_NUMBER() OVER (PARTITION BY _t7.p_container ORDER BY CASE WHEN LINEITEM.l_extendedprice IS NULL THEN 1 ELSE 0 END DESC, LINEITEM.l_extendedprice DESC, CASE WHEN LINEITEM.l_shipdate IS NULL THEN 1 ELSE 0 END, LINEITEM.l_shipdate) AS _w
  FROM _t3 AS _t7
  JOIN tpch.LINEITEM AS LINEITEM
    ON LINEITEM.l_partkey = _t7.p_partkey
    AND LINEITEM.l_shipmode = 'RAIL'
    AND LINEITEM.l_tax = 0
), _s3 AS (
  SELECT
    p_partkey,
    ANY_VALUE(l_shipdate) AS anything_l_shipdate,
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
  p_container COLLATE utf8mb4_bin AS container,
  max_anything_l_shipdate AS highest_price_ship_date
FROM _t1
WHERE
  sum_n_rows <> 0
ORDER BY
  2,
  1
LIMIT 5
