WITH _s1 AS (
  SELECT DISTINCT
    EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) AS year_o_orderdate
  FROM tpch_s3_pq.tpch_sf1.orders
), _s5 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(orders.o_orderdate AS TIMESTAMP)) AS year_o_orderdate,
    SUM(lineitem.l_extendedprice) AS sum_l_extendedprice
  FROM tpch_s3_pq.tpch_sf1.orders AS orders
  JOIN tpch_s3_pq.tpch_sf1.lineitem AS lineitem
    ON lineitem.l_orderkey = orders.o_orderkey
  GROUP BY
    1
)
SELECT
  _s1.year_o_orderdate AS year,
  COALESCE(_s5.sum_l_extendedprice, 0) AS bug
FROM tpch_s3_pq.tpch_sf1.customer AS customer
CROSS JOIN _s1 AS _s1
LEFT JOIN _s5 AS _s5
  ON _s1.year_o_orderdate = _s5.year_o_orderdate
ORDER BY
  2 DESC
LIMIT 5
