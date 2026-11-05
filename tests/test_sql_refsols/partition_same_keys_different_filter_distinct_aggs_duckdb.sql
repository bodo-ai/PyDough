WITH _t0 AS (
  SELECT
    AVG(o_totalprice) AS avg_o_totalprice
  FROM tpch.orders
  WHERE
    EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1995
  GROUP BY
    o_orderstatus
), _s0 AS (
  SELECT
    MAX(avg_o_totalprice) AS max_avg_o_totalprice,
    MIN(avg_o_totalprice) AS min_avg_o_totalprice
  FROM _t0
), _t3 AS (
  SELECT
    AVG(o_totalprice) AS avg_o_totalprice
  FROM tpch.orders
  WHERE
    EXTRACT(YEAR FROM CAST(o_orderdate AS TIMESTAMP)) = 1996
  GROUP BY
    o_orderstatus
), _s1 AS (
  SELECT
    MAX(avg_o_totalprice) AS max_avg_o_totalprice,
    MIN(avg_o_totalprice) AS min_avg_o_totalprice
  FROM _t3
)
SELECT
  _s0.min_avg_o_totalprice AS min_1995,
  _s0.max_avg_o_totalprice AS max_1995,
  _s1.min_avg_o_totalprice AS min_1996,
  _s1.max_avg_o_totalprice AS max_1996
FROM _s0 AS _s0
CROSS JOIN _s1 AS _s1
