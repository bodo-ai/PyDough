WITH _s7 AS (
  SELECT
    LINEITEM.l_quantity,
    LINEITEM.l_suppkey
  FROM tpch.LINEITEM AS LINEITEM
  JOIN tpch.PART AS PART
    ON LINEITEM.l_partkey = PART.p_partkey
    AND PART.p_container LIKE 'LG%'
    AND PART.p_name LIKE '%tomato%'
  WHERE
    EXTRACT(MONTH FROM CAST(LINEITEM.l_shipdate AS DATETIME)) < 7
    AND EXTRACT(YEAR FROM CAST(LINEITEM.l_shipdate AS DATETIME)) = 1995
    AND LINEITEM.l_shipmode = 'SHIP'
), _t0 AS (
  SELECT
    ANY_VALUE(NATION.n_name) AS anything_n_name,
    ANY_VALUE(SUPPLIER.s_name) AS anything_s_name,
    ANY_VALUE(SUPPLIER.s_nationkey) AS anything_s_nationkey,
    SUM(_s7.l_quantity) AS sum_l_quantity
  FROM tpch.NATION AS NATION
  JOIN tpch.REGION AS REGION
    ON NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = 'AFRICA'
  JOIN tpch.SUPPLIER AS SUPPLIER
    ON NATION.n_nationkey = SUPPLIER.s_nationkey
    AND SUPPLIER.s_acctbal >= 8000.0
    AND SUPPLIER.s_comment LIKE '%careful%'
  LEFT JOIN _s7 AS _s7
    ON SUPPLIER.s_suppkey = _s7.l_suppkey
  GROUP BY
    SUPPLIER.s_suppkey
)
SELECT
  anything_s_name AS supplier_name,
  anything_n_name AS nation_name,
  COALESCE(sum_l_quantity, 0) AS supplier_quantity,
  (
    100.0 * COALESCE(sum_l_quantity, 0)
  ) / CASE
    WHEN SUM(COALESCE(sum_l_quantity, 0)) OVER (PARTITION BY anything_s_nationkey) > 0
    THEN SUM(COALESCE(sum_l_quantity, 0)) OVER (PARTITION BY anything_s_nationkey)
    ELSE NULL
  END AS national_qty_pct
FROM _t0
ORDER BY
  4 DESC
LIMIT 5
