WITH _u_0 AS (
  SELECT
    r_regionkey AS _u_1
  FROM tpch.REGION
  WHERE
    r_name = 'AFRICA'
  GROUP BY
    1
), _s7 AS (
  SELECT
    LINEITEM.l_quantity,
    LINEITEM.l_suppkey
  FROM tpch.LINEITEM AS LINEITEM
  JOIN tpch.PART AS PART
    ON LINEITEM.l_partkey = PART.p_partkey
    AND PART.p_container LIKE 'LG%'
    AND PART.p_name LIKE '%tomato%'
  WHERE
    EXTRACT(YEAR FROM CAST(LINEITEM.l_shipdate AS DATETIME)) = 1995
    AND LINEITEM.l_shipmode = 'SHIP'
), _t0 AS (
  SELECT
    ANY_VALUE(NATION.n_name) AS anything_n_name,
    ANY_VALUE(SUPPLIER.s_name) AS anything_s_name,
    ANY_VALUE(SUPPLIER.s_nationkey) AS anything_s_nationkey,
    SUM(_s7.l_quantity) AS sum_l_quantity
  FROM tpch.NATION AS NATION
  LEFT JOIN _u_0 AS _u_0
    ON NATION.n_regionkey = _u_0._u_1
  JOIN tpch.SUPPLIER AS SUPPLIER
    ON NATION.n_nationkey = SUPPLIER.s_nationkey
    AND SUPPLIER.s_acctbal >= 0.0
    AND SUPPLIER.s_comment LIKE '%careful%'
  LEFT JOIN _s7 AS _s7
    ON SUPPLIER.s_suppkey = _s7.l_suppkey
  WHERE
    NOT _u_0._u_1 IS NULL
  GROUP BY
    SUPPLIER.s_suppkey
)
SELECT
  anything_s_name AS supplier_name,
  anything_n_name AS nation_name,
  COALESCE(sum_l_quantity, 0) AS supplier_quantity,
  (
    100.0 * COALESCE(sum_l_quantity, 0)
  ) / SUM(COALESCE(sum_l_quantity, 0)) OVER (PARTITION BY anything_s_nationkey) AS national_qty_pct
FROM _t0
ORDER BY
  4 DESC
LIMIT 5
