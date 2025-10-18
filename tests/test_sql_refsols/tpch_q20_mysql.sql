WITH _s5 AS (
  SELECT
    LINEITEM.l_partkey,
    SUM(LINEITEM.l_quantity) AS sum_l_quantity
  FROM tpch.PART AS PART
  JOIN tpch.LINEITEM AS LINEITEM
    ON EXTRACT(YEAR FROM CAST(LINEITEM.l_shipdate AS DATETIME)) = 1994
    AND LINEITEM.l_partkey = PART.p_partkey
  WHERE
    PART.p_name LIKE 'forest%'
  GROUP BY
    1
)
SELECT
  ANY_VALUE(SUPPLIER.s_name) COLLATE utf8mb4_bin AS S_NAME,
  ANY_VALUE(SUPPLIER.s_address) AS S_ADDRESS
FROM tpch.SUPPLIER AS SUPPLIER
JOIN tpch.NATION AS NATION
  ON NATION.n_name = 'CANADA' AND NATION.n_nationkey = SUPPLIER.s_nationkey
JOIN tpch.PARTSUPP AS PARTSUPP
  ON PARTSUPP.ps_suppkey = SUPPLIER.s_suppkey
JOIN _s5 AS _s5
  ON PARTSUPP.ps_availqty > (
    0.5 * COALESCE(_s5.sum_l_quantity, 0)
  )
  AND PARTSUPP.ps_partkey = _s5.l_partkey
GROUP BY
  PARTSUPP.ps_suppkey
ORDER BY
  1
LIMIT 10
