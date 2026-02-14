WITH "_S2" AS (
  SELECT
    l_partkey AS L_PARTKEY,
    l_suppkey AS L_SUPPKEY,
    COUNT(*) AS N_ROWS,
    SUM(l_quantity) AS SUM_L_QUANTITY
  FROM TPCH.LINEITEM
  WHERE
    EXTRACT(YEAR FROM CAST(l_shipdate AS DATETIME)) = 1994 AND l_tax = 0
  GROUP BY
    l_partkey,
    l_suppkey
), "_T" AS (
  SELECT
    "_S2".L_SUPPKEY,
    "_S2".N_ROWS,
    PART.p_name AS P_NAME,
    "_S2".SUM_L_QUANTITY,
    ROW_NUMBER() OVER (PARTITION BY "_S2".L_SUPPKEY ORDER BY NVL("_S2".SUM_L_QUANTITY, 0) DESC) AS "_W"
  FROM "_S2" "_S2"
  JOIN TPCH.PART PART
    ON PART.p_partkey = "_S2".L_PARTKEY
)
SELECT
  SUPPLIER.s_name AS supplier_name,
  "_T".P_NAME AS part_name,
  NVL("_T".SUM_L_QUANTITY, 0) AS total_quantity,
  "_T".N_ROWS AS n_shipments
FROM TPCH.SUPPLIER SUPPLIER
JOIN TPCH.NATION NATION
  ON NATION.n_name = 'FRANCE' AND NATION.n_nationkey = SUPPLIER.s_nationkey
JOIN "_T" "_T"
  ON SUPPLIER.s_suppkey = "_T".L_SUPPKEY AND "_T"."_W" = 1
ORDER BY
  3 DESC NULLS LAST,
  1 NULLS FIRST
FETCH FIRST 3 ROWS ONLY
