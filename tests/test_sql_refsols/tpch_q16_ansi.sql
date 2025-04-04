WITH "_t0_2" AS (
  SELECT
    COUNT(DISTINCT "partsupp"."ps_suppkey") AS "supplier_count",
    "part"."p_brand" AS "p_brand",
    "part"."p_size" AS "p_size",
    "part"."p_type" AS "p_type"
  FROM "tpch"."part" AS "part"
  JOIN "tpch"."partsupp" AS "partsupp"
    ON "part"."p_partkey" = "partsupp"."ps_partkey"
  LEFT JOIN "tpch"."supplier" AS "supplier"
    ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
  WHERE
    "part"."p_brand" <> 'BRAND#45'
    AND "part"."p_size" IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND NOT "part"."p_type" LIKE 'MEDIUM POLISHED%%'
    AND NOT "supplier"."s_comment" LIKE '%Customer%Complaints%'
  GROUP BY
    "part"."p_brand",
    "part"."p_size",
    "part"."p_type"
)
SELECT
  "_t0"."p_brand" AS "P_BRAND",
  "_t0"."p_type" AS "P_TYPE",
  "_t0"."p_size" AS "P_SIZE",
  "_t0"."supplier_count" AS "SUPPLIER_COUNT"
FROM "_t0_2" AS "_t0"
ORDER BY
  "supplier_count" DESC,
  "p_brand",
  "p_type",
  "p_size"
LIMIT 10
