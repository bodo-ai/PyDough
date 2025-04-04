WITH "_t5_2" AS (
  SELECT
    SUM("lineitem"."l_quantity") AS "agg_0",
    "lineitem"."l_partkey" AS "part_key"
  FROM "tpch"."lineitem" AS "lineitem"
  WHERE
    "lineitem"."l_shipdate" < CAST('1995-01-01' AS DATE)
    AND "lineitem"."l_shipdate" >= CAST('1994-01-01' AS DATE)
  GROUP BY
    "lineitem"."l_partkey"
), "_t7" AS (
  SELECT
    COUNT() AS "agg_0",
    "partsupp"."ps_suppkey" AS "supplier_key"
  FROM "tpch"."partsupp" AS "partsupp"
  JOIN "tpch"."part" AS "part"
    ON "part"."p_name" LIKE 'forest%' AND "part"."p_partkey" = "partsupp"."ps_partkey"
  LEFT JOIN "_t5_2" AS "_t5"
    ON "_t5"."part_key" = "part"."p_partkey"
  WHERE
    "partsupp"."ps_availqty" > (
      COALESCE("_t5"."agg_0", 0) * 0.5
    )
  GROUP BY
    "partsupp"."ps_suppkey"
)
SELECT
  "supplier"."s_name" AS "S_NAME",
  "supplier"."s_address" AS "S_ADDRESS"
FROM "tpch"."supplier" AS "supplier"
LEFT JOIN "tpch"."nation" AS "nation"
  ON "nation"."n_nationkey" = "supplier"."s_nationkey"
LEFT JOIN "_t7" AS "_t7"
  ON "_t7"."supplier_key" = "supplier"."s_suppkey"
WHERE
  (
    "nation"."n_name" = 'CANADA' AND COALESCE("_t7"."agg_0", 0)
  ) > 0
ORDER BY
  "s_name"
LIMIT 10
