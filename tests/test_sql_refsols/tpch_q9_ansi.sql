WITH "_t2_2" AS (
  SELECT
    SUM(
      "lineitem"."l_extendedprice" * (
        1 - "lineitem"."l_discount"
      ) - "partsupp"."ps_supplycost" * "lineitem"."l_quantity"
    ) AS "agg_0",
    "nation"."n_name" AS "nation_name",
    EXTRACT(YEAR FROM "orders"."o_orderdate") AS "o_year"
  FROM "tpch"."nation" AS "nation"
  JOIN "tpch"."supplier" AS "supplier"
    ON "nation"."n_nationkey" = "supplier"."s_nationkey"
  JOIN "tpch"."partsupp" AS "partsupp"
    ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
  JOIN "tpch"."part" AS "part"
    ON "part"."p_name" LIKE '%green%' AND "part"."p_partkey" = "partsupp"."ps_partkey"
  JOIN "tpch"."lineitem" AS "lineitem"
    ON "lineitem"."l_partkey" = "partsupp"."ps_partkey"
    AND "lineitem"."l_suppkey" = "partsupp"."ps_suppkey"
  LEFT JOIN "tpch"."orders" AS "orders"
    ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
  GROUP BY
    "nation"."n_name",
    EXTRACT(YEAR FROM "orders"."o_orderdate")
), "_t0_2" AS (
  SELECT
    COALESCE("_t2"."agg_0", 0) AS "amount",
    "_t2"."nation_name" AS "nation",
    "_t2"."o_year" AS "o_year",
    "_t2"."nation_name" AS "ordering_1",
    "_t2"."o_year" AS "ordering_2"
  FROM "_t2_2" AS "_t2"
  ORDER BY
    "ordering_1",
    "ordering_2" DESC
  LIMIT 10
)
SELECT
  "_t0"."nation" AS "NATION",
  "_t0"."o_year" AS "O_YEAR",
  "_t0"."amount" AS "AMOUNT"
FROM "_t0_2" AS "_t0"
ORDER BY
  "_t0"."ordering_1",
  "_t0"."ordering_2" DESC
