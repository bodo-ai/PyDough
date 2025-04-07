WITH "_t2" AS (
  SELECT
    SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "agg_0",
    "partsupp"."ps_suppkey" AS "supplier_key"
  FROM "tpch"."partsupp" AS "partsupp"
  GROUP BY
    "partsupp"."ps_suppkey"
), "_t0" AS (
  SELECT
    "supplier"."s_suppkey" AS "key",
    "supplier"."s_nationkey" AS "nation_key"
  FROM "tpch"."supplier" AS "supplier"
), "_t6" AS (
  SELECT
    "nation"."n_name" AS "name",
    "nation"."n_nationkey" AS "key"
  FROM "tpch"."nation" AS "nation"
  WHERE
    "nation"."n_name" = 'GERMANY'
), "_t2_2" AS (
  SELECT
    SUM("_t2"."agg_0") AS "agg_0"
  FROM "_t2" AS "_t2"
  JOIN "_t0" AS "_t0"
    ON "_t0"."key" = "_t2"."supplier_key"
  JOIN "_t6" AS "_t6"
    ON "_t0"."nation_key" = "_t6"."key"
), "_t6_2" AS (
  SELECT
    SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "agg_1",
    "partsupp"."ps_partkey" AS "part_key",
    "partsupp"."ps_suppkey" AS "supplier_key"
  FROM "tpch"."partsupp" AS "partsupp"
  GROUP BY
    "partsupp"."ps_partkey",
    "partsupp"."ps_suppkey"
), "_t9_2" AS (
  SELECT
    SUM("_t6"."agg_1") AS "agg_1",
    "_t6"."part_key" AS "part_key"
  FROM "_t6_2" AS "_t6"
  JOIN "_t0" AS "_t4"
    ON "_t4"."key" = "_t6"."supplier_key"
  JOIN "_t6" AS "_t10"
    ON "_t10"."key" = "_t4"."nation_key"
  GROUP BY
    "_t6"."part_key"
)
SELECT
  "_t9"."part_key" AS "PS_PARTKEY",
  COALESCE("_t9"."agg_1", 0) AS "VALUE"
FROM "_t2_2" AS "_t2"
LEFT JOIN "_t9_2" AS "_t9"
  ON TRUE
WHERE
  (
    COALESCE("_t2"."agg_0", 0) * 0.0001
  ) < COALESCE("_t9"."agg_1", 0)
ORDER BY
  "value" DESC
LIMIT 10
