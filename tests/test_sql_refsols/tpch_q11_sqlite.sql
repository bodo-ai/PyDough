WITH "_t0" AS (
  SELECT
    "supplier"."s_suppkey" AS "key",
    "supplier"."s_nationkey" AS "nation_key"
  FROM "tpch"."supplier" AS "supplier"
), "_t5" AS (
  SELECT
    "nation"."n_name" AS "name",
    "nation"."n_nationkey" AS "key"
  FROM "tpch"."nation" AS "nation"
  WHERE
    "nation"."n_name" = 'GERMANY'
), "_t2_2" AS (
  SELECT
    SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "agg_0"
  FROM "tpch"."partsupp" AS "partsupp"
  JOIN "_t0" AS "_t0"
    ON "_t0"."key" = "partsupp"."ps_suppkey"
  JOIN "_t5" AS "_t5"
    ON "_t0"."nation_key" = "_t5"."key"
), "_t9" AS (
  SELECT
    SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "agg_1",
    "partsupp"."ps_partkey" AS "part_key"
  FROM "tpch"."partsupp" AS "partsupp"
  JOIN "_t0" AS "_t4"
    ON "_t4"."key" = "partsupp"."ps_suppkey"
  JOIN "_t5" AS "_t8"
    ON "_t4"."nation_key" = "_t8"."key"
  GROUP BY
    "partsupp"."ps_partkey"
)
SELECT
  "_t9"."part_key" AS "PS_PARTKEY",
  COALESCE("_t9"."agg_1", 0) AS "VALUE"
FROM "_t2_2" AS "_t2"
LEFT JOIN "_t9" AS "_t9"
  ON TRUE
WHERE
  (
    COALESCE("_t2"."agg_0", 0) * 0.0001
  ) < COALESCE("_t9"."agg_1", 0)
ORDER BY
  "value" DESC
LIMIT 10
