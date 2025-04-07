WITH "_t2" AS (
  SELECT
    "region"."r_name" AS "name",
    "region"."r_regionkey" AS "key"
  FROM "tpch"."region" AS "region"
  WHERE
    "region"."r_name" = 'EUROPE'
), "_t3_2" AS (
  SELECT
    "partsupp"."ps_partkey" AS "part_key",
    "partsupp"."ps_suppkey" AS "supplier_key",
    "partsupp"."ps_supplycost" AS "supplycost"
  FROM "tpch"."partsupp" AS "partsupp"
), "_t5" AS (
  SELECT
    MIN("_t3"."supplycost") AS "agg_0",
    "_t3"."part_key" AS "part_key",
    "_t3"."supplier_key" AS "supplier_key"
  FROM "_t3_2" AS "_t3"
  GROUP BY
    "_t3"."part_key",
    "_t3"."supplier_key"
), "_t6" AS (
  SELECT
    MIN("_t5"."agg_0") AS "agg_0",
    "_t5"."part_key" AS "part_key"
  FROM "tpch"."nation" AS "nation"
  JOIN "_t2" AS "_t2"
    ON "_t2"."key" = "nation"."n_regionkey"
  JOIN "tpch"."supplier" AS "supplier"
    ON "nation"."n_nationkey" = "supplier"."s_nationkey"
  JOIN "_t5" AS "_t5"
    ON "_t5"."supplier_key" = "supplier"."s_suppkey"
  GROUP BY
    "_t5"."part_key"
), "_t16" AS (
  SELECT
    MIN("_t6"."agg_0") AS "best_cost",
    "part"."p_partkey" AS "key_9"
  FROM "_t6" AS "_t6"
  JOIN "tpch"."part" AS "part"
    ON "_t6"."part_key" = "part"."p_partkey"
    AND "part"."p_size" = 15
    AND "part"."p_type" LIKE '%BRASS'
  GROUP BY
    "part"."p_partkey"
), "_t17" AS (
  SELECT
    "part"."p_partkey" AS "p_partkey",
    "part"."p_mfgr" AS "p_mfgr",
    "nation"."n_name" AS "n_name",
    "supplier"."s_acctbal" AS "s_acctbal",
    "supplier"."s_address" AS "s_address",
    "supplier"."s_comment" AS "s_comment",
    "supplier"."s_name" AS "s_name",
    "supplier"."s_phone" AS "s_phone",
    "part"."p_partkey" AS "key_19",
    "_t13"."supplycost" AS "supplycost"
  FROM "tpch"."nation" AS "nation"
  JOIN "_t2" AS "_t5"
    ON "_t5"."key" = "nation"."n_regionkey"
  JOIN "tpch"."supplier" AS "supplier"
    ON "nation"."n_nationkey" = "supplier"."s_nationkey"
  JOIN "_t3_2" AS "_t13"
    ON "_t13"."supplier_key" = "supplier"."s_suppkey"
  JOIN "tpch"."part" AS "part"
    ON "_t13"."part_key" = "part"."p_partkey"
    AND "part"."p_size" = 15
    AND "part"."p_type" LIKE '%BRASS'
)
SELECT
  "_t17"."s_acctbal" AS "S_ACCTBAL",
  "_t17"."s_name" AS "S_NAME",
  "_t17"."n_name" AS "N_NAME",
  "_t17"."p_partkey" AS "P_PARTKEY",
  "_t17"."p_mfgr" AS "P_MFGR",
  "_t17"."s_address" AS "S_ADDRESS",
  "_t17"."s_phone" AS "S_PHONE",
  "_t17"."s_comment" AS "S_COMMENT"
FROM "_t16" AS "_t16"
JOIN "_t17" AS "_t17"
  ON "_t16"."best_cost" = "_t17"."supplycost" AND "_t16"."key_9" = "_t17"."key_19"
ORDER BY
  "s_acctbal" DESC,
  "n_name",
  "s_name",
  "p_partkey"
LIMIT 10
