SELECT
  "customer"."c_custkey" AS "id",
  ROW_NUMBER() OVER (ORDER BY "customer"."c_acctbal" DESC NULLS FIRST) AS "rk"
FROM "tpch"."customer" AS "customer"
