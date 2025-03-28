WITH _t2 AS (
  SELECT
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY customer.c_acctbal DESC) - 1.0
        ) - (
          CAST((
            COUNT(customer.c_acctbal) OVER (PARTITION BY nation.n_regionkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN customer.c_acctbal
      ELSE NULL
    END AS expr_7,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          CAST((
            COUNT(CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END) OVER (PARTITION BY nation.n_regionkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_8,
    CASE
      WHEN ABS(
        (
          ROW_NUMBER() OVER (PARTITION BY nation.n_regionkey ORDER BY CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END DESC) - 1.0
        ) - (
          CAST((
            COUNT(CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END) OVER (PARTITION BY nation.n_regionkey) - 1.0
          ) AS REAL) / 2.0
        )
      ) < 1.0
      THEN CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END
      ELSE NULL
    END AS expr_6,
    CASE WHEN customer.c_acctbal < 0 THEN customer.c_acctbal ELSE NULL END AS negative_acctbal,
    CASE WHEN customer.c_acctbal >= 0 THEN customer.c_acctbal ELSE NULL END AS non_negative_acctbal,
    nation.n_regionkey AS region_key
  FROM tpch.nation AS nation
  JOIN tpch.customer AS customer
    ON customer.c_nationkey = nation.n_nationkey
), _table_alias_3 AS (
  SELECT
    AVG(_t2.expr_6) AS agg_0,
    AVG(_t2.expr_7) AS agg_1,
    AVG(_t2.expr_8) AS agg_2,
    COUNT(_t2.negative_acctbal) AS agg_4,
    COUNT(_t2.non_negative_acctbal) AS agg_3,
    _t2.region_key AS region_key
  FROM _t2 AS _t2
  GROUP BY
    _t2.region_key
)
SELECT
  region.r_name AS region_name,
  COALESCE(_table_alias_3.agg_4, 0) AS n_red_acctbal,
  COALESCE(_table_alias_3.agg_3, 0) AS n_black_acctbal,
  _table_alias_3.agg_2 AS median_red_acctbal,
  _table_alias_3.agg_0 AS median_black_acctbal,
  _table_alias_3.agg_1 AS median_overall_acctbal
FROM tpch.region AS region
LEFT JOIN _table_alias_3 AS _table_alias_3
  ON _table_alias_3.region_key = region.r_regionkey
ORDER BY
  region.r_name
