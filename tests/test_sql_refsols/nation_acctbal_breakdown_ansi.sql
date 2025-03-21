SELECT
  nation_name,
  n_red_acctbal,
  n_black_acctbal,
  median_red_acctbal,
  median_black_acctbal,
  media_overall_acctbal
FROM (
  SELECT
    COALESCE(agg_3, 0) AS n_black_acctbal,
    COALESCE(agg_4, 0) AS n_red_acctbal,
    agg_0 AS media_overall_acctbal,
    agg_1 AS median_black_acctbal,
    agg_2 AS median_red_acctbal,
    name AS nation_name,
    name AS ordering_5
  FROM (
    SELECT
      agg_0,
      agg_1,
      agg_2,
      agg_3,
      agg_4,
      name
    FROM (
      SELECT
        key,
        name
      FROM (
        SELECT
          _table_alias_0.key AS key,
          _table_alias_0.name AS name,
          _table_alias_1.name AS name_3
        FROM (
          SELECT
            n_nationkey AS key,
            n_name AS name,
            n_regionkey AS region_key
          FROM tpch.NATION
        ) AS _table_alias_0
        LEFT JOIN (
          SELECT
            r_regionkey AS key,
            r_name AS name
          FROM tpch.REGION
        ) AS _table_alias_1
          ON region_key = _table_alias_1.key
      )
      WHERE
        name_3 = 'AMERICA'
    )
    LEFT JOIN (
      SELECT
        COUNT(negative_acctbal) AS agg_4,
        COUNT(non_negative_acctbal) AS agg_3,
        MEDIAN(acctbal) AS agg_0,
        MEDIAN(negative_acctbal) AS agg_2,
        MEDIAN(non_negative_acctbal) AS agg_1,
        nation_key
      FROM (
        SELECT
          CASE WHEN acctbal >= 0 THEN acctbal ELSE NULL END AS non_negative_acctbal,
          CASE WHEN acctbal < 0 THEN acctbal ELSE NULL END AS negative_acctbal,
          acctbal,
          nation_key
        FROM (
          SELECT
            c_acctbal AS acctbal,
            c_nationkey AS nation_key
          FROM tpch.CUSTOMER
        )
      )
      GROUP BY
        nation_key
    )
      ON key = nation_key
  )
)
ORDER BY
  ordering_5
