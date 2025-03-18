SELECT
  region_key,
  nation_name,
  n_above_avg_customers,
  n_above_avg_suppliers,
  min_cust_acctbal,
  max_cust_acctbal
FROM (
  SELECT
    COALESCE(agg_4_22, 0) AS n_above_avg_customers,
    COALESCE(agg_5, 0) AS n_above_avg_suppliers,
    agg_6 AS nation_name,
    agg_6_12 AS max_cust_acctbal,
    agg_7_13 AS min_cust_acctbal,
    agg_9 AS region_key,
    agg_7,
    agg_8
  FROM (
    SELECT
      agg_4 AS agg_4_22,
      agg_5,
      agg_6,
      agg_6_12,
      agg_7,
      agg_7_13,
      agg_8,
      agg_9
    FROM (
      SELECT
        _table_alias_0.agg_6 AS agg_6,
        _table_alias_0.agg_7 AS agg_7,
        _table_alias_1.agg_6 AS agg_6_12,
        _table_alias_1.agg_7 AS agg_7_13,
        agg_3_11,
        agg_5,
        agg_8,
        agg_9
      FROM (
        SELECT
          agg_3_11,
          agg_5,
          agg_6,
          agg_7,
          agg_8,
          agg_9
        FROM (
          SELECT
            MAX(agg_3) AS agg_3_11,
            MAX(agg_6) AS agg_6,
            MAX(agg_7) AS agg_7,
            MAX(agg_8) AS agg_8,
            MAX(agg_9) AS agg_9,
            COUNT() AS agg_5,
            agg_3
          FROM (
            SELECT
              agg_3,
              agg_6,
              agg_7,
              agg_8,
              agg_9
            FROM (
              SELECT
                account_balance,
                agg_1,
                agg_3,
                agg_6,
                agg_7,
                agg_8,
                agg_9
              FROM (
                SELECT
                  MAX(avg_supp_acctbal) AS agg_1,
                  MAX(key) AS agg_3,
                  MAX(nation_name) AS agg_6,
                  MAX(ordering_2) AS agg_7,
                  MAX(ordering_3) AS agg_8,
                  MAX(region_key) AS agg_9,
                  key
                FROM (
                  SELECT
                    avg_supp_acctbal,
                    key,
                    nation_name,
                    ordering_2,
                    ordering_3,
                    region_key
                  FROM (
                    SELECT
                      acctbal,
                      avg_cust_acctbal,
                      avg_supp_acctbal,
                      key,
                      nation_name,
                      ordering_2,
                      ordering_3,
                      region_key
                    FROM (
                      SELECT
                        agg_0 AS avg_cust_acctbal,
                        agg_1 AS avg_supp_acctbal,
                        name AS nation_name,
                        name AS ordering_3,
                        region_key AS ordering_2,
                        key,
                        region_key
                      FROM (
                        SELECT
                          agg_0,
                          key,
                          name,
                          region_key
                        FROM (
                          SELECT
                            n_nationkey AS key,
                            n_name AS name,
                            n_regionkey AS region_key
                          FROM tpch.NATION
                        )
                        LEFT JOIN (
                          SELECT
                            AVG(acctbal) AS agg_0,
                            nation_key
                          FROM (
                            SELECT
                              c_acctbal AS acctbal,
                              c_nationkey AS nation_key
                            FROM tpch.CUSTOMER
                          )
                          GROUP BY
                            nation_key
                        )
                          ON key = nation_key
                      )
                      LEFT JOIN (
                        SELECT
                          AVG(account_balance) AS agg_1,
                          nation_key
                        FROM (
                          SELECT
                            s_acctbal AS account_balance,
                            s_nationkey AS nation_key
                          FROM tpch.SUPPLIER
                        )
                        GROUP BY
                          nation_key
                      )
                        ON key = nation_key
                    )
                    INNER JOIN (
                      SELECT
                        c_acctbal AS acctbal,
                        c_nationkey AS nation_key
                      FROM tpch.CUSTOMER
                    )
                      ON key = nation_key
                  )
                  WHERE
                    acctbal > avg_cust_acctbal
                )
                GROUP BY
                  key
              )
              INNER JOIN (
                SELECT
                  s_acctbal AS account_balance,
                  s_nationkey AS nation_key
                FROM tpch.SUPPLIER
              )
                ON agg_3 = nation_key
            )
            WHERE
              account_balance > agg_1
          )
          GROUP BY
            agg_3
        )
        WHERE
          agg_9 IN (1, 3)
      ) AS _table_alias_0
      LEFT JOIN (
        SELECT
          MAX(acctbal) AS agg_6,
          MIN(acctbal) AS agg_7,
          nation_key
        FROM (
          SELECT
            c_acctbal AS acctbal,
            c_nationkey AS nation_key
          FROM tpch.CUSTOMER
        )
        GROUP BY
          nation_key
      ) AS _table_alias_1
        ON agg_3_11 = nation_key
    )
    INNER JOIN (
      SELECT
        COUNT() AS agg_4,
        key
      FROM (
        SELECT
          key
        FROM (
          SELECT
            acctbal,
            avg_cust_acctbal,
            key
          FROM (
            SELECT
              agg_0 AS avg_cust_acctbal,
              key
            FROM (
              SELECT
                n_nationkey AS key
              FROM tpch.NATION
            )
            LEFT JOIN (
              SELECT
                AVG(acctbal) AS agg_0,
                nation_key
              FROM (
                SELECT
                  c_acctbal AS acctbal,
                  c_nationkey AS nation_key
                FROM tpch.CUSTOMER
              )
              GROUP BY
                nation_key
            )
              ON key = nation_key
          )
          INNER JOIN (
            SELECT
              c_acctbal AS acctbal,
              c_nationkey AS nation_key
            FROM tpch.CUSTOMER
          )
            ON key = nation_key
        )
        WHERE
          acctbal > avg_cust_acctbal
      )
      GROUP BY
        key
    )
      ON agg_3_11 = key
  )
)
ORDER BY
  agg_7,
  agg_8
