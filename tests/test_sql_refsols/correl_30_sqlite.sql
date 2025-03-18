SELECT
  region_name,
  nation_name,
  n_above_avg_customers,
  n_above_avg_suppliers
FROM (
  SELECT
    COALESCE(agg_2_20, 0) AS n_above_avg_customers,
    COALESCE(agg_3, 0) AS n_above_avg_suppliers,
    agg_5 AS nation_name,
    agg_5 AS ordering_5,
    agg_7 AS ordering_4,
    agg_7 AS region_name
  FROM (
    SELECT
      agg_2 AS agg_2_20,
      agg_3,
      agg_5,
      agg_7
    FROM (
      SELECT
        MAX(agg_4) AS agg_4_10,
        MAX(agg_5) AS agg_5,
        MAX(agg_7) AS agg_7,
        COUNT() AS agg_3,
        agg_4
      FROM (
        SELECT
          agg_4,
          agg_5,
          agg_7
        FROM (
          SELECT
            account_balance,
            agg_1,
            agg_4,
            agg_5,
            agg_7
          FROM (
            SELECT
              MAX(avg_supp_acctbal) AS agg_1,
              MAX(key) AS agg_4,
              MAX(name) AS agg_5,
              MAX(region_name) AS agg_7,
              key
            FROM (
              SELECT
                avg_supp_acctbal,
                key,
                name,
                region_name
              FROM (
                SELECT
                  acctbal,
                  avg_cust_acctbal,
                  avg_supp_acctbal,
                  key,
                  name,
                  region_name
                FROM (
                  SELECT
                    agg_0 AS avg_cust_acctbal,
                    agg_1 AS avg_supp_acctbal,
                    LOWER(name_4) AS region_name,
                    key,
                    name
                  FROM (
                    SELECT
                      agg_0,
                      agg_1,
                      key,
                      name,
                      name_4
                    FROM (
                      SELECT
                        _table_alias_0.key AS key,
                        _table_alias_0.name AS name,
                        _table_alias_1.name AS name_4,
                        agg_0,
                        agg_1
                      FROM (
                        SELECT
                          agg_0,
                          agg_1,
                          key,
                          name,
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
                      NOT name_4 IN ('MIDDLE EAST', 'AFRICA', 'ASIA')
                  )
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
            ON agg_4 = nation_key
        )
        WHERE
          account_balance > agg_1
      )
      GROUP BY
        agg_4
    )
    INNER JOIN (
      SELECT
        COUNT() AS agg_2,
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
                _table_alias_2.key AS key,
                name AS name_13,
                agg_0
              FROM (
                SELECT
                  agg_0,
                  key,
                  region_key
                FROM (
                  SELECT
                    n_nationkey AS key,
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
              ) AS _table_alias_2
              LEFT JOIN (
                SELECT
                  r_regionkey AS key,
                  r_name AS name
                FROM tpch.REGION
              ) AS _table_alias_3
                ON region_key = _table_alias_3.key
            )
            WHERE
              NOT name_13 IN ('MIDDLE EAST', 'AFRICA', 'ASIA')
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
      ON agg_4_10 = key
  )
)
ORDER BY
  ordering_4,
  ordering_5
