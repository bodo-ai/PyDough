SELECT
  CNTRY_CODE,
  NUM_CUSTS,
  TOTACCTBAL
FROM (
  SELECT
    COALESCE(agg_1, 0) AS NUM_CUSTS,
    COALESCE(agg_2, 0) AS TOTACCTBAL,
    cntry_code AS CNTRY_CODE,
    cntry_code AS ordering_3
  FROM (
    SELECT
      COUNT() AS agg_1,
      SUM(acctbal) AS agg_2,
      cntry_code
    FROM (
      SELECT
        acctbal,
        cntry_code
      FROM (
        SELECT
          acctbal,
          agg_0,
          cntry_code
        FROM (
          SELECT
            acctbal,
            cntry_code,
            key
          FROM (
            SELECT
              SUBSTRING(phone, 1, 2) AS cntry_code,
              acctbal,
              key
            FROM (
              SELECT
                acctbal,
                key,
                phone
              FROM (
                SELECT
                  acctbal,
                  global_avg_balance,
                  key,
                  phone
                FROM (
                  SELECT
                    AVG(acctbal) AS global_avg_balance
                  FROM (
                    SELECT
                      acctbal
                    FROM (
                      SELECT
                        SUBSTRING(phone, 1, 2) AS cntry_code,
                        acctbal
                      FROM (
                        SELECT
                          acctbal,
                          phone
                        FROM (
                          SELECT
                            c_acctbal AS acctbal,
                            c_phone AS phone
                          FROM tpch.CUSTOMER
                        ) AS _t10
                        WHERE
                          acctbal > 0.0
                      ) AS _t9
                    ) AS _t8
                    WHERE
                      cntry_code IN ('13', '31', '23', '29', '30', '18', '17')
                  ) AS _t7
                ) AS _table_alias_0
                INNER JOIN (
                  SELECT
                    c_acctbal AS acctbal,
                    c_custkey AS key,
                    c_phone AS phone
                  FROM tpch.CUSTOMER
                ) AS _table_alias_1
                  ON TRUE
              ) AS _t6
              WHERE
                acctbal > global_avg_balance
            ) AS _t5
          ) AS _t4
          WHERE
            cntry_code IN ('13', '31', '23', '29', '30', '18', '17')
        ) AS _table_alias_2
        LEFT JOIN (
          SELECT
            COUNT() AS agg_0,
            customer_key
          FROM (
            SELECT
              o_custkey AS customer_key
            FROM tpch.ORDERS
          ) AS _t11
          GROUP BY
            customer_key
        ) AS _table_alias_3
          ON key = customer_key
      ) AS _t3
      WHERE
        COALESCE(agg_0, 0) = 0
    ) AS _t2
    GROUP BY
      cntry_code
  ) AS _t1
) AS _t0
ORDER BY
  ordering_3
