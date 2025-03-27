SELECT
  C_CUSTKEY,
  C_NAME,
  REVENUE,
  C_ACCTBAL,
  N_NAME,
  C_ADDRESS,
  C_PHONE,
  C_COMMENT
FROM (
  SELECT
    C_ACCTBAL,
    C_ADDRESS,
    C_COMMENT,
    C_CUSTKEY,
    C_NAME,
    C_PHONE,
    N_NAME,
    REVENUE,
    ordering_1,
    ordering_2
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS REVENUE,
      COALESCE(agg_0, 0) AS ordering_1,
      acctbal AS C_ACCTBAL,
      address AS C_ADDRESS,
      comment AS C_COMMENT,
      key AS C_CUSTKEY,
      key AS ordering_2,
      name AS C_NAME,
      name_4 AS N_NAME,
      phone AS C_PHONE
    FROM (
      SELECT
        _table_alias_4.key AS key,
        _table_alias_4.name AS name,
        _table_alias_5.name AS name_4,
        acctbal,
        address,
        agg_0,
        comment,
        phone
      FROM (
        SELECT
          acctbal,
          address,
          agg_0,
          comment,
          key,
          name,
          nation_key,
          phone
        FROM (
          SELECT
            c_acctbal AS acctbal,
            c_address AS address,
            c_comment AS comment,
            c_custkey AS key,
            c_name AS name,
            c_nationkey AS nation_key,
            c_phone AS phone
          FROM tpch.CUSTOMER
        ) AS _table_alias_2
        LEFT JOIN (
          SELECT
            SUM(amt) AS agg_0,
            customer_key
          FROM (
            SELECT
              extended_price * (
                1 - discount
              ) AS amt,
              customer_key
            FROM (
              SELECT
                customer_key,
                discount,
                extended_price
              FROM (
                SELECT
                  customer_key,
                  key
                FROM (
                  SELECT
                    o_custkey AS customer_key,
                    o_orderdate AS order_date,
                    o_orderkey AS key
                  FROM tpch.ORDERS
                ) AS _t5
                WHERE
                  (
                    order_date < CAST('1994-01-01' AS DATE)
                  )
                  AND (
                    order_date >= CAST('1993-10-01' AS DATE)
                  )
              ) AS _table_alias_0
              INNER JOIN (
                SELECT
                  discount,
                  extended_price,
                  order_key
                FROM (
                  SELECT
                    l_discount AS discount,
                    l_extendedprice AS extended_price,
                    l_orderkey AS order_key,
                    l_returnflag AS return_flag
                  FROM tpch.LINEITEM
                ) AS _t6
                WHERE
                  return_flag = 'R'
              ) AS _table_alias_1
                ON key = order_key
            ) AS _t4
          ) AS _t3
          GROUP BY
            customer_key
        ) AS _table_alias_3
          ON key = customer_key
      ) AS _table_alias_4
      LEFT JOIN (
        SELECT
          n_nationkey AS key,
          n_name AS name
        FROM tpch.NATION
      ) AS _table_alias_5
        ON nation_key = _table_alias_5.key
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_1 DESC,
    ordering_2
  LIMIT 20
) AS _t0
ORDER BY
  ordering_1 DESC,
  ordering_2
