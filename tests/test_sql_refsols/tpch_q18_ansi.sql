SELECT
  C_NAME,
  C_CUSTKEY,
  O_ORDERKEY,
  O_ORDERDATE,
  O_TOTALPRICE,
  TOTAL_QUANTITY
FROM (
  SELECT
    C_CUSTKEY,
    C_NAME,
    O_ORDERDATE,
    O_ORDERKEY,
    O_TOTALPRICE,
    TOTAL_QUANTITY,
    ordering_1,
    ordering_2
  FROM (
    SELECT
      O_ORDERDATE AS ordering_2,
      O_TOTALPRICE AS ordering_1,
      C_CUSTKEY,
      C_NAME,
      O_ORDERDATE,
      O_ORDERKEY,
      O_TOTALPRICE,
      TOTAL_QUANTITY
    FROM (
      SELECT
        COALESCE(agg_0, 0) AS TOTAL_QUANTITY,
        key AS O_ORDERKEY,
        key_2 AS C_CUSTKEY,
        name AS C_NAME,
        order_date AS O_ORDERDATE,
        total_price AS O_TOTALPRICE
      FROM (
        SELECT
          agg_0,
          key,
          key_2,
          name,
          order_date,
          total_price
        FROM (
          SELECT
            _table_alias_0.key AS key,
            _table_alias_1.key AS key_2,
            name,
            order_date,
            total_price
          FROM (
            SELECT
              o_custkey AS customer_key,
              o_orderkey AS key,
              o_orderdate AS order_date,
              o_totalprice AS total_price
            FROM tpch.ORDERS
          ) AS _table_alias_0
          LEFT JOIN (
            SELECT
              c_custkey AS key,
              c_name AS name
            FROM tpch.CUSTOMER
          ) AS _table_alias_1
            ON customer_key = _table_alias_1.key
        )
        LEFT JOIN (
          SELECT
            SUM(quantity) AS agg_0,
            order_key
          FROM (
            SELECT
              l_orderkey AS order_key,
              l_quantity AS quantity
            FROM tpch.LINEITEM
          )
          GROUP BY
            order_key
        )
          ON key = order_key
      )
    )
    WHERE
      TOTAL_QUANTITY > 300
  )
  ORDER BY
    ordering_1 DESC,
    ordering_2
  LIMIT 10
)
ORDER BY
  ordering_1 DESC,
  ordering_2
