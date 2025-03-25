SELECT
  NATION,
  O_YEAR,
  AMOUNT
FROM (
  SELECT
    AMOUNT,
    NATION,
    O_YEAR,
    ordering_1,
    ordering_2
  FROM (
    SELECT
      COALESCE(agg_0, 0) AS AMOUNT,
      nation_name AS NATION,
      nation_name AS ordering_1,
      o_year AS O_YEAR,
      o_year AS ordering_2
    FROM (
      SELECT
        SUM(value) AS agg_0,
        nation_name,
        o_year
      FROM (
        SELECT
          CAST(STRFTIME('%Y', order_date) AS INTEGER) AS o_year,
          (
            extended_price * (
              1 - discount
            )
          ) - (
            supplycost * quantity
          ) AS value,
          nation_name
        FROM (
          SELECT
            discount,
            extended_price,
            nation_name,
            order_date,
            quantity,
            supplycost
          FROM (
            SELECT
              discount,
              extended_price,
              nation_name,
              order_key,
              quantity,
              supplycost
            FROM (
              SELECT
                nation_name,
                part_key,
                supplier_key,
                supplycost
              FROM (
                SELECT
                  nation_name,
                  part_key,
                  supplier_key,
                  supplycost
                FROM (
                  SELECT
                    _table_alias_1.key AS key_2,
                    nation_name
                  FROM (
                    SELECT
                      n_name AS nation_name,
                      n_nationkey AS key
                    FROM tpch.NATION
                  ) AS _table_alias_0
                  INNER JOIN (
                    SELECT
                      s_suppkey AS key,
                      s_nationkey AS nation_key
                    FROM tpch.SUPPLIER
                  ) AS _table_alias_1
                    ON _table_alias_0.key = nation_key
                ) AS _table_alias_2
                INNER JOIN (
                  SELECT
                    ps_partkey AS part_key,
                    ps_suppkey AS supplier_key,
                    ps_supplycost AS supplycost
                  FROM tpch.PARTSUPP
                ) AS _table_alias_3
                  ON key_2 = supplier_key
              ) AS _table_alias_4
              INNER JOIN (
                SELECT
                  key
                FROM (
                  SELECT
                    p_name AS name,
                    p_partkey AS key
                  FROM tpch.PART
                ) AS _t5
                WHERE
                  name LIKE '%green%'
              ) AS _table_alias_5
                ON part_key = key
            ) AS _table_alias_6
            INNER JOIN (
              SELECT
                l_discount AS discount,
                l_extendedprice AS extended_price,
                l_orderkey AS order_key,
                l_partkey AS part_key,
                l_quantity AS quantity,
                l_suppkey AS supplier_key
              FROM tpch.LINEITEM
            ) AS _table_alias_7
              ON (
                _table_alias_6.part_key = _table_alias_7.part_key
              )
              AND (
                _table_alias_6.supplier_key = _table_alias_7.supplier_key
              )
          ) AS _table_alias_8
          LEFT JOIN (
            SELECT
              o_orderkey AS key,
              o_orderdate AS order_date
            FROM tpch.ORDERS
          ) AS _table_alias_9
            ON order_key = key
        ) AS _t4
      ) AS _t3
      GROUP BY
        nation_name,
        o_year
    ) AS _t2
  ) AS _t1
  ORDER BY
    ordering_1,
    ordering_2 DESC
  LIMIT 10
) AS _t0
ORDER BY
  ordering_1,
  ordering_2 DESC
