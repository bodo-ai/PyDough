SELECT
  o_year AS O_YEAR,
  CAST(COALESCE(agg_0, 0) AS REAL) / COALESCE(agg_1, 0) AS MKT_SHARE
FROM (
  SELECT
    SUM(brazil_volume) AS agg_0,
    SUM(volume) AS agg_1,
    o_year
  FROM (
    SELECT
      brazil_volume,
      o_year,
      volume
    FROM (
      SELECT
        CAST(STRFTIME('%Y', order_date) AS INTEGER) AS o_year,
        IIF(nation_name = 'BRAZIL', volume, 0) AS brazil_volume,
        customer_key,
        volume
      FROM (
        SELECT
          customer_key,
          nation_name,
          order_date,
          volume
        FROM (
          SELECT
            extended_price * (
              1 - discount
            ) AS volume,
            nation_name,
            order_key
          FROM (
            SELECT
              discount,
              extended_price,
              nation_name,
              order_key
            FROM (
              SELECT
                nation_name,
                part_key,
                supplier_key
              FROM (
                SELECT
                  nation_name,
                  part_key,
                  supplier_key
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
                    ps_suppkey AS supplier_key
                  FROM tpch.PARTSUPP
                ) AS _table_alias_3
                  ON key_2 = supplier_key
              ) AS _table_alias_4
              INNER JOIN (
                SELECT
                  key
                FROM (
                  SELECT
                    p_partkey AS key,
                    p_type AS part_type
                  FROM tpch.PART
                ) AS _t4
                WHERE
                  part_type = 'ECONOMY ANODIZED STEEL'
              ) AS _table_alias_5
                ON part_key = key
            ) AS _table_alias_6
            INNER JOIN (
              SELECT
                l_discount AS discount,
                l_extendedprice AS extended_price,
                l_orderkey AS order_key,
                l_partkey AS part_key,
                l_suppkey AS supplier_key
              FROM tpch.LINEITEM
            ) AS _table_alias_7
              ON (
                _table_alias_6.part_key = _table_alias_7.part_key
              )
              AND (
                _table_alias_6.supplier_key = _table_alias_7.supplier_key
              )
          ) AS _t3
        ) AS _table_alias_8
        INNER JOIN (
          SELECT
            customer_key,
            key,
            order_date
          FROM (
            SELECT
              o_custkey AS customer_key,
              o_orderdate AS order_date,
              o_orderkey AS key
            FROM tpch.ORDERS
          ) AS _t5
          WHERE
            (
              order_date <= '1996-12-31'
            ) AND (
              order_date >= '1995-01-01'
            )
        ) AS _table_alias_9
          ON order_key = key
      ) AS _t2
    ) AS _table_alias_14
    INNER JOIN (
      SELECT
        _table_alias_12.key AS key
      FROM (
        SELECT
          _table_alias_10.key AS key,
          region_key
        FROM (
          SELECT
            c_custkey AS key,
            c_nationkey AS nation_key
          FROM tpch.CUSTOMER
        ) AS _table_alias_10
        INNER JOIN (
          SELECT
            n_nationkey AS key,
            n_regionkey AS region_key
          FROM tpch.NATION
        ) AS _table_alias_11
          ON nation_key = _table_alias_11.key
      ) AS _table_alias_12
      INNER JOIN (
        SELECT
          key
        FROM (
          SELECT
            r_name AS name,
            r_regionkey AS key
          FROM tpch.REGION
        ) AS _t6
        WHERE
          name = 'AMERICA'
      ) AS _table_alias_13
        ON region_key = _table_alias_13.key
    ) AS _table_alias_15
      ON customer_key = key
  ) AS _t1
  GROUP BY
    o_year
) AS _t0
