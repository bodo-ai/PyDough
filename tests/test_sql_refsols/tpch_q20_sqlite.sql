SELECT
  S_NAME,
  S_ADDRESS
FROM (
  SELECT
    S_ADDRESS,
    S_NAME,
    ordering_1
  FROM (
    SELECT
      S_NAME AS ordering_1,
      S_ADDRESS,
      S_NAME
    FROM (
      SELECT
        S_ADDRESS,
        S_NAME,
        agg_0,
        name_3
      FROM (
        SELECT
          _table_alias_0.key AS key,
          name AS name_3,
          S_ADDRESS,
          S_NAME
        FROM (
          SELECT
            s_address AS S_ADDRESS,
            s_name AS S_NAME,
            s_nationkey AS nation_key,
            s_suppkey AS key
          FROM tpch.SUPPLIER
        ) AS _table_alias_0
        LEFT JOIN (
          SELECT
            n_nationkey AS key,
            n_name AS name
          FROM tpch.NATION
        ) AS _table_alias_1
          ON nation_key = _table_alias_1.key
      ) AS _table_alias_6
      LEFT JOIN (
        SELECT
          COUNT() AS agg_0,
          supplier_key
        FROM (
          SELECT
            supplier_key
          FROM (
            SELECT
              agg_0,
              availqty,
              supplier_key
            FROM (
              SELECT
                availqty,
                key,
                supplier_key
              FROM (
                SELECT
                  ps_availqty AS availqty,
                  ps_partkey AS part_key,
                  ps_suppkey AS supplier_key
                FROM tpch.PARTSUPP
              ) AS _table_alias_2
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
                  name LIKE 'forest%'
              ) AS _table_alias_3
                ON part_key = key
            ) AS _table_alias_4
            LEFT JOIN (
              SELECT
                SUM(quantity) AS agg_0,
                part_key
              FROM (
                SELECT
                  part_key,
                  quantity
                FROM (
                  SELECT
                    l_partkey AS part_key,
                    l_quantity AS quantity,
                    l_shipdate AS ship_date
                  FROM tpch.LINEITEM
                ) AS _t7
                WHERE
                  (
                    ship_date < '1995-01-01'
                  ) AND (
                    ship_date >= '1994-01-01'
                  )
              ) AS _t6
              GROUP BY
                part_key
            ) AS _table_alias_5
              ON key = part_key
          ) AS _t4
          WHERE
            availqty > (
              COALESCE(agg_0, 0) * 0.5
            )
        ) AS _t3
        GROUP BY
          supplier_key
      ) AS _table_alias_7
        ON key = supplier_key
    ) AS _t2
    WHERE
      (
        (
          name_3 = 'CANADA'
        ) AND COALESCE(agg_0, 0)
      ) > 0
  ) AS _t1
  ORDER BY
    ordering_1
  LIMIT 10
) AS _t0
ORDER BY
  ordering_1
