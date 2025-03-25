SELECT
  COALESCE(agg_0, 0) / 7.0 AS AVG_YEARLY
FROM (
  SELECT
    SUM(extended_price) AS agg_0
  FROM (
    SELECT
      extended_price
    FROM (
      SELECT
        extended_price,
        part_avg_quantity,
        quantity
      FROM (
        SELECT
          agg_0 AS part_avg_quantity,
          key
        FROM (
          SELECT
            key
          FROM (
            SELECT
              p_brand AS brand,
              p_container AS container,
              p_partkey AS key
            FROM tpch.PART
          ) AS _t3
          WHERE
            (
              brand = 'Brand#23'
            ) AND (
              container = 'MED BOX'
            )
        ) AS _table_alias_0
        LEFT JOIN (
          SELECT
            AVG(quantity) AS agg_0,
            part_key
          FROM (
            SELECT
              l_partkey AS part_key,
              l_quantity AS quantity
            FROM tpch.LINEITEM
          ) AS _t4
          GROUP BY
            part_key
        ) AS _table_alias_1
          ON key = part_key
      ) AS _table_alias_2
      INNER JOIN (
        SELECT
          l_extendedprice AS extended_price,
          l_partkey AS part_key,
          l_quantity AS quantity
        FROM tpch.LINEITEM
      ) AS _table_alias_3
        ON key = part_key
    ) AS _t2
    WHERE
      quantity < (
        0.2 * part_avg_quantity
      )
  ) AS _t1
) AS _t0
