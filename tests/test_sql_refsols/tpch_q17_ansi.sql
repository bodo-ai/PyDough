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
        )
        WHERE
          (
            brand = 'Brand#23'
          ) AND (
            container = 'MED BOX'
          )
      )
      LEFT JOIN (
        SELECT
          AVG(quantity) AS agg_0,
          part_key
        FROM (
          SELECT
            l_partkey AS part_key,
            l_quantity AS quantity
          FROM tpch.LINEITEM
        )
        GROUP BY
          part_key
      )
        ON key = part_key
    )
    INNER JOIN (
      SELECT
        l_extendedprice AS extended_price,
        l_partkey AS part_key,
        l_quantity AS quantity
      FROM tpch.LINEITEM
    )
      ON (
        key = part_key
      ) AND (
        quantity < (
          0.2 * part_avg_quantity
        )
      )
  )
)
