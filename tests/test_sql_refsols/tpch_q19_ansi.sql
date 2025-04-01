SELECT
  COALESCE(agg_0, 0) AS REVENUE
FROM (
  SELECT
    SUM(expr_1) AS agg_0
  FROM (
    SELECT
      extended_price * (
        1 - discount
      ) AS expr_1
    FROM (
      SELECT
        discount,
        extended_price
      FROM (
        SELECT
          brand,
          container,
          discount,
          extended_price,
          quantity,
          size
        FROM (
          SELECT
            discount,
            extended_price,
            part_key,
            quantity
          FROM (
            SELECT
              l_discount AS discount,
              l_extendedprice AS extended_price,
              l_partkey AS part_key,
              l_quantity AS quantity,
              l_shipinstruct AS ship_instruct,
              l_shipmode AS ship_mode
            FROM tpch.LINEITEM
          )
          WHERE
            (
              ship_instruct = 'DELIVER IN PERSON'
            ) AND ship_mode IN ('AIR', 'AIR REG')
        )
        INNER JOIN (
          SELECT
            brand,
            container,
            key,
            size
          FROM (
            SELECT
              p_brand AS brand,
              p_container AS container,
              p_partkey AS key,
              p_size AS size
            FROM tpch.PART
          )
          WHERE
            size >= 1
        )
          ON part_key = key
      )
      WHERE
        (
          (
            (
              size <= 5
            )
            AND (
              quantity >= 1
            )
            AND (
              quantity <= 11
            )
            AND container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            AND (
              brand = 'Brand#12'
            )
          )
          OR (
            (
              size <= 10
            )
            AND (
              quantity >= 10
            )
            AND (
              quantity <= 20
            )
            AND container IN ('MED BAG', 'MED BOX', 'MED PACK', 'MED PKG')
            AND (
              brand = 'Brand#23'
            )
          )
        )
        OR (
          (
            size <= 15
          )
          AND (
            quantity >= 20
          )
          AND (
            quantity <= 30
          )
          AND container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
          AND (
            brand = 'Brand#34'
          )
        )
    )
  )
)
