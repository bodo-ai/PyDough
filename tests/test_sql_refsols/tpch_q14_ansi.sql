SELECT
  (
    100.0 * COALESCE(agg_0, 0)
  ) / COALESCE(agg_1, 0) AS PROMO_REVENUE
FROM (
  SELECT
    SUM(promo_value) AS agg_0,
    SUM(value) AS agg_1
  FROM (
    SELECT
      CASE
        WHEN part_type LIKE 'PROMO%'
        THEN extended_price * (
          1 - discount
        )
        ELSE 0
      END AS promo_value,
      extended_price * (
        1 - discount
      ) AS value
    FROM (
      SELECT
        discount,
        extended_price,
        part_type
      FROM (
        SELECT
          discount,
          extended_price,
          part_key
        FROM (
          SELECT
            l_discount AS discount,
            l_extendedprice AS extended_price,
            l_partkey AS part_key,
            l_shipdate AS ship_date
          FROM tpch.LINEITEM
        )
        WHERE
          (
            ship_date < CAST('1995-10-01' AS DATE)
          )
          AND (
            ship_date >= CAST('1995-09-01' AS DATE)
          )
      )
      LEFT JOIN (
        SELECT
          p_partkey AS key,
          p_type AS part_type
        FROM tpch.PART
      )
        ON part_key = key
    )
  )
)
