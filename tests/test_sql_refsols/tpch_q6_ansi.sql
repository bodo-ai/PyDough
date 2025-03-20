SELECT
  COALESCE(agg_0, 0) AS REVENUE
FROM (
  SELECT
    SUM(amt) AS agg_0
  FROM (
    SELECT
      extended_price * discount AS amt
    FROM (
      SELECT
        discount,
        extended_price
      FROM (
        SELECT
          l_discount AS discount,
          l_extendedprice AS extended_price,
          l_quantity AS quantity,
          l_shipdate AS ship_date
        FROM tpch.LINEITEM
      )
      WHERE
        (
          discount <= 0.07
        )
        AND (
          quantity < 24
        )
        AND (
          ship_date < CAST('1995-01-01' AS DATE)
        )
        AND (
          discount >= 0.05
        )
        AND (
          ship_date >= CAST('1994-01-01' AS DATE)
        )
    )
  )
)
