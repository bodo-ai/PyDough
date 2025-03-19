SELECT
  SUM(TEMP_COL0) AS REVENUE
FROM (
  SELECT
    L_EXTENDEDPRICE * L_DISCOUNT AS TEMP_COL0
  FROM (
    SELECT
      L_DISCOUNT,
      L_EXTENDEDPRICE
    FROM (
      SELECT
        L_DISCOUNT,
        L_EXTENDEDPRICE,
        L_QUANTITY,
        L_SHIPDATE
      FROM LINEITEM
    )
    WHERE
      (
        L_QUANTITY < 24
      )
      AND (
        L_DISCOUNT <= 0.07
      )
      AND (
        L_DISCOUNT >= 0.05
      )
      AND (
        L_SHIPDATE < '1995-01-01'
      )
      AND (
        L_SHIPDATE >= '1994-01-01'
      )
  )
)
