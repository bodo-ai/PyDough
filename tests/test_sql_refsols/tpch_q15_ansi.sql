SELECT
  S_SUPPKEY,
  S_NAME,
  S_ADDRESS,
  S_PHONE,
  TOTAL_REVENUE
FROM (
  SELECT
    S_SUPPKEY AS ordering_2,
    S_ADDRESS,
    S_NAME,
    S_PHONE,
    S_SUPPKEY,
    TOTAL_REVENUE
  FROM (
    SELECT
      COALESCE(agg_1, 0) AS TOTAL_REVENUE,
      address AS S_ADDRESS,
      key AS S_SUPPKEY,
      name AS S_NAME,
      phone AS S_PHONE,
      max_revenue
    FROM (
      SELECT
        address,
        agg_1,
        key,
        max_revenue,
        name,
        phone
      FROM (
        SELECT
          address,
          key,
          max_revenue,
          name,
          phone
        FROM (
          SELECT
            MAX(total_revenue) AS max_revenue
          FROM (
            SELECT
              COALESCE(agg_0, 0) AS total_revenue
            FROM (
              SELECT
                agg_0
              FROM (
                SELECT
                  s_suppkey AS key
                FROM tpch.SUPPLIER
              )
              LEFT JOIN (
                SELECT
                  SUM(extended_price * (
                    1 - discount
                  )) AS agg_0,
                  supplier_key
                FROM (
                  SELECT
                    discount,
                    extended_price,
                    supplier_key
                  FROM (
                    SELECT
                      l_discount AS discount,
                      l_extendedprice AS extended_price,
                      l_shipdate AS ship_date,
                      l_suppkey AS supplier_key
                    FROM tpch.LINEITEM
                  )
                  WHERE
                    (
                      ship_date < CAST('1996-04-01' AS DATE)
                    )
                    AND (
                      ship_date >= CAST('1996-01-01' AS DATE)
                    )
                )
                GROUP BY
                  supplier_key
              )
                ON key = supplier_key
            )
          )
        )
        INNER JOIN (
          SELECT
            s_address AS address,
            s_suppkey AS key,
            s_name AS name,
            s_phone AS phone
          FROM tpch.SUPPLIER
        )
          ON TRUE
      )
      LEFT JOIN (
        SELECT
          SUM(extended_price * (
            1 - discount
          )) AS agg_1,
          supplier_key
        FROM (
          SELECT
            discount,
            extended_price,
            supplier_key
          FROM (
            SELECT
              l_discount AS discount,
              l_extendedprice AS extended_price,
              l_shipdate AS ship_date,
              l_suppkey AS supplier_key
            FROM tpch.LINEITEM
          )
          WHERE
            (
              ship_date < CAST('1996-04-01' AS DATE)
            )
            AND (
              ship_date >= CAST('1996-01-01' AS DATE)
            )
        )
        GROUP BY
          supplier_key
      )
        ON key = supplier_key
    )
  )
  WHERE
    TOTAL_REVENUE = max_revenue
)
ORDER BY
  ordering_2
