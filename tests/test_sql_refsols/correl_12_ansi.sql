SELECT
  brand
FROM (
  SELECT
    _table_alias_0.brand AS brand,
    _table_alias_0.brand AS ordering_2
  FROM (
    SELECT
      agg_1 AS brand_avg_price,
      brand,
      global_avg_price
    FROM (
      SELECT
        AVG(retail_price) AS global_avg_price
      FROM (
        SELECT
          p_retailprice AS retail_price
        FROM tpch.PART
      )
    )
    LEFT JOIN (
      SELECT
        AVG(retail_price) AS agg_1,
        brand
      FROM (
        SELECT
          p_brand AS brand,
          p_retailprice AS retail_price
        FROM tpch.PART
      )
      GROUP BY
        brand
    )
      ON TRUE
  ) AS _table_alias_0
  SEMI JOIN (
    SELECT
      brand
    FROM (
      SELECT
        p_brand AS brand,
        p_retailprice AS retail_price,
        p_size AS size
      FROM tpch.PART
    )
    WHERE
      (
        retail_price < _table_alias_0.global_avg_price
      )
      AND (
        size < 3
      )
      AND (
        retail_price > _table_alias_0.brand_avg_price
      )
  ) AS _table_alias_1
    ON _table_alias_0.brand = _table_alias_1.brand
)
ORDER BY
  ordering_2
