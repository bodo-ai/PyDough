SELECT
  brand
FROM (
  SELECT
    _table_alias_0.brand AS brand,
    _table_alias_0.brand AS ordering_1
  FROM (
    SELECT
      AVG(retail_price) AS avg_price,
      brand
    FROM (
      SELECT
        p_brand AS brand,
        p_retailprice AS retail_price
      FROM tpch.PART
    )
    GROUP BY
      brand
  ) AS _table_alias_0
  SEMI JOIN (
    SELECT
      brand
    FROM (
      SELECT
        p_brand AS brand,
        p_retailprice AS retail_price
      FROM tpch.PART
    )
    WHERE
      retail_price > (
        1.4 * _table_alias_0.avg_price
      )
  ) AS _table_alias_1
    ON _table_alias_0.brand = _table_alias_1.brand
)
ORDER BY
  ordering_1
