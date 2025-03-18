SELECT
  COUNT() AS n
FROM (
  SELECT
    account_balance
  FROM (
    SELECT
      agg_0 AS avg_price,
      account_balance,
      key
    FROM (
      SELECT
        account_balance,
        key
      FROM (
        SELECT
          s_acctbal AS account_balance,
          s_nationkey AS nation_key,
          s_suppkey AS key
        FROM tpch.SUPPLIER
      )
      WHERE
        nation_key = 19
    )
    LEFT JOIN (
      SELECT
        AVG(retail_price) AS agg_0,
        supplier_key
      FROM (
        SELECT
          retail_price,
          supplier_key
        FROM (
          SELECT
            ps_partkey AS part_key,
            ps_suppkey AS supplier_key
          FROM tpch.PARTSUPP
        )
        INNER JOIN (
          SELECT
            p_partkey AS key,
            p_retailprice AS retail_price
          FROM tpch.PART
        )
          ON part_key = key
      )
      GROUP BY
        supplier_key
    )
      ON key = supplier_key
  ) AS _table_alias_0
  SEMI JOIN (
    SELECT
      supplier_key
    FROM (
      SELECT
        ps_partkey AS part_key,
        ps_suppkey AS supplier_key,
        ps_supplycost AS supplycost
      FROM tpch.PARTSUPP
    ) AS _table_alias_1
    SEMI JOIN (
      SELECT
        key
      FROM (
        SELECT
          p_container AS container,
          p_partkey AS key,
          p_retailprice AS retail_price
        FROM tpch.PART
      )
      WHERE
        (
          retail_price < (
            _table_alias_1.supplycost * 1.5
          )
        )
        AND (
          retail_price < _table_alias_0.avg_price
        )
        AND (
          container = 'LG DRUM'
        )
    )
      ON part_key = key
  )
    ON key = supplier_key
)
