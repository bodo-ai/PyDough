SELECT
  COUNT() AS n
FROM (
  SELECT
    account_balance
  FROM (
    SELECT
      account_balance,
      agg_1
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
        nation_key <= 3
    )
    LEFT JOIN (
      SELECT
        COUNT() AS agg_1,
        supplier_key
      FROM (
        SELECT
          supplier_key
        FROM (
          SELECT
            ps_partkey AS part_key,
            ps_suppkey AS supplier_key,
            ps_supplycost AS supplycost
          FROM tpch.PARTSUPP
        ) AS _table_alias_0
        WHERE
          EXISTS(
            SELECT
              1
            FROM (
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
                    _table_alias_0.supplycost * 1.5
                  )
                )
                AND (
                  container LIKE 'SM%'
                )
            )
            WHERE
              part_key = key
          )
      )
      GROUP BY
        supplier_key
    )
      ON key = supplier_key
  )
  WHERE
    COALESCE(agg_1, 0) > 0
)
