SELECT
  COUNT() AS n
FROM (
  SELECT
    account_balance
  FROM (
    SELECT
      NTILE(10000) OVER (ORDER BY account_balance NULLS LAST, key NULLS LAST) AS tile,
      account_balance,
      nation_key
    FROM (
      SELECT
        s_acctbal AS account_balance,
        s_nationkey AS nation_key,
        s_suppkey AS key
      FROM tpch.SUPPLIER
    )
  ) AS _table_alias_0
  SEMI JOIN (
    SELECT
      key
    FROM (
      SELECT
        *
      FROM (
        SELECT
          _table_alias_3.key AS key,
          _table_alias_4.key AS key_5,
          acctbal,
          rname
        FROM (
          SELECT
            _table_alias_1.key AS key,
            name AS rname
          FROM (
            SELECT
              n_nationkey AS key,
              n_regionkey AS region_key
            FROM tpch.NATION
          ) AS _table_alias_1
          LEFT JOIN (
            SELECT
              r_regionkey AS key,
              r_name AS name
            FROM tpch.REGION
          ) AS _table_alias_2
            ON region_key = _table_alias_2.key
        ) AS _table_alias_3
        INNER JOIN (
          SELECT
            c_acctbal AS acctbal,
            c_custkey AS key,
            c_nationkey AS nation_key
          FROM tpch.CUSTOMER
        ) AS _table_alias_4
          ON _table_alias_3.key = nation_key
      )
      QUALIFY
        (
          rname = 'EUROPE'
        )
        AND (
          NTILE(10000) OVER (ORDER BY acctbal NULLS LAST, key_5 NULLS LAST) = _table_alias_0.tile
        )
    )
  )
    ON nation_key = key
)
