SELECT
  name
FROM (
  SELECT
    name AS ordering_1,
    name
  FROM (
    SELECT
      key,
      name,
      smallest_bal
    FROM (
      SELECT
        MIN(account_balance) AS smallest_bal
      FROM (
        SELECT
          s_acctbal AS account_balance
        FROM tpch.SUPPLIER
      )
    )
    INNER JOIN (
      SELECT
        r_regionkey AS key,
        r_name AS name
      FROM tpch.REGION
    )
      ON TRUE
  ) AS _table_alias_0
  WHERE
    EXISTS(
      SELECT
        1
      FROM (
        SELECT
          region_key
        FROM (
          SELECT
            n_nationkey AS key,
            n_regionkey AS region_key
          FROM tpch.NATION
        )
        INNER JOIN (
          SELECT
            nation_key
          FROM (
            SELECT
              s_acctbal AS account_balance,
              s_nationkey AS nation_key
            FROM tpch.SUPPLIER
          )
          WHERE
            account_balance <= (
              _table_alias_0.smallest_bal + 4.0
            )
        )
          ON key = nation_key
      )
      WHERE
        key = region_key
    )
)
ORDER BY
  ordering_1
