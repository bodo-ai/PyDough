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
        MIN(acctbal) AS smallest_bal
      FROM (
        SELECT
          c_acctbal AS acctbal
        FROM tpch.CUSTOMER
      )
    )
    INNER JOIN (
      SELECT
        n_nationkey AS key,
        n_name AS name
      FROM tpch.NATION
    )
      ON TRUE
  ) AS _table_alias_0
  ANTI JOIN (
    SELECT
      nation_key
    FROM (
      SELECT
        c_acctbal AS acctbal,
        c_nationkey AS nation_key
      FROM tpch.CUSTOMER
    )
    WHERE
      acctbal <= (
        _table_alias_0.smallest_bal + 5.0
      )
  )
    ON key = nation_key
)
ORDER BY
  ordering_1
