SELECT
  S_ACCTBAL,
  S_NAME,
  N_NAME,
  P_PARTKEY,
  P_MFGR,
  S_ADDRESS,
  S_PHONE,
  S_COMMENT
FROM (
  SELECT
    N_NAME,
    P_MFGR,
    P_PARTKEY,
    S_ACCTBAL,
    S_ADDRESS,
    S_COMMENT,
    S_NAME,
    S_PHONE,
    ordering_1,
    ordering_2,
    ordering_3,
    ordering_4
  FROM (
    SELECT
      key_19 AS P_PARTKEY,
      key_19 AS ordering_4,
      manufacturer AS P_MFGR,
      n_name AS N_NAME,
      n_name AS ordering_2,
      s_acctbal AS S_ACCTBAL,
      s_acctbal AS ordering_1,
      s_address AS S_ADDRESS,
      s_comment AS S_COMMENT,
      s_name AS S_NAME,
      s_name AS ordering_3,
      s_phone AS S_PHONE
    FROM (
      SELECT
        MIN(supplycost) AS best_cost,
        key_9
      FROM (
        SELECT
          key AS key_9,
          supplycost
        FROM (
          SELECT
            part_key,
            supplycost
          FROM (
            SELECT
              _table_alias_3.key AS key_5
            FROM (
              SELECT
                _table_alias_0.key AS key
              FROM (
                SELECT
                  n_nationkey AS key,
                  n_regionkey AS region_key
                FROM tpch.NATION
              ) AS _table_alias_0
              INNER JOIN (
                SELECT
                  key
                FROM (
                  SELECT
                    r_name AS name,
                    r_regionkey AS key
                  FROM tpch.REGION
                )
                WHERE
                  name = 'EUROPE'
              ) AS _table_alias_1
                ON region_key = _table_alias_1.key
            ) AS _table_alias_2
            INNER JOIN (
              SELECT
                s_suppkey AS key,
                s_nationkey AS nation_key
              FROM tpch.SUPPLIER
            ) AS _table_alias_3
              ON _table_alias_2.key = nation_key
          )
          INNER JOIN (
            SELECT
              ps_partkey AS part_key,
              ps_suppkey AS supplier_key,
              ps_supplycost AS supplycost
            FROM tpch.PARTSUPP
          )
            ON key_5 = supplier_key
        )
        INNER JOIN (
          SELECT
            key
          FROM (
            SELECT
              p_partkey AS key,
              p_size AS size,
              p_type AS part_type
            FROM tpch.PART
          )
          WHERE
            (
              size = 15
            ) AND (
              part_type LIKE '%BRASS'
            )
        )
          ON part_key = key
      )
      GROUP BY
        key_9
    )
    INNER JOIN (
      SELECT
        key AS key_19,
        manufacturer,
        n_name,
        s_acctbal,
        s_address,
        s_comment,
        s_name,
        s_phone,
        supplycost
      FROM (
        SELECT
          n_name,
          part_key,
          s_acctbal,
          s_address,
          s_comment,
          s_name,
          s_phone,
          supplycost
        FROM (
          SELECT
            _table_alias_7.key AS key_15,
            account_balance AS s_acctbal,
            address AS s_address,
            comment AS s_comment,
            name AS s_name,
            phone AS s_phone,
            n_name
          FROM (
            SELECT
              _table_alias_4.key AS key,
              n_name
            FROM (
              SELECT
                n_name AS n_name,
                n_nationkey AS key,
                n_regionkey AS region_key
              FROM tpch.NATION
            ) AS _table_alias_4
            INNER JOIN (
              SELECT
                key
              FROM (
                SELECT
                  r_name AS name,
                  r_regionkey AS key
                FROM tpch.REGION
              )
              WHERE
                name = 'EUROPE'
            ) AS _table_alias_5
              ON region_key = _table_alias_5.key
          ) AS _table_alias_6
          INNER JOIN (
            SELECT
              s_acctbal AS account_balance,
              s_address AS address,
              s_comment AS comment,
              s_suppkey AS key,
              s_name AS name,
              s_nationkey AS nation_key,
              s_phone AS phone
            FROM tpch.SUPPLIER
          ) AS _table_alias_7
            ON _table_alias_6.key = nation_key
        )
        INNER JOIN (
          SELECT
            ps_partkey AS part_key,
            ps_suppkey AS supplier_key,
            ps_supplycost AS supplycost
          FROM tpch.PARTSUPP
        )
          ON key_15 = supplier_key
      )
      INNER JOIN (
        SELECT
          key,
          manufacturer
        FROM (
          SELECT
            p_mfgr AS manufacturer,
            p_partkey AS key,
            p_size AS size,
            p_type AS part_type
          FROM tpch.PART
        )
        WHERE
          (
            size = 15
          ) AND (
            part_type LIKE '%BRASS'
          )
      )
        ON part_key = key
    )
      ON (
        key_9 = key_19
      ) AND (
        supplycost = best_cost
      )
  )
  ORDER BY
    ordering_1 DESC,
    ordering_2,
    ordering_3,
    ordering_4
  LIMIT 10
)
ORDER BY
  ordering_1 DESC,
  ordering_2,
  ordering_3,
  ordering_4
