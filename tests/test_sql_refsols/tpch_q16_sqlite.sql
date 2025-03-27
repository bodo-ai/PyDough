SELECT
  P_BRAND,
  P_TYPE,
  P_SIZE,
  SUPPLIER_COUNT
FROM (
  SELECT
    P_BRAND,
    P_SIZE,
    P_TYPE,
    SUPPLIER_COUNT,
    ordering_1,
    ordering_2,
    ordering_3,
    ordering_4
  FROM (
    SELECT
      COUNT(DISTINCT supplier_key) AS SUPPLIER_COUNT,
      COUNT(DISTINCT supplier_key) AS ordering_1,
      p_brand AS P_BRAND,
      p_brand AS ordering_2,
      p_size AS P_SIZE,
      p_size AS ordering_4,
      p_type AS P_TYPE,
      p_type AS ordering_3
    FROM (
      SELECT
        p_brand,
        p_size,
        p_type,
        supplier_key
      FROM (
        SELECT
          comment AS comment_2,
          p_brand,
          p_size,
          p_type,
          supplier_key
        FROM (
          SELECT
            p_brand,
            p_size,
            p_type,
            supplier_key
          FROM (
            SELECT
              brand AS p_brand,
              part_type AS p_type,
              size AS p_size,
              key
            FROM (
              SELECT
                p_brand AS brand,
                p_partkey AS key,
                p_size AS size,
                p_type AS part_type
              FROM tpch.PART
            ) AS _t4
            WHERE
              (
                brand <> 'BRAND#45'
              )
              AND size IN (49, 14, 23, 45, 19, 3, 36, 9)
              AND (
                NOT part_type LIKE 'MEDIUM POLISHED%%'
              )
          ) AS _table_alias_0
          INNER JOIN (
            SELECT
              ps_partkey AS part_key,
              ps_suppkey AS supplier_key
            FROM tpch.PARTSUPP
          ) AS _table_alias_1
            ON key = part_key
        ) AS _table_alias_2
        LEFT JOIN (
          SELECT
            s_comment AS comment,
            s_suppkey AS key
          FROM tpch.SUPPLIER
        ) AS _table_alias_3
          ON supplier_key = key
      ) AS _t3
      WHERE
        NOT comment_2 LIKE '%Customer%Complaints%'
    ) AS _t2
    GROUP BY
      p_brand,
      p_size,
      p_type
  ) AS _t1
  ORDER BY
    ordering_1 DESC,
    ordering_2,
    ordering_3,
    ordering_4
  LIMIT 10
) AS _t0
ORDER BY
  ordering_1 DESC,
  ordering_2,
  ordering_3,
  ordering_4
