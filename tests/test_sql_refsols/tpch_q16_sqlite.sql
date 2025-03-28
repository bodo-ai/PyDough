SELECT
  P_BRAND,
  P_TYPE,
  P_SIZE,
  SUPPLIER_COUNT
FROM (
  SELECT
    COUNT(DISTINCT supplier_key) AS SUPPLIER_COUNT,
    p_brand AS P_BRAND,
    p_size AS P_SIZE,
    p_type AS P_TYPE
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
          )
          WHERE
            (
              brand <> 'BRAND#45'
            )
            AND size IN (49, 14, 23, 45, 19, 3, 36, 9)
            AND (
              NOT part_type LIKE 'MEDIUM POLISHED%%'
            )
        )
        INNER JOIN (
          SELECT
            ps_partkey AS part_key,
            ps_suppkey AS supplier_key
          FROM tpch.PARTSUPP
        )
          ON key = part_key
      )
      LEFT JOIN (
        SELECT
          s_comment AS comment,
          s_suppkey AS key
        FROM tpch.SUPPLIER
      )
        ON supplier_key = key
    )
    WHERE
      NOT comment_2 LIKE '%Customer%Complaints%'
  )
  GROUP BY
    p_brand,
    p_size,
    p_type
)
ORDER BY
  SUPPLIER_COUNT DESC,
  P_BRAND,
  P_TYPE,
  P_SIZE
LIMIT 10
