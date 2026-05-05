WITH _u_0 AS (
  SELECT
    nation_key AS _u_1
  FROM asian_nations_t5
  GROUP BY
    1
)
SELECT
  CUSTOMER.c_name COLLATE utf8mb4_bin AS name
FROM tpch.CUSTOMER AS CUSTOMER
LEFT JOIN _u_0 AS _u_0
  ON CUSTOMER.c_nationkey = _u_0._u_1
WHERE
  NOT _u_0._u_1 IS NULL
ORDER BY
  1
LIMIT 5
