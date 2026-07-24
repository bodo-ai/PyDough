WITH _u_0 AS (
  SELECT
    salesperson_id AS _u_1
  FROM cassandra.defog.sales
  GROUP BY
    1
)
SELECT
  salespersons._id,
  salespersons.first_name,
  salespersons.last_name
FROM mongo.defog.salespersons AS salespersons
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = salespersons._id
WHERE
  _u_0._u_1 IS NULL
