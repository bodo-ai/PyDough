WITH _s1 AS (
  SELECT
    column1 AS order_id,
    column2 AS user_id,
    column3 AS amount
  FROM (VALUES
    (101.0, 1.0, 250.0),
    (102.0, 2.0, 150.5),
    (103.0, 1.0, 300.0),
    (104.0, 3.0, 450.75),
    (105.0, 2.0, 200.0)) AS orders
)
SELECT
  column1 AS id1,
  column2 AS name1,
  _s1.order_id,
  _s1.amount
FROM (VALUES
  (1, 'John'),
  (2, 'Jane'),
  (3, 'Bob'),
  (4, 'Alice'),
  (5, 'Charlie')) AS users
JOIN _s1 AS _s1
  ON _s1.user_id = column1
