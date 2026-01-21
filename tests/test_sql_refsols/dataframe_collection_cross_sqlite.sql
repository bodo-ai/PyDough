SELECT
  users.column1 AS id1,
  users.column2 AS name1,
  orders.column1 AS order_id,
  orders.column3 AS amount
FROM (VALUES
  (1, 'John'),
  (2, 'Jane'),
  (3, 'Bob'),
  (4, 'Alice'),
  (5, 'Charlie')) AS users
JOIN (VALUES
  (101.0, 1.0, 250.0),
  (102.0, 2.0, 150.5),
  (103.0, 1.0, 300.0),
  (104.0, 3.0, 450.75),
  (105.0, 2.0, 200.0)) AS orders
  ON orders.column2 = users.column1
