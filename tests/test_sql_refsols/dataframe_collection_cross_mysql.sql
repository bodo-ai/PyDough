SELECT
  users.id_ AS id1,
  users.name AS name1,
  orders.order_id,
  orders.amount
FROM (VALUES
  ROW(1, 'John'),
  ROW(2, 'Jane'),
  ROW(3, 'Bob'),
  ROW(4, 'Alice'),
  ROW(5, 'Charlie')) AS users(id_, name)
JOIN (VALUES
  ROW(101.0, 1.0, 250.0),
  ROW(102.0, 2.0, 150.5),
  ROW(103.0, 1.0, 300.0),
  ROW(104.0, 3.0, 450.75),
  ROW(105.0, 2.0, 200.0)) AS orders(order_id, user_id, amount)
  ON orders.user_id = users.id_
