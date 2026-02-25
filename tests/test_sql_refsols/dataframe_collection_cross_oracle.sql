SELECT
  USERS.ID_ AS id1,
  USERS.NAME AS name1,
  ORDERS.ORDER_ID AS order_id,
  ORDERS.AMOUNT AS amount
FROM (VALUES
  (1, 'John'),
  (2, 'Jane'),
  (3, 'Bob'),
  (4, 'Alice'),
  (5, 'Charlie')) AS USERS(ID_, NAME)
JOIN (VALUES
  (101.0, 1.0, 250.0),
  (102.0, 2.0, 150.5),
  (103.0, 1.0, 300.0),
  (104.0, 3.0, 450.75),
  (105.0, 2.0, 200.0)) AS ORDERS(ORDER_ID, USER_ID, AMOUNT)
  ON ORDERS.USER_ID = USERS.ID_
