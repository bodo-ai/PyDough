WITH "_S1" AS (
  SELECT
    COLUMN1 AS ORDER_ID,
    COLUMN2 AS USER_ID,
    COLUMN3 AS AMOUNT
  FROM (VALUES
    (101.0, 1.0, 250.0),
    (102.0, 2.0, 150.5),
    (103.0, 1.0, 300.0),
    (104.0, 3.0, 450.75),
    (105.0, 2.0, 200.0)) AS ORDERS(ORDER_ID, USER_ID, AMOUNT)
)
SELECT
  COLUMN1 AS id1,
  COLUMN2 AS name1,
  "_S1".ORDER_ID AS order_id,
  "_S1".AMOUNT AS amount
FROM (VALUES
  (1, 'John'),
  (2, 'Jane'),
  (3, 'Bob'),
  (4, 'Alice'),
  (5, 'Charlie')) AS USERS(ID_, NAME)
JOIN "_S1" "_S1"
  ON COLUMN1 = "_S1".USER_ID
