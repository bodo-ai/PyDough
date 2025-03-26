SELECT
  _id
FROM (
  SELECT
    _id
  FROM main.customers
)
SEMI JOIN (
  SELECT
    customer_id
  FROM main.sales
)
  ON _id = customer_id
