SELECT
  _id
FROM (
  SELECT
    _id
  FROM main.customers
)
WHERE
  EXISTS(
    SELECT
      1
    FROM (
      SELECT
        customer_id
      FROM main.sales
    )
    WHERE
      _id = customer_id
  )
