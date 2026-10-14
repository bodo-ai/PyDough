WITH _s3 AS (
  SELECT
    ANY_VALUE(Customer.SupportRepId) AS anything_SupportRepId,
    SUM(Invoice.Total) AS sum_Total
  FROM main.Customer AS Customer
  LEFT JOIN main.Invoice AS Invoice
    ON Customer.CustomerId = Invoice.CustomerId
  GROUP BY
    Customer.CustomerId
)
SELECT
  _s3.anything_SupportRepId AS employee_id,
  COALESCE(SUM(_s3.sum_Total), 0) AS total_invoice
FROM main.Employee AS Employee
JOIN _s3 AS _s3
  ON Employee.EmployeeId = _s3.anything_SupportRepId
GROUP BY
  1
ORDER BY
  1
