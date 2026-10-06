WITH _s3 AS (
  SELECT
    ANY_VALUE(Customer.supportrepid) AS anything_SupportRepId,
    SUM(Invoice.total) AS sum_Total
  FROM main.Customer AS Customer
  LEFT JOIN main.Invoice AS Invoice
    ON Customer.customerid = Invoice.customerid
  GROUP BY
    Customer.customerid
)
SELECT
  _s3.anything_SupportRepId AS employee_id,
  COALESCE(SUM(_s3.sum_Total), 0) AS total_invoice
FROM main.Employee AS Employee
JOIN _s3 AS _s3
  ON Employee.employeeid = _s3.anything_SupportRepId
GROUP BY
  1
ORDER BY
  1
