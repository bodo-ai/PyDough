WITH _t1 AS (
  SELECT
    ANY_VALUE(Customer.supportrepid) AS anything_SupportRepId,
    SUM(Invoice.total) AS sum_Total
  FROM main.Customer AS Customer
  LEFT JOIN main.Invoice AS Invoice
    ON Customer.customerid = Invoice.customerid
  GROUP BY
    Customer.customerid
), _s3 AS (
  SELECT
    anything_SupportRepId,
    SUM(sum_Total) AS sum_sum_Total
  FROM _t1
  GROUP BY
    1
)
SELECT
  Employee.employeeid AS employee_id,
  COALESCE(_s3.sum_sum_Total, 0) AS total_invoice
FROM main.Employee AS Employee
JOIN _s3 AS _s3
  ON Employee.employeeid = _s3.anything_SupportRepId
ORDER BY
  1
