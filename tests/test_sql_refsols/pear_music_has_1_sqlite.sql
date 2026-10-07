WITH _s3 AS (
  SELECT
    MAX(customer.supportrepid) AS anything_supportrepid,
    SUM(invoice.total) AS sum_total
  FROM main.customer AS customer
  LEFT JOIN main.invoice AS invoice
    ON customer.customerid = invoice.customerid
  GROUP BY
    customer.customerid
)
SELECT
  _s3.anything_supportrepid AS employee_id,
  COALESCE(SUM(_s3.sum_total), 0) AS total_invoice
FROM main.employee AS employee
JOIN _s3 AS _s3
  ON _s3.anything_supportrepid = employee.employeeid
GROUP BY
  1
ORDER BY
  1
