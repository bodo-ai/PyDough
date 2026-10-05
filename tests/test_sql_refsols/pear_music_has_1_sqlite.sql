WITH _t1 AS (
  SELECT
    MAX(customer.supportrepid) AS anything_supportrepid,
    SUM(invoice.total) AS sum_total
  FROM main.customer AS customer
  LEFT JOIN main.invoice AS invoice
    ON customer.customerid = invoice.customerid
  GROUP BY
    customer.customerid
), _s3 AS (
  SELECT
    anything_supportrepid,
    SUM(sum_total) AS sum_sum_total
  FROM _t1
  GROUP BY
    1
)
SELECT
  employee.employeeid AS employee_id,
  COALESCE(_s3.sum_sum_total, 0) AS total_invoice
FROM main.employee AS employee
JOIN _s3 AS _s3
  ON _s3.anything_supportrepid = employee.employeeid
ORDER BY
  1
