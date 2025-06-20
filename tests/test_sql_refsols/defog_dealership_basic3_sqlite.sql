SELECT
  _s0._id AS salesperson_id
FROM main.salespersons AS _s0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.sales AS _s1
    JOIN main.payments_received AS _s2
      ON _s1._id = _s2.sale_id AND _s2.payment_method = 'cash'
    WHERE
      _s0._id = _s1.salesperson_id
  )
