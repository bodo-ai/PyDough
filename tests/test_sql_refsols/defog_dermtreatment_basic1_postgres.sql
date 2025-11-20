WITH _s1 AS (
  SELECT
    doc_id,
    COUNT(*) AS n_rows,
    SUM(tot_drug_amt) AS sum_totdrugamt
  FROM main.treatments
  WHERE
    start_dt >= DATE_TRUNC('DAY', CURRENT_TIMESTAMP - INTERVAL '6 MONTH')
  GROUP BY
    1
), _t1 AS (
  SELECT
    doctors.specialty,
    SUM(_s1.n_rows) AS sum_nrows,
    SUM(_s1.sum_totdrugamt) AS sum_sumtotdrugamt
  FROM main.doctors AS doctors
  LEFT JOIN _s1 AS _s1
    ON _s1.doc_id = doctors.doc_id
  GROUP BY
    1
)
SELECT
  specialty,
  sum_nrows AS num_treatments,
  COALESCE(sum_sumtotdrugamt, 0) AS total_drug_amount
FROM _t1
WHERE
  sum_nrows > 0
ORDER BY
  3 DESC NULLS LAST
LIMIT 3
