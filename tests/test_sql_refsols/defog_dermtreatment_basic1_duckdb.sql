WITH _s1 AS (
  SELECT
    doc_id,
    COUNT(*) AS n_rows,
    SUM(tot_drug_amt) AS sum_tot_drug_amt
  FROM main.treatments
  WHERE
    start_dt >= DATE_TRUNC(
      'DAY',
      CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS TIMESTAMP) - INTERVAL '6' MONTH
    )
  GROUP BY
    1
), _s3 AS (
  SELECT
    doctors.specialty,
    SUM(_s1.n_rows) AS sum_n_rows,
    SUM(_s1.sum_tot_drug_amt) AS sum_sum_tot_drug_amt
  FROM main.doctors AS doctors
  JOIN _s1 AS _s1
    ON _s1.doc_id = doctors.doc_id
  GROUP BY
    1
)
SELECT
  doctors.specialty,
  ANY_VALUE(_s3.sum_n_rows) AS num_treatments,
  COALESCE(ANY_VALUE(_s3.sum_sum_tot_drug_amt), 0) AS total_drug_amount
FROM main.doctors AS doctors
JOIN _s3 AS _s3
  ON _s3.specialty = doctors.specialty
GROUP BY
  1
ORDER BY
  3 DESC
LIMIT 3
