WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(tot_drug_amt) AS sum_tot_drug_amt,
    doc_id
  FROM main.treatments
  WHERE
    (
      (
        CAST(STRFTIME('%Y', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%Y', start_dt) AS INTEGER)
      ) * 12 + CAST(STRFTIME('%m', DATETIME('now')) AS INTEGER) - CAST(STRFTIME('%m', start_dt) AS INTEGER)
    ) <= 6
  GROUP BY
    doc_id
)
SELECT
  doctors.specialty,
  SUM(_s0.n_rows) AS num_treatments,
  COALESCE(SUM(_s0.sum_tot_drug_amt), 0) AS total_drug_amount
FROM _s0 AS _s0
JOIN main.doctors AS doctors
  ON _s0.doc_id = doctors.doc_id
GROUP BY
  doctors.specialty
ORDER BY
  COALESCE(SUM(_s0.sum_tot_drug_amt), 0) DESC
LIMIT 3
