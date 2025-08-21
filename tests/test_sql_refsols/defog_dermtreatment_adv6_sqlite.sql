WITH _s1 AS (
  SELECT
    COUNT(DISTINCT drug_id) AS ndistinct_drug_id,
    doc_id
  FROM main.treatments
  GROUP BY
    doc_id
)
SELECT
  doctors.doc_id,
  doctors.specialty,
  COALESCE(_s1.ndistinct_drug_id, 0) AS num_distinct_drugs,
  ROW_NUMBER() OVER (PARTITION BY doctors.specialty ORDER BY COALESCE(_s1.ndistinct_drug_id, 0) DESC) AS SDRSDR
FROM main.doctors AS doctors
LEFT JOIN _s1 AS _s1
  ON _s1.doc_id = doctors.doc_id
