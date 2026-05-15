WITH _s1 AS (
  SELECT
    doc_id,
    COUNT(DISTINCT drug_id) AS ndistinct_drug_id
  FROM treatments
  GROUP BY
    1
)
SELECT
  doctors.doc_id,
  doctors.specialty,
  _s1.ndistinct_drug_id AS num_distinct_drugs,
  DENSE_RANK() OVER (PARTITION BY doctors.specialty ORDER BY CASE WHEN _s1.ndistinct_drug_id IS NULL THEN 1 ELSE 0 END DESC, _s1.ndistinct_drug_id DESC) AS SDRSDR
FROM doctors AS doctors
JOIN _s1 AS _s1
  ON _s1.doc_id = doctors.doc_id
