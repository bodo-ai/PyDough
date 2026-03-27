SELECT
  COUNT(*) AS PIC_female
FROM cassandra.defog.patients
WHERE
  gender = 'Female' AND ins_type = 'private'
