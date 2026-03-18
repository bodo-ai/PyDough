SELECT
  COUNT(*) AS PIC_female
FROM postgres.patients
WHERE
  gender = 'Female' AND ins_type = 'private'
