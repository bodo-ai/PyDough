SELECT
  COUNT(*) AS PIC_female
FROM postgres.main.patients
WHERE
  gender = 'Female' AND ins_type = 'private'
