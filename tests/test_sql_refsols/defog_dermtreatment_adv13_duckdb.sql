SELECT
  COUNT(*) AS PIC_female
FROM main.patients
WHERE
  gender = 'Female' AND ins_type = 'private'
