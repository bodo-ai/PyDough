SELECT
  COUNT(*) AS PIC_female
FROM patients
WHERE
  gender = 'Female' AND ins_type = 'private'
