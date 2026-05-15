SELECT
  COUNT(*) AS PIC_female
FROM dermtreatment.patients
WHERE
  gender = 'Female' AND ins_type = 'private'
