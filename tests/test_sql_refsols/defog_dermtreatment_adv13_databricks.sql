SELECT
  COUNT(*) AS PIC_female
FROM defog.dermtreatment.patients
WHERE
  gender = 'Female' AND ins_type = 'private'
