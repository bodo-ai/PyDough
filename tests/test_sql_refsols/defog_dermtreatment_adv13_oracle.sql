SELECT
  COUNT(*) AS PIC_female
FROM MAIN.PATIENTS
WHERE
  gender = 'Female' AND ins_type = 'private'
