SELECT
  drug_id,
  drug_name,
  manufacturer,
  drug_type,
  moa AS mechanism_of_activation,
  fda_appr_dt AS fda_approval_date,
  admin_route AS administration_route,
  dos_amt AS recommended_dosage_amount,
  dos_unit AS dosage_units,
  dos_freq_hrs AS dose_frequency_hours,
  ndc AS national_drug_code
FROM main.drugs
WHERE
  drug_name = 'Drugalin'
