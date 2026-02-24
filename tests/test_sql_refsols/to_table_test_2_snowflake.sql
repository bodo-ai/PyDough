SELECT
  name
FROM defog.public.asian_nations_t2
WHERE
  CONTAINS(name, 'I')
