SELECT
  name
FROM cassandra.defog.restaurant
WHERE
  LOWER(city_name) = 'los angeles' AND rating > 4
