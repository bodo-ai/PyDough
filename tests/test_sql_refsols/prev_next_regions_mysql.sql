SELECT
  LAG(r_name, 2) OVER (ORDER BY CASE WHEN r_name COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, r_name COLLATE utf8mb4_bin) AS two_preceding,
  LAG(r_name, 1) OVER (ORDER BY CASE WHEN r_name COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, r_name COLLATE utf8mb4_bin) AS one_preceding,
  r_name COLLATE utf8mb4_bin AS current_region,
  LEAD(r_name, 1) OVER (ORDER BY CASE WHEN r_name COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, r_name COLLATE utf8mb4_bin) AS one_following,
  LEAD(r_name, 2) OVER (ORDER BY CASE WHEN r_name COLLATE utf8mb4_bin IS NULL THEN 1 ELSE 0 END, r_name COLLATE utf8mb4_bin) AS two_following
FROM tpch.REGION
ORDER BY
  3
