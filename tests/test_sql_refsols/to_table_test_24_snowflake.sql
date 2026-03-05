SELECT
  euro_regions_t24.rkey,
  euro_regions_t24.rname,
  tiers_t24.tier_id,
  tiers_t24.tier_label
FROM e2e_tests_db.public.euro_regions_t24 AS euro_regions_t24
CROSS JOIN e2e_tests_db.public.tiers_t24 AS tiers_t24
ORDER BY
  3 NULLS FIRST
