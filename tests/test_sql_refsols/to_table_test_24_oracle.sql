SELECT
  euro_regions_t24.rkey,
  euro_regions_t24.rname,
  tiers_t24.tier_id,
  tiers_t24.tier_label
FROM euro_regions_t24 euro_regions_t24
CROSS JOIN tiers_t24 tiers_t24
ORDER BY
  3 NULLS FIRST
