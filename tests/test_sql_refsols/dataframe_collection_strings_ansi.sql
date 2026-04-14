SELECT
  column1 AS normal_strings,
  column2 AS empty_string,
  column3 AS special_characters
FROM (VALUES
  ('hello', '', '''simple quoted'''),
  ('world', 'not_empty', '"double quoted"'),
  ('pydough', '', 'unicode_ß_ç_ü'),
  (NULL, NULL, NULL),
  ('test_string', ' ', 'tap_space	newline_
_test')) AS strings(normal_strings, empty_string, special_characters)
