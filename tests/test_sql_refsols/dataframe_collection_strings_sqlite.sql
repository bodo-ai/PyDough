SELECT
  strings.column1 AS normal_strings,
  strings.column2 AS empty_string,
  strings.column3 AS special_characters
FROM (VALUES
  ('hello', '', '''simple quoted'''),
  ('world', 'not_empty', '"double quoted"'),
  ('pydough', '', 'unicode_ß_ç_ü'),
  (NULL, NULL, NULL),
  ('test_string', ' ', 'tap_space	newline_
_test')) AS strings
