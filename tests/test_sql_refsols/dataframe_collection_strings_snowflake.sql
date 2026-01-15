SELECT
  strings.normal_strings,
  strings.empty_string,
  strings.special_characters
FROM (VALUES
  ('hello', '', '\'simple quoted\''),
  ('world', 'not_empty', '"double quoted"'),
  ('pydough', '', 'unicode_ß_ç_ü'),
  (NULL, NULL, NULL),
  ('test_string', ' ', 'tap_space\tnewline_\n_test')) AS strings(normal_strings, empty_string, special_characters)
