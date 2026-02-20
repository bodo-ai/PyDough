SELECT
  COLUMN1 AS normal_strings,
  COLUMN2 AS empty_string,
  COLUMN3 AS special_characters
FROM (VALUES
  ('hello', '', '''simple quoted'''),
  ('world', 'not_empty', '"double quoted"'),
  ('pydough', '', 'unicode_ß_ç_ü'),
  (NULL, NULL, NULL),
  ('test_string', ' ', 'tap_space	newline_
_test')) AS STRINGS(NORMAL_STRINGS, EMPTY_STRING, SPECIAL_CHARACTERS)
