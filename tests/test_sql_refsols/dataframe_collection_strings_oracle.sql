SELECT
  STRINGS.NORMAL_STRINGS AS normal_strings,
  STRINGS.EMPTY_STRING AS empty_string,
  STRINGS.SPECIAL_CHARACTERS AS special_characters
FROM (VALUES
  ('hello', '', '''simple quoted'''),
  ('world', 'not_empty', '"double quoted"'),
  ('pydough', '', 'unicode_ß_ç_ü'),
  (NULL, NULL, NULL),
  ('test_string', ' ', 'tap_space	newline_
_test')) AS STRINGS(NORMAL_STRINGS, EMPTY_STRING, SPECIAL_CHARACTERS)
