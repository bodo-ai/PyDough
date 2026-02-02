SELECT
  strings.normal_strings,
  strings.empty_string,
  strings.special_characters
FROM (VALUES
  ROW('hello', '', '''simple quoted'''),
  ROW('world', 'not_empty', '"double quoted"'),
  ROW('pydough', '', 'unicode_ß_ç_ü'),
  ROW(NULL, NULL, NULL),
  ROW('test_string', ' ', 'tap_space\tnewline_\n_test')) AS strings(normal_strings, empty_string, special_characters)
