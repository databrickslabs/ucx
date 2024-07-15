SELECT
  col1 AS commandlanguage, /* r, scala, python, sql */
  col2 AS dbr_version_major, /* INT */
  col3 AS dbr_version_minor, /* INT */
  col4 AS dbr_type, /* STRING */
  col5 AS pattern, /* expansion / compatibility with code patterns */
  col6 AS issue /* issue / finding short description */
FROM VALUES
  ('r', NULL, NULL, NULL, NULL, 'AF300.2 - R Language support'),
  ('scala', 13, 3, NULL, NULL, 'AF300.3 - Scala Language support'),
  (NULL, 11, 3, NULL, NULL, 'AF300.4 - Minimum DBR version'),
  (NULL, NULL, NULL, 'cpu', NULL, 'AF300.5 - ML Runtime cpu'),
  (NULL, NULL, NULL, 'gpu', NULL, 'AF300.6 - ML Runtime gpu')