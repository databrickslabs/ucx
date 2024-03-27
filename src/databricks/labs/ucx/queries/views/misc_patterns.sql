SELECT 
    col1 AS commandLanguage,        -- r, scala, python, sql
    col2 as dbr_version_major,      -- INT 
    col3 as dbr_version_minor,      -- INT
    col4 as dbr_type,               -- STRING
    col5 AS pattern,                -- expansion / compatibility with code patterns
    col6 AS issue                   -- issue / finding short description
FROM VALUES
    ('r',       null,   null,   null,       null,  'AF300.2 - R Language support'),
    ('scala',   13,     3,      null,       null,  'AF300.3 - Scala Language support'),
    (null,      11,     3,      null,       null,  'AF300.4 - Minimum DBR version'),
    (null,      null,   null,   'cpu',      null,  'AF300.5 - ML Runtime cpu'),
    (null,      null,   null,   'gpu',      null,  'AF300.6 - ML Runtime gpu')