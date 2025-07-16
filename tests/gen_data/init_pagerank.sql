-- TODO

CREATE TABLE SITES (
  s_key INTEGER NOT NULL,
  s_name TEXT NOT NULL
);

CREATE TABLE LINKS (
  l_source INTEGER NOT NULL,
  l_target INTEGER
);

-- INSERT INTO SITES (s_key, s_name) VALUES
-- (1, 'Site A'),
-- (2, 'Site B'),
-- (3, 'Site C'),
-- (4, 'Site D'),
-- (5, 'Site E')
-- ;

-- INSERT INTO LINKS (l_source, l_target) VALUES
-- (1, 2), (1, 3), (1, 4), (1, 5),
-- (2, 1), (2, 3),
-- (3, NULL),
-- (4, 1), (4, 2), (4, 3),
-- (5, 1), (5, 4)
-- ;

-- INSERT INTO SITES (s_key, s_name) VALUES
-- (1, 'Site A'),
-- (2, 'Site B'),
-- (3, 'Site C'),
-- (4, 'Site D')
-- ;

-- INSERT INTO LINKS (l_source, l_target) VALUES
-- (1, 2),
-- (2, 1), (2, 3),
-- (3, 4),
-- (4, 1), (4, 2)
-- ;

