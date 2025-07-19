-- Custom SQL schema to initialize a custom PageRank database with tables for
-- web sites and links between them. The following assumptions are made:
-- 1. Websites without any outgoing links have an edge (key, NULL) in the LINKS
--    table, to denote that the page implicitly links to all other pages.
-- 2. Every website has a self-link (key, key) in the LINKS table, which should
--    be ignored in PageRank calculations.

CREATE TABLE SITES (
  s_key INTEGER NOT NULL,
  s_name TEXT NOT NULL
);

CREATE TABLE LINKS (
  l_source INTEGER NOT NULL,
  l_target INTEGER
);
