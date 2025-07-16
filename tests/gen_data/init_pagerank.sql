-- Custom SQL schema to initialize a custom PageRank database with tables for
-- web sites and links between them. The following assumptions are made:
-- 1. Websites without any outgoing links have an edge (key, NULL) in the LINKS
--    table, to denote that the page implicitly links to all other pages.
-- 2. If there are no websites without any outgoing links, then any websites
--    without incoming links have a dummy self-link for simplicity, which
--    should not be counted in the PageRank calculation (but is required for
-- joins to work).

CREATE TABLE SITES (
  s_key INTEGER NOT NULL,
  s_name TEXT NOT NULL
);

CREATE TABLE LINKS (
  l_source INTEGER NOT NULL,
  l_target INTEGER
);
