SELECT DISTINCT
  author.name AS author_name
FROM author AS author
JOIN writes AS writes
  ON author.aid = writes.aid
JOIN domain_publication AS domain_publication
  ON domain_publication.pid = writes.pid
JOIN domain AS domain
  ON domain.did = domain_publication.did AND domain.name = 'Computer Science'
