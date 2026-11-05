SELECT
  COUNT(album."artistid") AS n_albums
FROM main.artist AS artist
LEFT JOIN main.album AS album
  ON album."artistid" = artist."artistid"
WHERE
  artist.name = 'AC/DC'
GROUP BY
  artist."artistid"
