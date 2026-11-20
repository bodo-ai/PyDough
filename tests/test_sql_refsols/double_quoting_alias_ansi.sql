SELECT
  COUNT(album."ArtistId") AS n_albums
FROM main.artist AS artist
LEFT JOIN main.album AS album
  ON album."ArtistId" = artist."ArtistId"
WHERE
  artist.name = 'AC/DC'
GROUP BY
  artist."ArtistId"
