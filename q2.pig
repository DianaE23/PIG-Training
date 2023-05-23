meteo_bruts = LOAD 'C:/Hadoop/hadoop-3.3.1/isd-history.csv' 
USING PigStorage(';') 
AS (usaf:long,station:chararray,ctry:chararray,state:double,icao:double,lat:double,lon:double,elev:double,beg:long,end:long);


-- 2 Afficher les informations des stations qui étaient actives en 1920. Vous 
-- verrez que leur répartition n’est pas homogène, et donc ça va être difficile 
-- de voir une évolution globale pour la Terre. 

station1920 = FILTER meteo_bruts BY beg <= 19201231 AND end >= 19200101;
DUMP station1920;

