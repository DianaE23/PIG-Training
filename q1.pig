meteo_bruts = LOAD 'C:/Hadoop/hadoop-3.3.1/isd-history.csv' 
USING PigStorage(';') 
AS (usaf:long,station:chararray,ctry:chararray,state:double,icao:double,lat:double,lon:double,elev:double,beg:long,end:long);


-- 1 Afficher les informations des stations nommées “LANNION”

lannion = FILTER meteo_bruts BY station == 'LANNION';
DESCRIBE lannion;
DUMP lannion;


