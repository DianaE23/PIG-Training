meteo_bruts = LOAD 'C:/Hadoop/hadoop-3.3.1/isd-history.csv' 
USING PigStorage(';') 
AS (usaf:long,station:chararray,ctry:chararray,state:double,icao:double,lat:double,lon:double,elev:double,beg:long,end:long);


-- 4 Afficher le code du pays qui a le plus de stations, afficher aussi le nombre 
-- de stations qu’il possède.

codes_pays = FOREACH meteo_bruts GENERATE ctry;

C = GROUP codes_pays BY ctry;
D = FOREACH C GENERATE group as ctry,COUNT(codes_pays) as totalcount;
E = ORDER D BY totalcount DESC;
F = LIMIT E 1;
--Le résultat est : 
DUMP F;