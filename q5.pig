meteo_bruts = LOAD 'C:/Hadoop/hadoop-3.3.1/isd-history.csv' 
USING PigStorage(';') 
AS (usaf:long,station:chararray,ctry:chararray,state:double,icao:double,lat:double,lon:double,elev:double,beg:long,end:long);


-- 5 Afficher le nombre total de stations différentes par leur code ICAO du 
-- fichier. De nombreuses stations sont des doublons (ou n’ont pas de code 
-- ICAO). Elles ont généré des relevés à des périodes discontinues.

icao2  = FOREACH meteo_bruts GENERATE icao;

C = GROUP icao2 by icao;
D = FOREACH C GENERATE group as icao,COUNT(icao2) as totalcount;
E = ORDER D BY totalcount DESC;
--Le résultat est : 
DUMP E;