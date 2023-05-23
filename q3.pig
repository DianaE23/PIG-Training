meteo_bruts = LOAD 'C:/Hadoop/hadoop-3.3.1/isd-history.csv' 
USING PigStorage(';') 
AS (usaf:long,station:chararray,ctry:chararray,state:double,icao:double,lat:double,lon:double,elev:double,beg:long,end:long);


-- 3 Afficher les codes des pays, en un seul exemplaire des stations de 
-- l’hémisphère sud. 


pays_sud = FILTER meteo_bruts BY lat < 0;

codes_pays = FOREACH pays_sud GENERATE ctry;

unique_codes = DISTINCT codes_pays;

DUMP unique_codes;