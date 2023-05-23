meteo_bruts = LOAD 'C:/Hadoop/hadoop-3.3.1/isd-history.csv' 
USING PigStorage(';') 
AS (usaf:long,station:chararray,ctry:chararray,state:double,icao:double,lat:double,lon:double,elev:double,beg:long,end:long);


--Afficher l’altitude moyenne des stations. (meteo_bruts.station, meteo_bruts.ctry), 

lat_group = Group meteo_bruts All;
lat_avg = foreach lat_group  generate (meteo_bruts.ctry,meteo_bruts.state), AVG(meteo_bruts.lat); 
-- LA REPONSE EST *********************************************************** :
dump lat_avg;