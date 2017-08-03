
Updates between aug2015 and july2016

Biggest change is addition of 7 line Hudson Yards station as complex mn119

Performed this test to insure old records matched new

select old.complex_id, old.complex_nm, old.trains, old.tot2014,
new.complex_id, new.complex_nm, new.trains, new.tot2014, 
(old.tot2014-new.tot2014) as differ
from a_subway_complexes as old, subway_complexes as new
where old.complex_id=new.complex_id

CREATE TABLE a_subway_complexes (
	complex_id TEXT NOT NULL PRIMARY KEY,
	complex_nm TEXT,
	trains TEXT,
	station_ct INTEGER,	
	bcode TEXT,
	stop_lat REAL,
	stop_lon REAL,
	tot2007 INTEGER,
	tot2008 INTEGER,
	tot2009 INTEGER,
	tot2010 INTEGER,
	tot2011 INTEGER,
	tot2012 INTEGER,
	tot2013 INTEGER,
	tot2014 INTEGER,
	tot2015 INTEGER,
	avwkdy07 INTEGER,
	avwkdy08 INTEGER,
	avwkdy09 INTEGER,
	avwkdy10 INTEGER,
	avwkdy11 INTEGER,
	avwkdy12 INTEGER,
	avwkdy13 INTEGER,
	avwkdy14 INTEGER,
	avwkdy15 INTEGER,
	avwken07 INTEGER,
	avwken08 INTEGER,
	avwken09 INTEGER,
	avwken10 INTEGER,
	avwken11 INTEGER,
	avwken12 INTEGER,
	avwken13 INTEGER,
	avwken14 INTEGER,
	avwken15 INTEGER,
	srv_notes TEXT);

SELECT AddGeometryColumn (
	'a_subway_complexes', 'geometry', 2263, 'POINT', 'XY');

INSERT INTO a_subway_complexes (complex_id, complex_nm, trains, station_ct, bcode, stop_lat, stop_lon, tot2007, tot2008, tot2009, tot2010, tot2011, tot2012, tot2013, tot2014, tot2015, avwkdy07, avwkdy08, avwkdy09, avwkdy10, avwkdy11, avwkdy12, avwkdy13, avwkdy14, avwkdy15, avwken07, avwken08, avwken09, avwken10, avwken11, avwken12, avwken13, avwken14, avwken15, srv_notes)
SELECT complex_id, complex_nm, trains, station_ct, bcode, stop_lat, stop_lon, tot2007, tot2008, tot2009, tot2010, tot2011, tot2012, tot2013, tot2014, tot2015, avwkdy07, avwkdy08, avwkdy09, avwkdy10, avwkdy11, avwkdy12, avwkdy13, avwkdy14, avwkdy15, avwken07, avwken08, avwken09, avwken10, avwken11, avwken12, avwken13, avwken14, avwken15, srv_notes
FROM subway_complexes
ORDER BY bcode, complex_id, trains;

UPDATE a_subway_complexes SET geometry=Transform(MakePoint(stop_lon, stop_lat, 4269),2263) 

CREATE TABLE a_subway_complexes_notes (
	id INTEGER NOT NULL PRIMARY KEY,
	complex_id TEXT NOT NULL,
	bcode TEXT,
	segment TEXT,
	name TEXT,
	trains TEXT,
	direction TEXT,
	close_dates TEXT)

INSERT INTO a_subway_complexes_notes (id, complex_id, bcode, segment, name, trains, direction, close_dates)
SELECT id, complex_id, bcode, segment, name, trains, direction, close_dates
FROM subway_complexes_notes




