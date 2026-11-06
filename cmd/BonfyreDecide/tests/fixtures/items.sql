-- Item archive fixture for bonfyre monitor (fixed timestamps; tests pass --days 36500).
CREATE TABLE item(source TEXT, key TEXT, kind TEXT, family TEXT, title TEXT, body TEXT, href TEXT, publisher TEXT, at INTEGER);
INSERT INTO item VALUES('rss','a1','news','x','Target recalls 2M kettles over burn risk','<p>Target Corporation said &amp; recalls</p>','https://e.test/1','Reuters',1790000000);
INSERT INTO item VALUES('rss2','b7','news','x','Target recalls 2M kettles over burn risk','syndicated copy','https://e.test/2','AP',1790006400);
INSERT INTO item VALUES('rss','a2','news','x','Archery target practice opens downtown','A new target range','https://e.test/3','Local',1790080000);
INSERT INTO item VALUES('rss','a3','review','x','Shoppers love Target''s new store layout','Great reviews for target','',NULL,1790070000);
INSERT INTO item VALUES('rss','a4','news','x','Targeted ads rule changes','no whole word here','https://e.test/4','Wire',1790082000);
INSERT INTO item VALUES('rss',5,'news','x',NULL,'Body says TARGET corp sued in court','https://e.test/5','Court News',1790082500);
INSERT INTO item VALUES('rss','old','news','x','Target old news','target','https://e.test/6','Old',-2000000000);
