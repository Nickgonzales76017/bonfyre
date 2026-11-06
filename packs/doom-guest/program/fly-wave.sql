-- One wave through the fly for the scene in ./frames.bin (two frames, the older
-- first, each 320x200 RGB after HEADER bytes). MIRROR is 0, or 1 to present the
-- scene left-right reversed. L1 carries brightness above the frame's mean and
-- brightening since the older frame; L2 the opposite. Activity spreads six
-- synapses through the signed wiring with the eyes held on, and the descending
-- neurons are read over the last four. One row is added to table readout.
CREATE OR REPLACE TEMP TABLE px AS
  SELECT f, CASE WHEN MIRROR = 1 THEN 318 - x ELSE x END AS x, y, sum(v) / 765.0 AS lum
  FROM (SELECT i // (192000 + HEADER) AS f, ((i % (192000 + HEADER)) - HEADER) // 3 AS pix, v
        FROM (SELECT unnest(range(len(l))) AS i, ('0x' || unnest(l))::INTEGER AS v
              FROM (SELECT regexp_extract_all(hex(content), '..') AS l FROM read_blob('frames.bin')))
        WHERE (i % (192000 + HEADER)) >= HEADER),
       LATERAL (SELECT pix % 320 AS x, pix // 320 AS y)
  WHERE x % 2 = 0 AND y % 2 = 0 AND y < 168
  GROUP BY 1, 2, 3;
CREATE OR REPLACE TEMP TABLE seen AS
  SELECT p.f, t.id, avg(p.lum) AS lum FROM tap t JOIN px p ON p.x = t.x AND p.y = t.y GROUP BY 1, 2;
CREATE OR REPLACE TEMP TABLE a0 AS
  SELECT s.id,
         CASE e.cell_type WHEN 'L1' THEN greatest(0, s.lum - m.m) + 3 * greatest(0, s.lum - p.lum)
                          ELSE greatest(0, m.m - s.lum) + 3 * greatest(0, p.lum - s.lum) END AS v
  FROM seen s JOIN eye e USING (id) JOIN seen p ON p.id = s.id AND p.f = 0, (SELECT avg(lum) AS m FROM seen WHERE f = 1) m
  WHERE s.f = 1;
CREATE OR REPLACE TEMP TABLE a1 AS SELECT id, sum(v) AS v FROM (SELECT ws.post AS id, sum(ws.wn * a.v) AS v FROM a0 a JOIN ws ON ws.pre = a.id GROUP BY 1 HAVING sum(ws.wn * a.v) > 0 UNION ALL SELECT * FROM a0) GROUP BY 1;
CREATE OR REPLACE TEMP TABLE a2 AS SELECT id, sum(v) AS v FROM (SELECT ws.post AS id, sum(ws.wn * a.v) AS v FROM a1 a JOIN ws ON ws.pre = a.id GROUP BY 1 HAVING sum(ws.wn * a.v) > 0 UNION ALL SELECT * FROM a0) GROUP BY 1;
CREATE OR REPLACE TEMP TABLE a3 AS SELECT id, sum(v) AS v FROM (SELECT ws.post AS id, sum(ws.wn * a.v) AS v FROM a2 a JOIN ws ON ws.pre = a.id GROUP BY 1 HAVING sum(ws.wn * a.v) > 0 UNION ALL SELECT * FROM a0) GROUP BY 1;
CREATE OR REPLACE TEMP TABLE a4 AS SELECT id, sum(v) AS v FROM (SELECT ws.post AS id, sum(ws.wn * a.v) AS v FROM a3 a JOIN ws ON ws.pre = a.id GROUP BY 1 HAVING sum(ws.wn * a.v) > 0 UNION ALL SELECT * FROM a0) GROUP BY 1;
CREATE OR REPLACE TEMP TABLE a5 AS SELECT id, sum(v) AS v FROM (SELECT ws.post AS id, sum(ws.wn * a.v) AS v FROM a4 a JOIN ws ON ws.pre = a.id GROUP BY 1 HAVING sum(ws.wn * a.v) > 0 UNION ALL SELECT * FROM a0) GROUP BY 1;
CREATE OR REPLACE TEMP TABLE a6 AS SELECT ws.post AS id, sum(ws.wn * a.v) AS v FROM a5 a JOIN ws ON ws.pre = a.id GROUP BY 1 HAVING sum(ws.wn * a.v) > 0;
INSERT INTO readout
  SELECT MIRROR,
         coalesce(sum(s.v) FILTER (WHERE d.cell_type IN ('DNa01', 'DNa02') AND d.side = 'left'), 0),
         coalesce(sum(s.v) FILTER (WHERE d.cell_type IN ('DNa01', 'DNa02') AND d.side = 'right'), 0),
         coalesce(sum(s.v) FILTER (WHERE d.cell_type = 'DNp09'), 0),
         coalesce(sum(s.v) FILTER (WHERE d.cell_type = 'MDN'), 0),
         coalesce(sum(s.v) FILTER (WHERE d.cell_type = 'DNp01'), 0),
         (SELECT count(*) FROM a6)
  FROM dn d LEFT JOIN (SELECT * FROM a3 UNION ALL SELECT * FROM a4 UNION ALL SELECT * FROM a5 UNION ALL SELECT * FROM a6) s ON s.id = d.id;
