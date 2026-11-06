-- Build one map's world from the WAD: passable sector edges (locked doors carry
-- their key colour), the exit line, and key cards with the sector they lie in.
-- WADPATH, MAPNAME and MAPNUMBER are substituted by the caller.
CREATE MACRO u16(h, o) AS (('0x' || substr(h, 2*o+3, 2) || substr(h, 2*o+1, 2))::INTEGER);
CREATE MACRO i16(h, o) AS (CASE WHEN u16(h, o) >= 32768 THEN u16(h, o) - 65536 ELSE u16(h, o) END);
CREATE MACRO u32(h, o) AS (('0x' || substr(h, 2*o+7, 2) || substr(h, 2*o+5, 2) || substr(h, 2*o+3, 2) || substr(h, 2*o+1, 2))::BIGINT);
CREATE TABLE wad AS SELECT hex(content) AS h FROM read_blob('WADPATH');
CREATE TABLE lump AS
  SELECT i, u32(h, ofs + 16*i) AS pos, u32(h, ofs + 16*i + 4) AS len,
         replace(decode(unhex(substr(h, 2*(ofs + 16*i + 8) + 1, 16))), chr(0), '') AS name
  FROM (SELECT h, u32(h, 4) AS n, u32(h, 8) AS ofs FROM wad), range(0, n::BIGINT) t(i);
CREATE TABLE mk AS SELECT i FROM lump WHERE name = 'MAPNAME';
CREATE TABLE v AS SELECT k AS id, i16(h, pos + 4*k) AS x, i16(h, pos + 4*k + 2) AS y
  FROM wad, (SELECT pos, len FROM lump, mk WHERE lump.i = mk.i + 4) s, range(0, (len/4)::BIGINT) t(k);
CREATE TABLE sd AS SELECT k AS id, u16(h, pos + 30*k + 28) AS sector
  FROM wad, (SELECT pos, len FROM lump, mk WHERE lump.i = mk.i + 3) s, range(0, (len/30)::BIGINT) t(k);
CREATE TABLE sec AS SELECT k AS id, i16(h, pos + 26*k) AS floor, i16(h, pos + 26*k + 2) AS ceil, u16(h, pos + 26*k + 24) AS tag
  FROM wad, (SELECT pos, len FROM lump, mk WHERE lump.i = mk.i + 8) s, range(0, (len/26)::BIGINT) t(k);
CREATE TABLE l0 AS SELECT k AS id, u16(h, pos + 14*k) AS v1, u16(h, pos + 14*k + 2) AS v2, u16(h, pos + 14*k + 4) AS flags,
         u16(h, pos + 14*k + 6) AS special, u16(h, pos + 14*k + 10) AS rs, u16(h, pos + 14*k + 12) AS ls
  FROM wad, (SELECT pos, len FROM lump, mk WHERE lump.i = mk.i + 2) s, range(0, (len/14)::BIGINT) t(k);
CREATE TABLE l AS SELECT l0.id, a.x AS x1, a.y AS y1, b.x AS x2, b.y AS y2, l0.flags, l0.special, sr.sector AS rsec, sl.sector AS lsec,
         CASE WHEN l0.special IN (26, 32) THEN 1 WHEN l0.special IN (27, 34) THEN 2 WHEN l0.special IN (28, 33) THEN 3 ELSE 0 END AS lock
  FROM l0 JOIN v a ON a.id = l0.v1 JOIN v b ON b.id = l0.v2 LEFT JOIN sd sr ON sr.id = l0.rs LEFT JOIN sd sl ON sl.id = l0.ls;
-- An edge is passable when the step up is at most 24 and the opening at least
-- 56, or one side is a closed door, or either side is tagged to move.
CREATE TABLE pass AS
  SELECT d.a, d.b, d.mx, d.my, d.lock
  FROM (SELECT rsec AS a, lsec AS b, (x1+x2)/2.0 AS mx, (y1+y2)/2.0 AS my, flags, lock FROM l WHERE lsec IS NOT NULL AND rsec <> lsec
        UNION ALL
        SELECT lsec, rsec, (x1+x2)/2.0, (y1+y2)/2.0, flags, lock FROM l WHERE lsec IS NOT NULL AND rsec <> lsec) d
  JOIN sec sa ON sa.id = d.a JOIN sec sb ON sb.id = d.b
  WHERE (d.flags & 1) = 0
    AND (sa.tag > 0 OR sb.tag > 0
         OR (sb.floor - sa.floor <= 24 AND (least(sa.ceil, sb.ceil) - greatest(sa.floor, sb.floor) >= 56 OR sa.ceil = sa.floor OR sb.ceil = sb.floor)));
CREATE TABLE exitp AS SELECT special, rsec AS sector, (x1+x2)/2.0 AS mx, (y1+y2)/2.0 AS my FROM l WHERE special IN (11, 52) ORDER BY special LIMIT 1;
-- A key card lies in the sector on its side of the nearest line.
CREATE TABLE keyt AS
  SELECT x, y, lock, sector FROM (
    SELECT t.x, t.y, t.lock,
           CASE WHEN (l.x2-l.x1)*(t.y-l.y1) - (l.y2-l.y1)*(t.x-l.x1) < 0 OR l.lsec IS NULL THEN l.rsec ELSE l.lsec END AS sector,
           row_number() OVER (PARTITION BY t.id ORDER BY
             CASE WHEN (t.x-l.x1)*(l.x2-l.x1) + (t.y-l.y1)*(l.y2-l.y1) <= 0 THEN (t.x-l.x1)*(t.x-l.x1) + (t.y-l.y1)*(t.y-l.y1)
                  WHEN (t.x-l.x1)*(l.x2-l.x1) + (t.y-l.y1)*(l.y2-l.y1) >= (l.x2-l.x1)*(l.x2-l.x1) + (l.y2-l.y1)*(l.y2-l.y1) THEN (t.x-l.x2)*(t.x-l.x2) + (t.y-l.y2)*(t.y-l.y2)
                  ELSE pow((l.x2-l.x1)*(t.y-l.y1) - (l.y2-l.y1)*(t.x-l.x1), 2) / ((l.x2-l.x1)*(l.x2-l.x1) + (l.y2-l.y1)*(l.y2-l.y1)) END) AS nearest
    FROM (SELECT k AS id, i16(h, pos + 10*k) AS x, i16(h, pos + 10*k + 2) AS y,
                 CASE u16(h, pos + 10*k + 6) WHEN 5 THEN 1 WHEN 6 THEN 2 WHEN 13 THEN 3 ELSE 0 END AS lock, u16(h, pos + 10*k + 8) AS flags
          FROM wad, (SELECT pos, len FROM lump, mk WHERE lump.i = mk.i + 1) s, range(0, (len/10)::BIGINT) q(k)) t, l
    WHERE t.lock > 0 AND (t.flags & 2) = 2 AND (t.flags & 16) = 0 AND (l.x1 <> l.x2 OR l.y1 <> l.y2))
  WHERE nearest = 1;
CREATE TABLE held (lock INTEGER);
CREATE TABLE visit (cx INTEGER, cy INTEGER);
CREATE TABLE clock (leveltime INTEGER);
CREATE TABLE mapno AS SELECT MAPNUMBER AS n;
DROP TABLE wad;
SELECT (SELECT count(*) FROM pass) edges, (SELECT count(*) FROM pass WHERE lock > 0) locked_edges, (SELECT count(*) FROM keyt) keys,
       (SELECT special || ' in sector ' || sector FROM exitp) exit_line;
