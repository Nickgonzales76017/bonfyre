-- Score every candidate branch. Reads ./state.json and ./candidates.jsonl, the
-- files the runtime's branch verb writes. Goal: the nearest reachable key card
-- not yet held, otherwise the exit. Locked edges count only once their key is held.
-- A door opens only for a guest that faces it, so facing the waypoint is scored:
-- without that term a turn never beats a strafe and the guest waits at the door.
WITH RECURSIVE st AS (SELECT * FROM read_json('state.json')),
c AS (SELECT * FROM read_json('candidates.jsonl', format = 'newline_delimited')),
openp AS (SELECT * FROM pass WHERE lock = 0 OR lock IN (SELECT lock FROM held)),
goals AS (SELECT lock AS g, sector, x::DOUBLE AS gx, y::DOUBLE AS gy FROM keyt WHERE lock NOT IN (SELECT lock FROM held)
          UNION ALL SELECT 0, sector, mx, my FROM exitp),
d(g, sector, n) AS (SELECT g, sector, 0 FROM goals
                    UNION SELECT d.g, p.a, d.n + 1 FROM d JOIN openp p ON p.b = d.sector WHERE d.n < 60),
hop AS (SELECT g, sector, min(n) AS n FROM d GROUP BY g, sector),
goal AS (SELECT g.* FROM goals g LEFT JOIN hop h ON h.g = g.g AND h.sector = (SELECT sector FROM st)
         ORDER BY CASE WHEN g.g <> 0 AND h.n IS NOT NULL THEN 0 WHEN g.g = 0 THEN 1 ELSE 2 END, h.n LIMIT 1),
aimed AS (
  SELECT c.*, coalesce(w.mx, (SELECT gx FROM goal)) AS wx, coalesce(w.my, (SELECT gy FROM goal)) AS wy
  FROM c LEFT JOIN LATERAL (
    SELECT p.mx, p.my FROM openp p LEFT JOIN hop h2 ON h2.g = (SELECT g FROM goal) AND h2.sector = p.b
    WHERE p.a = c.sector AND c.sector <> (SELECT sector FROM goal)
    ORDER BY coalesce(h2.n, 50), pow(p.mx - c.x, 2) + pow(p.my - c.y, 2) LIMIT 1) w ON true),
scored AS (
  SELECT a.i,
    CASE WHEN a.tic IS NULL THEN -1e9
         WHEN a.gs <> 0 OR a.map <> (SELECT n FROM mapno) THEN 1e9
         WHEN a.health <= 0 THEN -1e9
         ELSE a.kills * 20000 - a.mhp * 40 + a.health * 50
              - coalesce((SELECT h.n FROM hop h, goal WHERE h.g = goal.g AND h.sector = a.sector), 50) * 2000
              - sqrt(pow(a.wx - a.x, 2) + pow(a.wy - a.y, 2))
              - 1.5 * abs(((degrees(atan2(a.wy - a.y, a.wx - a.x)) - a.angle + 540) % 360) - 180)
              + CASE WHEN a.enemy >= 0 THEN 500 - abs(a.bearing) * 10 ELSE 0 END
              - 60 * (SELECT count(*) FROM visit WHERE cx = round(a.x / 48.0) AND cy = round(a.y / 48.0))
    END AS v
  FROM aimed a)
SELECT v FROM scored ORDER BY i;
