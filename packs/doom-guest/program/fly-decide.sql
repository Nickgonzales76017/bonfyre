-- Turn the two waves in table readout (the scene and its mirror image) into
-- keys. A drive that appears on the left for the scene and on the right for its
-- mirror is about the scene; a drive that stays on one side for both is the
-- wiring's own asymmetry, and averaging the two cancels it. Steering is toward
-- the stronger side, for a time proportional to the imbalance. The giant fiber
-- fires when it stands at twice the median of its own history.
CREATE OR REPLACE TEMP TABLE d AS
  SELECT (n.steer_left + m.steer_right) / 2 AS l, (n.steer_right + m.steer_left) / 2 AS r,
         (n.walk + m.walk) / 2 AS walk, (n.back + m.back) / 2 AS back, (n.fiber + m.fiber) / 2 AS fiber, n.active
  FROM readout n, readout m WHERE n.mirror = 0 AND m.mirror = 1;
CREATE OR REPLACE TEMP TABLE o AS
  SELECT round((l - r) / (l + r + 1e-12), 4) AS steer, walk, back, fiber, active,
         (SELECT count(*) FROM fiber_history) >= 5 AND fiber > 2 * (SELECT median(v) FROM fiber_history) AS fire
  FROM d;
INSERT INTO fiber_history SELECT fiber FROM d;
DELETE FROM readout;
SELECT steer, walk, back, fiber, active, fire,
       CASE WHEN fire THEN 'd fire;t 3;u fire;t 5;' ELSE '' END
       || CASE WHEN steer > 0.02 THEN 'd left;t ' || least(12, greatest(2, round(abs(steer) * 40)))::INTEGER || ';u left;t 1;'
               WHEN steer < -0.02 THEN 'd right;t ' || least(12, greatest(2, round(abs(steer) * 40)))::INTEGER || ';u right;t 1;'
               ELSE '' END
       || CASE WHEN back > walk THEN 'd down;t 10;u down;t 1' ELSE 'd up;t 12;u up;t 1' END AS macro
FROM o;
