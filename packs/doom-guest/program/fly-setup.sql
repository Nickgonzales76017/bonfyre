-- Build the fly once. Needs tables edge (FlyWire connectivity), ann (neuron
-- annotations) and colmap (optic-lobe column assignment) already loaded from
-- the receipted foreign bodies.
--
-- Wiring: connections of five synapses or more, signed by predicted
-- transmitter, each divided by the receiving neuron's total input.
CREATE OR REPLACE TABLE ws AS
  SELECT e.pre, e.post, e.sw / i.total AS wn
  FROM (SELECT Presynaptic_ID AS pre, Postsynaptic_ID AS post, "Excitatory x Connectivity"::DOUBLE AS sw, Connectivity::DOUBLE AS syn
        FROM edge WHERE Connectivity >= 5) e
  JOIN (SELECT Postsynaptic_ID AS post, sum(Connectivity)::DOUBLE AS total FROM edge WHERE Connectivity >= 5 GROUP BY 1) i USING (post);
-- Eyes: every L1 and L2 sits in a column of the published hex grid. Grid y is
-- dorsal-up (correlation -0.98 with ventral brain position). Grid x runs with
-- the medulla's front-back axis (+0.92 for Mi1); the first optic chiasm inverts
-- that axis, so +x is taken as the frontal field. The screen is laid over both
-- eyes with the frontal edge of each at the screen's centre.
CREATE OR REPLACE TABLE eye AS
  SELECT c.root_id AS id, c.type AS cell_type, c.hemisphere AS side,
         CASE c.hemisphere WHEN 'left' THEN 160.0 * (c.x + 9) / 17.0 ELSE 320.0 - 160.0 * (c.x + 9) / 17.0 END AS sx,
         168.0 * (30 - c.y) / 59.0 AS sy
  FROM colmap c WHERE c.type IN ('L1', 'L2');
-- Each eye cell reads the pixels near its own direction.
CREATE OR REPLACE TABLE tap AS
  SELECT e.id, px.x, px.y
  FROM eye e, (SELECT x, y FROM range(0, 320, 2) tx(x), range(0, 168, 2) ty(y)) px
  WHERE abs(px.x - e.sx) <= 5 AND abs(px.y - e.sy) <= 3;
CREATE OR REPLACE TABLE dn AS
  SELECT root_id AS id, cell_type, side FROM ann
  WHERE cell_type IN ('DNa01', 'DNa02', 'DNp09', 'DNp01', 'MDN') AND side IN ('left', 'right');
CREATE OR REPLACE TABLE readout (mirror INTEGER, steer_left DOUBLE, steer_right DOUBLE, walk DOUBLE, back DOUBLE, fiber DOUBLE, active BIGINT);
CREATE OR REPLACE TABLE fiber_history (v DOUBLE);
SELECT (SELECT count(*) FROM ws) connections, (SELECT count(*) FROM eye) eye_cells, (SELECT count(*) FROM tap) taps, (SELECT count(*) FROM dn) readouts;
