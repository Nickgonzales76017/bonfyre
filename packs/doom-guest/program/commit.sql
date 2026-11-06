-- Record the committed state: where the guest has been, and any key card it is standing on.
-- A level clock that runs backwards means the level restarted; what was held and visited went with it.
DELETE FROM held WHERE (SELECT leveltime FROM read_json('state.json')) < (SELECT coalesce(max(leveltime), 0) FROM clock);
DELETE FROM visit WHERE (SELECT leveltime FROM read_json('state.json')) < (SELECT coalesce(max(leveltime), 0) FROM clock);
DELETE FROM clock;
INSERT INTO clock SELECT leveltime FROM read_json('state.json');
INSERT INTO visit SELECT round(x / 48.0)::INTEGER, round(y / 48.0)::INTEGER FROM read_json('state.json');
INSERT INTO held SELECT k.lock FROM keyt k, read_json('state.json') s
  WHERE sqrt(pow(k.x - s.x, 2) + pow(k.y - s.y, 2)) < 28 AND k.lock NOT IN (SELECT lock FROM held);
