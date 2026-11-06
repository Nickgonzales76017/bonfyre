-- Native analytical realization of the donor's RICE operation, not its whole PM skill.
-- Origin: alirezarezvani/claude-skills @ 19392f7a08264ed00486a251f5b2098321771f94
-- product-team/skills/product-manager-toolkit/scripts/rice_prioritizer.py
-- MIT, Copyright (c) 2025 Alireza Rezvani. Preserve the upstream LICENSE alongside reuse.
-- Inputs: bf_priority_input(ordinal BIGINT,id VARCHAR,name VARCHAR,reach VARCHAR,
--         impact VARCHAR,confidence VARCHAR,effort VARCHAR)
--         bf_priority_settings(capacity VARCHAR), exactly one row.
-- No network, models, persistent writes, commitments or publication.
-- Deliberate native changes: malformed rows are held instead of default-scored;
-- rounding is integer nearest/even; oversize items require splitting, not overbooking.
-- Reach is an estimate per source period. Summed reach is not distinct users.
CREATE TEMP TABLE bf_priority_capacity AS
SELECT CASE WHEN count(*) <> 1 THEN error('one capacity row required')
 WHEN bool_and(regexp_full_match(coalesce(capacity,''),'[0-9]{1,6}')
  AND try_cast(capacity AS BIGINT) BETWEEN 1 AND 100000)
 THEN max(cast(capacity AS BIGINT)) ELSE error('invalid capacity') END AS capacity
FROM bf_priority_settings;
CREATE TEMP TABLE bf_priority_bound AS
SELECT CASE WHEN count(*) > 10000 THEN error('priority row bound exceeded')
 ELSE count(*) END AS input_rows FROM bf_priority_input;
CREATE TEMP MACRO bf_priority_round_even(n,d) AS
 (n // d + CASE WHEN 2*(n%d)>d OR (2*(n%d)=d AND (n//d)%2=1) THEN 1 ELSE 0 END);
CREATE TEMP TABLE bf_priority_rows AS
WITH typed AS (
 SELECT *, lower(impact) AS impact_key,lower(confidence) AS confidence_key,
 lower(effort) AS effort_key,try_cast(reach AS HUGEINT) AS reach_n,
 count(*) OVER(PARTITION BY id) AS id_count,
 count(*) OVER(PARTITION BY ordinal) AS ordinal_count
 FROM bf_priority_input
), mapped AS (
 SELECT t.*,im.iq,cm.cp,em.months,
 concat_ws('; ',
  CASE WHEN ordinal IS NULL OR ordinal<1 OR ordinal_count<>1 THEN 'invalid or duplicate ordinal' END,
  CASE WHEN id IS NULL OR trim(id)='' OR length(id)>256 OR id_count<>1 THEN 'invalid or duplicate id' END,
  CASE WHEN name IS NULL OR trim(name)='' OR length(name)>1024 THEN 'invalid name' END,
  CASE WHEN NOT regexp_full_match(coalesce(reach,''),'[0-9]{1,13}')
    OR reach_n IS NULL OR reach_n<0 OR reach_n>1000000000000 THEN 'invalid reach' END,
  CASE WHEN im.iq IS NULL THEN 'unknown impact' END,
  CASE WHEN cm.cp IS NULL THEN 'unknown confidence' END,
  CASE WHEN em.months IS NULL THEN 'unknown effort' END) AS hold_reason
 FROM typed t
 LEFT JOIN (VALUES ('massive',12),('high',8),('medium',4),('low',2),('minimal',1)) im(k,iq) ON t.impact_key=im.k
 LEFT JOIN (VALUES ('high',100),('medium',80),('low',50)) cm(k,cp) ON t.confidence_key=cm.k
 LEFT JOIN (VALUES ('xl',13),('l',8),('m',5),('s',3),('xs',1)) em(k,months) ON t.effort_key=em.k
)
SELECT m.*,c.capacity,
 CASE WHEN hold_reason='' THEN cast(bf_priority_round_even(reach_n*iq*cp,4*months) AS BIGINT) END AS score_cents,
 CASE WHEN hold_reason<>'' THEN 'held'
  WHEN months>c.capacity THEN 'requires_split' ELSE 'candidate' END AS schedule_state
FROM mapped m CROSS JOIN bf_priority_capacity c CROSS JOIN bf_priority_bound b;
CREATE TEMP VIEW bf_priority_ranking AS
SELECT *,CASE WHEN hold_reason='' THEN
 row_number() OVER(ORDER BY score_cents DESC NULLS LAST,ordinal,id) END AS score_rank,
 CASE WHEN hold_reason='' THEN printf('%d.%02d',score_cents//100,score_cents%100) END AS score_display
FROM bf_priority_rows;
CREATE TEMP VIEW bf_priority_roadmap AS
WITH RECURSIVE eligible AS (
 SELECT *,row_number() OVER(ORDER BY score_cents DESC,ordinal,id) AS position
 FROM bf_priority_rows WHERE schedule_state='candidate'
), allocation(position,quarter_no,used) AS (
 SELECT position,cast(1 AS BIGINT),cast(months AS BIGINT) FROM eligible WHERE position=1
 UNION ALL
 SELECT e.position,CASE WHEN a.used+e.months<=e.capacity THEN a.quarter_no ELSE a.quarter_no+1 END,
 CASE WHEN a.used+e.months<=e.capacity THEN a.used+e.months ELSE cast(e.months AS BIGINT) END
 FROM allocation a JOIN eligible e ON e.position=a.position+1
)
SELECT r.*,a.quarter_no,a.used AS quarter_used_after,
 CASE WHEN a.quarter_no IS NOT NULL THEN r.capacity-a.used END AS capacity_remaining_after
FROM bf_priority_ranking r LEFT JOIN eligible e ON r.ordinal=e.ordinal
 LEFT JOIN allocation a ON e.position=a.position;
CREATE TEMP VIEW bf_priority_portfolio AS
SELECT count(*) AS input_rows,count(*) FILTER(WHERE hold_reason='') AS valid_rows,
 count(*) FILTER(WHERE hold_reason<>'') AS held_rows,
 count(*) FILTER(WHERE schedule_state='requires_split') AS requires_split,
 cast(sum(months) FILTER(WHERE hold_reason='') AS BIGINT) AS estimated_effort_months,
 cast(sum(reach_n) FILTER(WHERE hold_reason='') AS VARCHAR) AS reach_sum_not_unique_users,
 count(*) FILTER(WHERE hold_reason='' AND impact_key IN ('massive','high') AND effort_key IN ('xs','s')) AS quick_wins,
 count(*) FILTER(WHERE hold_reason='' AND impact_key IN ('massive','high') AND effort_key IN ('l','xl')) AS big_bets,
 cast(CASE WHEN count(*) FILTER(WHERE hold_reason='')>0 THEN
 bf_priority_round_even(cast(sum(score_cents) AS HUGEINT),count(*) FILTER(WHERE hold_reason='')) END AS BIGINT) AS average_score_cents
FROM bf_priority_rows;
CREATE TEMP VIEW bf_priority_sensitivity AS
SELECT r.ordinal,r.id,v.scenario,
 cast(bf_priority_round_even(reach_n*iq*cp*v.reach_num,4*months*v.reach_den*v.effort_factor) AS BIGINT) AS score_cents
FROM bf_priority_rows r CROSS JOIN (VALUES ('baseline',1,1,1),('half_reach',1,2,1),('double_reach',2,1,1),('double_effort',1,1,2)) v(scenario,reach_num,reach_den,effort_factor)
WHERE hold_reason='';
