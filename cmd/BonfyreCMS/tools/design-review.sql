-- Design review queue: where each measured reference design lands in the system.
-- Reads the CMS read-only. For one source, every design_fingerprint row gets:
--   candidate routes  = published route_presentation rows wearing the same pattern
--   token match       = whether its declared ground/accent equal the template those
--                       routes already wear (so it needs no new presentation_template)
-- Run with DuckDB:
--   SET VARIABLE source = '<source>'; SET VARIABLE tenant_namespace = '<exact CMS owner>'; replace __CMS_DB__ with the CMS path
--   (DuckDB ATTACH takes a literal path, not a variable).
-- Output is TSV for a reviewer; it selects nothing and changes no row.
INSTALL sqlite; LOAD sqlite;
ATTACH '__CMS_DB__' AS cms (TYPE sqlite, READ_ONLY);
WITH designs AS (
  SELECT fingerprint_id, template AS design_id, renderer_affinity AS pattern, status,
         json_extract_string(visual_vector, '$.pattern_basis') AS pattern_basis,
         lower(json_extract_string(visual_vector, '$.tokens.ground')) AS ground,
         lower(json_extract_string(visual_vector, '$.tokens.accent')) AS accent,
         json_extract_string(visual_vector, '$.archive') AS archive
  FROM cms.design_fingerprint WHERE namespace = getvariable('tenant_namespace') AND reference_source = getvariable('source')
),
routes AS (
  SELECT pattern, list(territory || ':' || route ORDER BY territory, route) AS candidate_routes,
         any_value(template) AS template
  FROM cms.route_presentation WHERE namespace = getvariable('tenant_namespace') AND status = 'published' GROUP BY pattern
),
templates AS (
  SELECT template, lower(json_extract_string(layout_tokens, '$.ground')) AS ground,
         lower(json_extract_string(layout_tokens, '$.accent')) AS accent
  FROM cms.presentation_template WHERE namespace = getvariable('tenant_namespace') AND status = 'published'
)
SELECT d.design_id, d.status, d.pattern, d.pattern_basis, d.archive,
       coalesce(len(r.candidate_routes), 0) AS route_count,
       array_to_string(r.candidate_routes[1:6], ' ') AS first_routes,
       coalesce(r.template, '') AS wears_template,
       CASE WHEN d.ground IS NULL THEN 'no tokens declared'
            WHEN t.ground = d.ground AND t.accent = d.accent THEN 'matches template'
            ELSE 'differs: ' || coalesce(d.ground, '?') || ' / ' || coalesce(d.accent, '?') END AS tokens
FROM designs d
LEFT JOIN routes r ON r.pattern = d.pattern
LEFT JOIN templates t ON t.template = r.template
ORDER BY d.status = 'observed_reference' DESC, route_count = 0 DESC, d.pattern, d.design_id;
