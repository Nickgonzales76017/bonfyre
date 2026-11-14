-- Design intake: reference designs -> design_fingerprint rows (JSON lines).
-- Source-neutral. Input: a folder tree where each design is a directory holding
-- code.html (and usually screen.png). Run with DuckDB (bonfyre-query sql):
--   SET VARIABLE designs = '<root>/*/*/*/code.html';  SET VARIABLE source = '<source name>';
-- Measures structure and declared tokens only; never imports markup, CSS or
-- scripts. The pattern is a first-pass heuristic (pattern_basis says why) and
-- the row stays an unselected observation until reviewed.
WITH files AS (
  SELECT filename, content,
         regexp_extract(filename, '/([^/]+)/code\.html$', 1) AS design_id,
         regexp_extract(filename, '/([^/]+)/[^/]+/[^/]+/code\.html$', 1) AS archive
  FROM read_text(getvariable('designs'))
  -- one record per design: a design repeated across archives keeps its first archive
  QUALIFY row_number() OVER (PARTITION BY design_id ORDER BY filename) = 1
),
measured AS (
  SELECT *,
    lower(content) AS lc,
    regexp_extract(content, '"background"\s*:\s*"(#[0-9a-fA-F]{3,8})"', 1) AS ground,
    regexp_extract(content, '"surface-container"\s*:\s*"(#[0-9a-fA-F]{3,8})"', 1) AS panel,
    regexp_extract(content, '"on-surface"\s*:\s*"(#[0-9a-fA-F]{3,8})"', 1) AS ink,
    regexp_extract(content, '"on-surface-variant"\s*:\s*"(#[0-9a-fA-F]{3,8})"', 1) AS muted,
    regexp_extract(content, '"primary"\s*:\s*"(#[0-9a-fA-F]{3,8})"', 1) AS accent,
    regexp_extract(content, '"outline"\s*:\s*"(#[0-9a-fA-F]{3,8})"', 1) AS rule,
    regexp_extract(content, '"(?:headline|display)"\s*:\s*\[\s*"([^"]+)"', 1) AS face_display,
    regexp_extract(content, '"body"\s*:\s*\[\s*"([^"]+)"', 1) AS face_body
  FROM files
),
counted AS (
  SELECT *,
    len(regexp_extract_all(lc, '<header')) AS n_header, len(regexp_extract_all(lc, '<nav')) AS n_nav,
    len(regexp_extract_all(lc, '<section')) AS n_section, len(regexp_extract_all(lc, '<article')) AS n_article,
    len(regexp_extract_all(lc, '<h1')) AS n_h1, len(regexp_extract_all(lc, '<h2')) AS n_h2,
    len(regexp_extract_all(lc, '<img')) AS n_img, len(regexp_extract_all(lc, '<form')) AS n_form,
    len(regexp_extract_all(lc, '<input|<textarea|<select')) AS n_field, len(regexp_extract_all(lc, '<button')) AS n_button,
    len(regexp_extract_all(lc, '<table')) AS n_table, len(regexp_extract_all(lc, '<tr')) AS n_row,
    len(regexp_extract_all(lc, '<video|<audio|play_arrow|waveform')) AS n_media,
    len(regexp_extract_all(lc, '\$[0-9]|price|checkout|cart|bid')) AS n_commerce,
    len(regexp_extract_all(lc, 'map|latitude|coordinates|spatial|3d')) AS n_spatial,
    len(regexp_extract_all(lc, 'grid-cols-[3-9]')) AS n_grid
  FROM measured
),
classified AS (
  SELECT *,
    CASE
      WHEN regexp_matches(design_id, 'cinema|audio|listen|watch|podcast|radio|video|screening') OR n_media >= 4 THEN 'media'
      WHEN regexp_matches(design_id, 'market|shop|auction|commerce|price|ledger|cost|finance|offer') OR n_commerce >= 8 THEN 'market'
      WHEN regexp_matches(design_id, 'map|spatial|world|3d|terrain|neighborhood') OR n_spatial >= 6 THEN 'spatial'
      WHEN regexp_matches(design_id, 'connection|connector|protocol|feed_ingress|syndication|integration|api') THEN 'connections'
      WHEN regexp_matches(design_id, 'workbench|studio|editor|console|pipeline|workflow|cms|atelier') OR n_field >= 8 THEN 'workbench'
      WHEN regexp_matches(design_id, 'profile|identity|member') THEN 'profile'
      WHEN regexp_matches(design_id, 'organization|directory|registry|roles|people|guild') OR n_table >= 1 OR n_grid >= 2 THEN 'directory'
      WHEN regexp_matches(design_id, 'detail|item') THEN 'detail'
      WHEN n_article >= 3 OR regexp_matches(design_id, 'feed|news|journal|publisher|reader|article') THEN 'feed'
      ELSE 'discover'
    END AS pattern,
    CASE WHEN regexp_matches(design_id, 'cinema|audio|listen|watch|podcast|radio|video|screening|market|shop|auction|commerce|price|ledger|cost|finance|offer|map|spatial|world|3d|terrain|neighborhood|connection|connector|protocol|feed_ingress|syndication|integration|api|workbench|studio|editor|console|pipeline|workflow|cms|atelier|profile|identity|member|organization|directory|registry|roles|people|guild|detail|item|feed|news|journal|publisher|reader|article')
         THEN 'name' ELSE 'structure' END AS pattern_basis
  FROM counted
)
SELECT to_json({
  fingerprint_id: 'reference:' || getvariable('source') || ':' || design_id,
  template: design_id,
  reference_source: getvariable('source'),
  reference_name: replace(design_id, '_', ' '),
  family: 'observed-reference',
  renderer_affinity: pattern,
  density: CASE WHEN n_section + n_article > 12 THEN 'tight' WHEN n_section + n_article < 4 THEN 'airy' ELSE 'standard' END,
  visual_vector: {
    archive: archive, pattern: pattern, pattern_basis: pattern_basis,
    tokens: {ground: ground, panel: panel, ink: ink, muted: muted, accent: accent, rule: rule,
             face_display: face_display, face_body: face_body},
    anatomy: {header: n_header, nav: n_nav, section: n_section, article: n_article, h1: n_h1, h2: n_h2,
              img: n_img, form: n_form, field: n_field, button: n_button, tables: n_table, rows: n_row,
              media: n_media, commerce: n_commerce, spatial: n_spatial, wide_grid: n_grid},
    html_sha256: sha256(content)
  },
  acceptance_contract: 'Structure and declared tokens measured from the reference code.html; screen.png is the visual target. Selection requires review against the live route in the preview.',
  cannot_infer: 'a measured reference design -> copied markup, a reviewed match, a selected route presentation, semantic truth or an authority to act; the pattern is a heuristic',
  status: 'observed_reference'
}) AS row
FROM classified
ORDER BY design_id;
