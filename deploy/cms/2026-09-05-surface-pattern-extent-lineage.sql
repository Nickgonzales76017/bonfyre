-- Section 68: SurfacePatterns are compiled views, not applications.
--
-- A pattern declares four things. Three of them were already columns on
-- surface_pattern:
--
--     the owner it reads      reads_type / reads_organ
--     the action layer        effect_class
--     the authority it needs  requires_authority
--
-- The fourth was missing, and it is the one that governs the other two:
--
--     the extent              page | workspace | section | card | chip
--
-- "Extent decides the action layer. Only a page or a workspace carries one; a
-- chip, card or section is a reference to a surface, and there is no room to
-- act inside a reference. A card offering BOOK asks for a decision from a
-- thumbnail."
--
-- And lineage, which is what makes "only the look was borrowed" a checkable
-- claim rather than a slogan: which project, which files, which commit.
--
-- BonfyreWebAddress already reads both columns and falls back when they are
-- absent, so applying this is safe in either order. Until it is applied every
-- authored pattern reports an empty extent, and the organ treats an unset
-- extent as permissive -- an unstated extent is not the same as a card, and
-- silently demoting every existing pattern to "no action layer" would take the
-- estate's own surfaces down for a migration.
--
-- Apply with:
--     sudo sqlite3 /var/lib/bonfyre/cms/bonfyre_cms.db < this-file
--     sudo systemctl restart bonfyre-web-address

ALTER TABLE surface_pattern ADD COLUMN extent TEXT;
ALTER TABLE surface_pattern ADD COLUMN lineage TEXT;

-- Every pattern this deployment currently serves is read at page extent: each
-- one answers a whole /_surface/<name> request and carries its own action
-- layer. Saying so explicitly is what lets a later card-extent pattern be
-- refused an action layer instead of inheriting one from an empty column.
UPDATE surface_pattern SET extent = 'page' WHERE extent IS NULL OR extent = '';

-- A pattern reads exactly one owner. This does not enforce it -- SQLite cannot
-- express the constraint against a column added later without rewriting the
-- table -- but it makes a violation visible to anyone who looks:
--
--     SELECT pattern FROM surface_pattern
--     WHERE COALESCE(reads_type,'') <> '' AND COALESCE(reads_organ,'') <> '';
--
-- should always return nothing. A pattern reading both would be two patterns
-- sharing a name, whose human and machine projections could diverge with
-- nothing noticing.
