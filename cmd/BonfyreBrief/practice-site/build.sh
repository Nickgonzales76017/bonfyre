#!/bin/sh
# The legacy migration practice as a draft site, made only through the site
# product's own verbs: pages, offers with intake questions, a presentation.
# Prices are read from the migration brief types, so the site and the
# assessment briefs cannot quote different ranges.  Nothing here publishes:
# the site lives in HOME and answers on loopback until its owner shares it.
#
#   build.sh WEB_BINARY HOME [PORT] [NAME]
set -eu
web=${1:?web-address binary required}
home=${2:?site home directory required}
port=${3:-8787}
name=${4:-migrations}
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
types="$here/../data/migration-brief-types.json"
pages="$here/pages"
[ -r "$types" ] || { echo "practice site: brief types not found at $types" >&2; exit 2; }
site() { "$web" site "$@" --home "$home" --port "$port"; }
quiet() { site "$@" >/dev/null || { echo "practice site: site $* failed" >&2; exit 1; }; }

# "Estimated $300 to $900 per site": a range is never an amount, so the
# words do not begin with a figure the offer would read as the price.
range() {
  sqlite3 :memory: "SELECT printf('Estimated \$%,d to \$%,d per %s', json_extract(t.value,'\$.estimate.low'),
      json_extract(t.value,'\$.estimate.high'), json_extract(t.value,'\$.estimate.unit'))
    FROM json_each(readfile('$types'),'\$.types') t WHERE json_extract(t.value,'\$.id')='$1'"
}
for id in hosting-account-acquisition php-rescue sql-erp-migration cad-ifc civic-legacy-modernization verification-rollback; do
  [ -n "$(range "$id")" ] || { echo "practice site: no estimate for brief type $id" >&2; exit 2; }
done

# The site product refuses the estate's name on an operator's site, so the
# practice carries a working name until its owner chooses one.
quiet new "$name" --title 'Legacy Migration Practice' \
  --tagline 'Old hosting, websites, databases and records, moved to systems you can maintain and checked for anything lost.' \
  --accent '#b93a0b' --no-open
quiet set "$name" --presentation "$here/presentation.json"
quiet set "$name" --footnote 'Draft for review. Not published. Every price here is a planning estimate, not a quote.'
# The first post a new site carries is the product's own greeting; the
# practice's own note takes its place, so the front page is never empty.
first=$(sqlite3 -readonly "$home/cms.db" "SELECT id FROM community_post WHERE community='$name' ORDER BY id LIMIT 1")
quiet post "$name" --title 'What to have ready before we talk' --text "$(cat "$pages/before-we-talk.txt")"
[ -z "$first" ] || quiet post "$name" --withdraw "$first"

page() { quiet page "$name" --no-open "$@"; }
page --title 'How a move runs' --summary 'Eight steps, in the same order for every system.' --group 'The work' --file "$pages/how-a-move-runs.md"
page --title 'What you receive' --summary 'The documents each step leaves with you.' --group 'The work' --file "$pages/what-you-receive.md"
page --title 'Sample assessment brief' --summary 'An assessment written for a hosting move, before any access was granted.' --group 'The work' --file "$pages/sample-hosting-brief.md"
page --title 'What it costs' --route /offers --summary 'Planning estimates by kind of move, and how they become a quote.' --group 'The work' --file "$pages/what-it-costs.md"
page --title 'Hosting accounts' --summary 'Sites, mail and databases off InterWorx, cPanel/WHM, Plesk or Virtualmin.' --group 'Practices' --file "$pages/hosting.md"
page --title 'PHP and WordPress sites' --summary 'Old PHP versions and neglected plugins, fixed on a copy first.' --group 'Practices' --file "$pages/php.md"
page --title 'Databases' --summary 'A new engine or version, with every difference listed.' --group 'Practices' --file "$pages/databases.md"
page --title 'ERP and business systems' --summary 'Odoo, Frappe/ERPNext and older business applications.' --group 'Practices' --file "$pages/erp.md"
page --title 'Public records and case systems' --summary 'Portals, forms, cases and rules for agencies and nonprofits.' --group 'Practices' --file "$pages/public-records.md"
page --title 'CAD, BIM and design files' --summary 'Drawings and building models between tools and versions.' --group 'Practices' --file "$pages/design-files.md"
page --title 'Documents and archives' --summary 'Libraries, shared drives and records archives.' --group 'Practices' --file "$pages/documents.md"
page --title 'Custom legacy applications' --summary 'Software built years ago by someone who has moved on.' --group 'Practices' --file "$pages/custom-apps.md"
page --title 'Leaving InterWorx' --summary 'SiteWorx accounts moved one at a time.' --group 'Leaving a hosting panel' --file "$pages/leaving-interworx.md"
page --title 'Leaving cPanel/WHM' --route /leaving-cpanel --summary 'cPanel accounts with their grants and mail intact.' --group 'Leaving a hosting panel' --file "$pages/leaving-cpanel.md"
page --title 'Leaving Plesk' --summary 'Subscriptions and domains, staged before DNS changes.' --group 'Leaving a hosting panel' --file "$pages/leaving-plesk.md"
page --title 'For agencies hosting client sites' --route /agencies --summary 'A few dozen client sites, sorted and moved in groups.' --group 'Leaving a hosting panel' --file "$pages/agencies.md"

# What the practice sells.  Each --ask names the fact its brief type needs, so
# the answers a person gives line up with the assessment that follows.
offer() { quiet offer "$name" "$@"; }
offer --title 'Hosting account move' --price "$(range hosting-account-acquisition)" \
  --text 'Inventory, a staged copy on the new server, mail copied before and after the switch, and a checked cutover for each site.' \
  --ask 'source_panel=Which control panel runs the server today (InterWorx, cPanel/WHM, Plesk, other)?' \
  --ask 'site_count=About how many sites are on it?' \
  --ask 'domain_list=Which domains are on it? List as many as you know.' \
  --ask 'mailbox_count=About how many mailboxes?' \
  --ask 'database_count=About how many databases?' \
  --ask 'dns_provider=Who looks after your DNS?' \
  --ask 'backup_location=Where are backups kept today, if anywhere?'
offer --title 'PHP and WordPress rescue' --price "$(range php-rescue)" \
  --text 'A compatibility check, fixes made on a copy, and a rehearsal at the new PHP version before the live site changes.' \
  --ask 'site_count=How many sites need this?' \
  --ask 'php_version=Which PHP version do they run now?' \
  --ask 'target_php_version=Which version do you want to reach, if you know?' \
  --ask 'plugin_list=Which plugins or frameworks matter most?'
offer --title 'Database and ERP migration' --price "$(range sql-erp-migration)" \
  --text 'Object mapping, a rehearsal conversion, and a side-by-side comparison of rows, types and query results for each database or ERP install.' \
  --ask 'source_engine=What runs today (engine or ERP, with version)?' \
  --ask 'destination_engine=What are you moving to?' \
  --ask 'database_count=How many databases or installs?' \
  --ask 'object_counts=Roughly how many tables, procedures or custom record types?' \
  --ask 'backup_location=Is there a recent backup you could restore from?'
offer --title 'Public records and case system modernization' --price "$(range civic-legacy-modernization)" \
  --text 'An inventory of datasets, forms and rules, personal data marked, and a plan to run the new system beside the old one before switching.' \
  --ask 'system_inventory=What does the current system hold (datasets, forms, cases, rules)?' \
  --ask 'personal_data_classes=What kinds of personal data are in it?' \
  --ask 'public_endpoints=What can the public read or download from it today?' \
  --ask 'system_count=How many systems are involved?'
offer --title 'Design and building model move' --price "$(range cad-ifc)" \
  --text 'A format check, a sample conversion, and a comparison of geometry and properties before the full set moves.' \
  --ask 'source_formats=Which formats and versions are the files in now?' \
  --ask 'model_set_count=How many model sets or projects?' \
  --ask 'editability_requirements=What has to stay editable after the move?'
offer --title 'Verification and rollback plan' --price "$(range verification-rollback)" \
  --text 'For a move someone else is running: a capture of the system before the move, one timed restore from backup, and a before-and-after comparison for sign-off.' \
  --ask 'capture_scope=What system or site is moving?' \
  --ask 'restore_drill_minutes=Has anyone restored it from backup before, and how long did it take?' \
  --ask 'sign_off_owner=Who will sign off the move on your side?'
offer --title 'Document and archive migration' --price 'Priced after the assessment' \
  --text 'File counts and fingerprints before and after the move, metadata and access rights carried over, and retention rules written down first.' \
  --ask 'archive_system=What holds the documents today?' \
  --ask 'approximate_volume=Roughly how many files or how much storage?' \
  --ask 'retention_rules=Are there rules about how long things must be kept?'
offer --title 'Custom application assessment' --price 'Priced after the assessment' \
  --text 'A reading of what the application does and how it is built, and a plan for replacing or moving it with its behaviour checked side by side.' \
  --ask 'language_stack=What is it built with, if you know?' \
  --ask 'source_access=Do you have the source code?' \
  --ask 'daily_use=What do people use it for each day?'
echo "{\"site\":\"$name\",\"home\":\"$home\",\"port\":$port,\"published\":false}"
