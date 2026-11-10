# Hosting account move assessment

For the account owner moving sites off an InterWorx, cPanel, Plesk or Virtualmin server.

Brief 3a89ee79e17ae9923e5a5fd0, written 2026-10-30T11:08:48Z from 5 source documents. Every quotation below is copied exactly from a document we retained, and each one says where it came from.

## What we read

1. virtualmin/virtualmin-gpl README (master), https://raw.githubusercontent.com/virtualmin/virtualmin-gpl/master/README.md, read 2026-10-28T01:15:14Z (sha256 `c883cadd9139…`)
2. cPanel migration implementation, https://github.com/CpanelInc/cPMigration, read 2026-10-28T01:17:46Z (sha256 `31e3e381119d…`)
3. Community InterWorx API client, https://github.com/jakdept/iworxApi, read 2026-10-28T01:17:51Z (sha256 `223613699426…`)
4. InterWorx CLI documentation, https://appendix.interworx.com/current/getting_started/setup_information/cli-overview.html, read 2026-10-28T01:17:43Z (sha256 `de50db3aa73c…`)
5. Plesk API implementation, https://github.com/plesk/api-php-lib, read 2026-10-28T01:17:49Z (sha256 `7220e82b380d…`)

## What the sources say

From virtualmin/virtualmin-gpl README (master):

- "- Mailboxes with spam and AV scanning using Postfix, Sendmail, or QMail (Postfix recommended)" [1, line 16]
- "- Domain backup and restoration" [1, line 26]
- "- Easy import of cPanel, Plesk, and DirectAdmin domain backups" [1, line 27]
- "- Comprehensive CLI and remote API" [1, line 29]
- "The best way to extend Virtualmin is usually through plugins. Virtualmin plugins are merely Webmin modules, with a few extra files, and some hooks into the Virtualmin API. Webmin module development is documented in the [Webmin Wiki](http://doxfer.webmin.com/Webmin/ModuleDevelopme…" [1, line 75]
- "Virtualmin is mostly built in Perl 5.16+, with the frontend built in JavaScript and HTML5. It is possible to build Virtualmin components or to interact with Virtualmin in other languages, either via the CLI or remote API, or through reproducing the necessary pieces of the Webmin …" [1, line 77]

From cPanel migration implementation:

- "cPMigration is a CLI tool created to streamline the migration process to cPanel" [2, line 501]
- "managed servers.  This tool is designed for migrating accounts from Plesk," [2, line 502]
- "DirectAdmin, and Ensim, and even other cPanel servers to the Destination server" [2, line 503]
- "cPMigration is not officially supported by cPanel, Inc.  It is developed and maintained by cPanel employees,  but is provided for educational purposes only.  Usage of this script is to done so at the operator's discretion." [2, line 506]
- "-a <username or domain>, specify single account" [2, line 529]
- "-i <ip address>,  specify ip address to restore to" [2, line 530]
- "-l <filename>,  Read accounts from list" [2, line 531]
- "-R <destip>, Set up Remote MySQL grants for destination ip (cPanel > cPanel migration only)" [2, line 535]

From Community InterWorx API client:

- "InterWorx API bindings for Golang" [3, line 497]
- "InterWorx NodeWorx and SiteWorx APIs appear to not do any session persistance:" [3, line 506]

From InterWorx CLI documentation:

- "Using the InterWorx CLI Utility" [4, line 161]
- "The InterWorx Control Panel offers a command-line utility, which allows server administrators and SiteWorx" [4, line 183]
- "Overview of the InterWorx CLI Utility" [4, line 211]
- "The CLI Utility is comprised of two commands:" [4, line 212]
- "command is used to complete administrator and reseller functions that" [4, line 215]
- "would normally be performed from the NodeWorx control panel" [4, line 216]
- "command is used to complete SiteWorx user functions on specific SiteWorx accounts that" [4, line 217]
- "would normally be performed from the SiteWorx control panel" [4, line 218]

From Plesk API implementation:

- "PHP library for Plesk XML-RPC API" [5, line 500]
- "PHP object-oriented library for Plesk XML-RPC API." [5, line 504]
- "composer require plesk/api-php-lib" [5, line 507]
- "In case of Plesk extension creation one can use an internal mechanism to access XML-RPC API. It does not require to pass authentication because the extension works in the context of Plesk." [5, line 533]
- "REMOTE_HOST=your-plesk-host.dom REMOTE_PASSWORD=password composer test" [5, line 541]
- "REMOTE_URL=https://your-plesk-host.dom:port REMOTE_PASSWORD=password composer test" [5, line 543]
- "REMOTE_URL=https://your-plesk-host.dom:port REMOTE_PASSWORD=password composer test:watch" [5, line 548]

## What has to be true before the move

- Every site, domain, mailbox and database on the source account is listed before anything moves. Not assessed yet: we have no inventory from you.
- A frozen copy of the source account exists and can be restored without the old host. Not assessed yet: we have no inventory from you.
- DNS records and their current TTLs are recorded, so the cutover can be reversed. Not assessed yet: we have no inventory from you.
- Mail keeps arriving during the move and old mail is copied with folder structure intact. Not assessed yet: we have no inventory from you.

## Proposed range

USD 300 to 900 per site. Proposed range. This is a planning estimate, not a quote, and it has not been calibrated against delivered work.

The range covers reviewer hours per site for inventory, staged copy, mail sync and cutover check.

## What is missing or needs review

- no authorized customer inventory supplied; every criterion stays unassessed

## Next steps we would take

None of these has started. Each one waits for your go-ahead and for access you grant.

1. Read the authorized account inventory
2. Confirm a restorable frozen copy of the source
3. Stage the copy on the destination without cutover
4. Review DNS and mail cutover with the account owner

## What this brief cannot tell you

- A cited passage does not show the source system matches it, or the upstream project supports this customer's case.
- An inventory-stated criterion does not show a verified or accepted criterion; only a reviewer with authorized access can verify it.
- A proposed range does not show a price, a quote, an invoice or permission to spend.
- A proposed task does not show admitted work, provider access, or authority to touch the customer's systems.
- A license mention in a README does not show the license of the code, or rights to adopt or republish it.
