# Hosting accounts

For whoever owns a server or reseller account on InterWorx, cPanel/WHM, Plesk or Virtualmin and needs the sites, mail and databases moved somewhere else.

## What moves

Sites and their files, domains and DNS records, mailboxes with their folders, databases and their users, scheduled jobs, and certificates. An account with many client sites is treated as a list of small moves, each checked on its own.

## What we check first

- Every site, domain, mailbox and database is listed before anything moves.
- A frozen copy of the old account exists and can be restored without the old host.
- DNS records and their current timings are written down, so the switch can be reversed.
- Mail keeps arriving during the move, and old mail arrives with its folders intact.

## Where it usually goes wrong

Mail sent during the switch that lands on the old server and is never copied. Databases with users or grants that were set by hand. PHP versions that differ between the old and new server, so a site that looked fine stops working a week later.

## Start

Read [how a move runs](/how-a-move-runs), then [ask for a hosting move estimate](/contact?about=Hosting%20account%20move). If your panel is InterWorx, cPanel or Plesk, there is a page for each under "Leaving a hosting panel".
