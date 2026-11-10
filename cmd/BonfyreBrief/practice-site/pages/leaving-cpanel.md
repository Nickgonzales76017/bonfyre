# Leaving cPanel/WHM

For owners of a WHM server or reseller account moving cPanel accounts to another host, another cPanel server, or a different panel.

## What we work from

cPanel accounts can be packaged and restored account by account, and WHM can list every account on a server. cPanel's own staff published a migration script, cPMigration, that is explicitly not officially supported, so we treat it as reference material and check each account ourselves.

## What the move covers

Each account's domains, files, databases with their users and grants, mailboxes and forwarders, and scheduled jobs. Remote database grants and PHP versions per account are recorded, because both are easy to lose.

## Before anything moves

A list of accounts from WHM, a backup of each that restores without the old server, and the DNS records for every domain with their current timings.

## Start

[Ask for a hosting move estimate](/contact?about=Hosting%20account%20move) and mention that the server runs cPanel/WHM.
