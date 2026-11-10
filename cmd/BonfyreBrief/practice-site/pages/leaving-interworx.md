# Leaving InterWorx

For server owners and resellers moving SiteWorx accounts off an InterWorx server.

## What we work from

InterWorx splits its work between NodeWorx, for the server and reseller side, and SiteWorx, for each hosted account. Its own documentation describes a command-line utility with one command for each side, which lets an inventory be read account by account instead of clicked through screen by screen.

## What the move covers

Each SiteWorx account becomes its own small move: its domains, files, databases, mailboxes and scheduled jobs. Accounts are staged on the new server and checked one at a time, so a problem with one site does not hold up the rest.

## Before anything moves

You give us read access to NodeWorx, or an export from it. We list every account and what it holds, confirm a backup that restores without the old server, and write down the DNS records for each domain.

## Start

[Ask for a hosting move estimate](/contact?about=Hosting%20account%20move) and mention that the server runs InterWorx.
