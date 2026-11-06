# Databases

For teams moving a database to a new engine or a new major version: SQL Server, Oracle, MySQL, PostgreSQL or SQLite, in any direction.

## What moves

Tables and their data, views, stored procedures, triggers, constraints, users and permissions. Scheduled jobs and the reports that read from the database are listed, because they are often what breaks.

## What we check first

- The source and destination engines and versions are named.
- Tables, views, procedures, triggers and permissions are counted on the source.
- A rollback point exists, and the time it takes to restore is known.

## How the result is checked

The conversion is run into a rehearsal copy. Row counts, column types, constraints and the results of a sample of your real queries are compared between the source and the copy. Every difference is listed, with what caused it and what was done.

## Start

[Ask for a database estimate](/contact?about=Database%20and%20ERP%20migration).
