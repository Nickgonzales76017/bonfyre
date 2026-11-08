---
slug: frappe-as-an-application-language
title: What we learned from Frappe without copying its whole runtime
description: The useful idea behind DocTypes, and why native application composition needs more than importing tables.
category: Engineering
icon: database
published: 2026-11-07
status: edited-public
topics: Frappe, ERPNext, DocTypes, native software
origin: Frappe-native compiler and schema discussions, June–November 2026
---

A CRM, helpdesk or accounting application contains far more domain knowledge than its database tables. There are fields, relationships, roles, forms, workflows, validation rules, reports and expectations about how people use the system.

That is what made Frappe interesting to us. The [Frappe Framework describes a DocType](https://frappe.io/framework/doctype) as a model that also carries behavior and user-interface metadata. ERPNext and the other Frappe applications have accumulated a considerable amount of practical business vocabulary through those structures.

## Keep the business grammar

Our Bonfÿre work includes compiling Frappe-derived schemas into native application representations. The installed development inventory recorded 1,840 DocTypes across 51 modules in November 2026, covering the foundations of ERPNext, CRM, HRMS, Helpdesk, LMS, Wiki, Drive and Insights.

A DocType helps answer questions a generic database cannot. What is a Sales Invoice? Which fields belong to a Support Ticket? How does a team relate a contact to a company? Which record is allowed in a particular workspace?

The goal is to use that accumulated knowledge when building native applications, not to require every Aurekai product to run an entire second application server.

## Controllers are a separate question

Importing a schema does not safely import arbitrary Python controller behavior, migrations or permission systems. The native representation needs to know where its own authority, validation and work execution come from. An application can then present a familiar field or workflow while continuing through Bonfÿre's established owners.

This also changes what a frontend means. A CRM interface isn't merely a collection of tables. It needs the right record relationships, actions and context. The [Bonfÿre technical guide](https://nickgonzales76017.github.io/bonfyre/docs.html) describes the Matrix and application sides of this work.

Frappe offered a useful lesson: the best part of a mature application platform may be the knowledge it has encoded about the work. The runtime that serves that knowledge can continue to evolve.
