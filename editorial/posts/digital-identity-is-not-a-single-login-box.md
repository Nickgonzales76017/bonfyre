---
slug: digital-identity-is-not-a-single-login-box
title: One login doesn't mean one identity across every business
description: What NIST federation guidance teaches about account boundaries and separate relying parties.
category: Standards
icon: people
published: 2026-11-08
status: edited-public
topics: NIST, identity, federation, accounts
origin: External standards, primary documentation and Aurekai integration research
source_url: https://www.nist.gov/publications/nist-sp-800-63-4-digital-identity-guidelines
---

A common login screen is convenient. A publisher, an independent retailer and a software agency may all want customers to sign in without creating another password. The challenge is that sharing an authentication mechanism does not mean the businesses should share their underlying customer records or authority.

The [NIST SP 800-63-4 guidelines](https://www.nist.gov/publications/nist-sp-800-63-4-digital-identity-guidelines) describe identity proofing, authenticators and federation as distinct areas. Its [federation section](https://csrc.nist.gov/pubs/sp/800/63/C/4/final) discusses how separate relying parties can accept assertions from an identity provider.

## Authentication is a statement with a context

An identity provider might assert that a person successfully authenticated. A retailer still has to decide whether that person may view an order. A company may grant them permission to approve an invoice but not to export payroll data.

The same human can appear as a buyer, employee, contributor and agency administrator. Those are relationships to different organizations, not one universal privilege.

## The design consequence

Bonfÿre's Matrix identity work keeps person and organization relationships available across applications. Account destinations and application authorities decide which records and actions belong to a particular context.

That allows a personal profile and a business workspace to refer to the same person without granting the personal profile the powers of a finance officer.

A well-designed federation system reduces repeated login friction. It should also make it easy to explain which account is acting, whose data will be changed and why the action is permitted.

The most useful identity system is one that helps people move between contexts without erasing their boundaries.
