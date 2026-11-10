---
slug: the-deployment-was-green-but-the-site-was-old
title: The deployment passed. Why was the website still old?
description: What four public domains and GitHub Pages taught us about deployment destinations and browser readback.
category: Engineering
icon: network
published: 2026-11-07
status: edited-public
topics: deployment, Pages, public web
origin: Four-apex rollout, GitHub Pages and Cloudflare discussions, October–November 2026
---

We ran into a frustrating pattern in the Aurekai release work. New source existed, tests passed and a deployment tool reported success, but the public page still showed older content.

Sometimes the pipeline built a different branch. Sometimes the artifact reached a different origin. Other times the new page existed but its links still opened the old application.

## Every release has a destination

Aurekai has Work, Life, Live and Shop, plus independent customer and personal sites. They share software but each has its own public address and release state.

During the GitHub Pages cleanup we found a mismatch between the master and gh-pages branches of the personal site. Updating the default branch didn't necessarily update the page people could reach. We aligned the branches and checked the actual publishing path.

## Look at the result in the browser

Bonfÿre's release work covers staged activation, dependencies, source revisions and destination readback. A browser that can open the workspace, use a control and inspect the result catches more than a screenshot.

A broken form, the wrong account in a link or an old data feed can survive a perfectly green build. The [GitHub Pages workflow](https://github.com/Nickgonzales76017/bonfyre/blob/main/.github/workflows/pages.yml) includes public interaction checks for that reason.

The experience a person can actually reach is the product we shipped.
