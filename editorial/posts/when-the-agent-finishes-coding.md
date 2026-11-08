---
slug: when-the-agent-finishes-coding
title: The agent finished coding. The project isn't finished.
description: What software delivery still needs after agents can write a feature in minutes.
category: Engineering
icon: briefcase
published: 2026-11-07
status: edited-public
topics: agents, workgraph, delivery
origin: Agent orchestration and WorkGraph discussions, August–November 2026
---

An agent can generate a feature, write tests and open a pull request while a team is still discussing the project. That is real progress. It also exposes everything around the application that code generation never solved.

Who owns the account? Where does its data live? What will it cost to run? Which organization may approve a change? How does the result arrive in the applications people already use?

## The work extends beyond a repository

A product launch may include a database change, customer records, a website deployment and a support process. Those actions have different owners. A git commit cannot stand in for a customer accepting a deliverable, and passing tests cannot tell you whether an outside service received the change.

That is why we built Bonfÿre's WorkGraph around tasks, dependencies and application destinations. CapabilityClosure helps find which implementation can perform each step: a native tool, model, installed application or external service.

## Agents contribute, applications decide

Codex, Claude Code and local models can research, implement and critique. The account applications still decide what can happen to their records and money. A person can make a business decision without spending an afternoon copying status between disconnected systems.

The next step for agents is participation in real organizations. The [WorkGraph technical guide](https://nickgonzales76017.github.io/bonfyre/docs.html#operators) and [Aurekai products](https://nickgonzales76017.github.io/bonfyre/explore.html) show the two sides of that work.
