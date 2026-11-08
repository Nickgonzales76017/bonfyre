---
slug: the-machine-told-me-so
title: The machine told me so
description: Milgram's obedience experiments illuminate a modern failure: people and systems can correctly perform a task whose purpose is wrong.
category: Research
icon: network
published: 2026-11-08
status: edited-public
topics: Milgram, AI authority, verification, purpose, oversight
origin: Aurekai editorial series on verification, accessible creation and living digital institutions; original sources linked
source_url: https://pubmed.ncbi.nlm.nih.gov/14049516/
---

In 1963, psychologist Stanley Milgram published the results of an experiment in which ordinary participants, following the directions of an authority figure, were willing to administer what they believed were dangerous electric shocks to another person. The apparatus was staged; the apparent recipient was a confederate. But the participant's dilemma was real, and the study raised difficult questions about obedience and responsibility.

In the original [Behavioral Study of Obedience](https://pubmed.ncbi.nlm.nih.gov/14049516/), 26 of 40 participants continued to the highest setting. The finding became famous, sometimes flattened into the claim that people are simply obedient. The more interesting problem for modern organizations is how a sequence of apparently reasonable steps can detach people from evaluating what those steps are for.

There is a temptation to apply the same story to AI by declaring that people will obey their chatbot. That is too narrow. The more unsettling arrangement is a team of people and models in which every participant is responsible for a well-defined piece, each verifies the previous piece, and nobody is authorized—or encouraged—to question the underlying objective.

## A project can be correct at every checkpoint and still be wrong

Imagine an organization decides that its community's poor participation numbers reflect an inadequate website. A research system gathers usability complaints. A design agent proposes a redesign. A coding agent builds it. A test system checks behavior and accessibility. A manager approves the release. Every stage has performed the assigned work.

But what if the participation problem was that people had no reason to return, no useful activities, and no community relationships? The redesign could be beautifully executed and irrelevant.

This is not an argument against engineering quality. It is an argument that verifying execution differs from validating purpose.

The [NIST AI Risk Management Framework](https://airc.nist.gov/airmf-resources/playbook/manage/) explicitly includes determining whether an AI system achieves its intended purpose *and whether it should be deployed at all*. Its [measurement guidance](https://airc.nist.gov/airmf-resources/playbook/measure/) calls for independent review, external perspectives, and mechanisms for course correction. These distinctions are not merely about model hallucinations; they are about institutional decisions.

## What should a verifier actually verify?

A useful verifier must be able to compare a task's outputs against evidence. But it should also retain the original human objective, preserve uncertainty and see which assumptions were never independently checked.

If the objective is unsupported, that should be an acceptable conclusion. The verifier needn't manufacture a better objective or accept the task merely because the implementation passed tests. It should distinguish a measured result, a source claim, a user instruction and an inference.

This is one of the more difficult ideas behind Aurekai's work on evidence, retained source identity, reviewable actions and independent authority. These capabilities do not guarantee good judgment. They could, however, give people the information and organizational latitude to question whether a sequence of individually valid actions adds up to something worth doing.

The lesson isn't to distrust every command from a machine. It is to avoid designing a system in which correctness has been reduced to compliance with the most recent command.

Continue with [When everyone agrees, who is thinking?](https://nickgonzales76017.github.io/bonfyre/blog/when-everyone-agrees-who-is-thinking/) and [The internet of bad ideas](https://nickgonzales76017.github.io/bonfyre/blog/the-internet-of-bad-ideas/).
