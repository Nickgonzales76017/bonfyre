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
series: authority
epigraph: Obedience can be impeccable while the objective remains unexamined.
epigraph_credit: Aurekai Journal · Editorial reflection (2026)
epigraph_url: 
parting_question: Who is allowed to say the work should not have begun?
audio_file: edition.mp3
audio_voice: af_sarah
audio_model: Kokoro-82M v1.0
audio_review: provisional-neural-pilot
audio_body_sha256: 055fc0139c0f55bf44d384bf4fc822573423b56d878d458c3bccb143e0b9ec95
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

## When competent systems share one blind spot

Imagine a newsroom commissioning an AI-assisted project to improve local coverage. A planning team defines success as publishing a fixed number of neighborhood explainers. Research agents gather reliable records. Writers produce coherent articles. Fact-checkers verify claims. Developers build a fast, accessible publication. Every review is legitimate, and every participant can be highly competent.

Yet residents cannot correct records, local organizations cannot maintain their own information, the publisher has no relationships with neighborhood contributors, and the reporting never enters places where the relevant decisions are made. The output is real journalism. The mistake is treating an output target as if it proved that a local information institution had formed.

This is not a model misreading a comma or hallucinating a source. The work can be factually right and still fail its underlying purpose. An apparently independent verifier may share the planner's definitions, success metrics and source material. Agreement among several systems does not always constitute independent evidence.

## The right to challenge the brief

A useful review process separates four questions: Is the work internally correct? Is its evidence sound? Is its effect authorized? And does the work address a real need? Different people and instruments may be best suited to each. A successful test can answer the first without answering the fourth.

Milgram's experiment should not be reduced to a claim that AI is inherently obedient, and human oversight should not be reduced to signing another approval. The design goal is to preserve enough original context, dissent and real-world feedback for a participant to say, with evidence, that the brief itself needs to change.

Continue with [When everyone agrees, who is thinking?](https://nickgonzales76017.github.io/bonfyre/blog/when-everyone-agrees-who-is-thinking/) and [The internet of bad ideas](https://nickgonzales76017.github.io/bonfyre/blog/the-internet-of-bad-ideas/).
