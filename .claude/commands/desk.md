---
description: What Aurekai is holding open for decision right now
allowed-tools: Bash(curl:*), Bash(python3:*), Skill
---

Read the desk and report what is open, most-closable first.

!`curl -sS -m 20 https://aurekai.life/studio/ -H "Accept: text/html" | grep -oE '<h3>[^<]*</h3><span class="desk-n">[0-9]+|desk-l">[^<]*' | sed 's/<[^>]*>//g' | head -40`

For each non-empty queue say what it is, how many, and the single act that closes
it. Name the one you would do first and why. If a queue is large, say whether it
is genuinely open or stranded rows — unfinished work especially, since
`in_progress` is a stored state and the operator store has four writers.
