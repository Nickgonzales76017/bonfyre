---
type: research
cssclasses:
  - research
title: Transcription Pricing Analysis
created: 2026-04-03
updated: 2026-04-03
status: active
tags:
  - active
  - research
  - pricing
  - monetization
aliases:
  - Transcription Pricing Analysis
---

## Related hubs
- [[04-Systems/02-Pipelines/Transcription Pipeline]]



# Research: Transcription Pricing Analysis

## Question
What should the per-file price be for a local AI transcription + summary + action-items deliverable, and what's the evidence behind that number?

## Why It Matters
Price too low and we subsidize buyers with our time. Price too high and nobody tries it. The right number has to account for what competitors charge, what the buyer's alternative costs them, and what our actual unit economics look like. This isn't a guess — it's the difference between a $30/hr grind and a $100/hr margin business.

---

## 1. Competitive Landscape (What the Market Already Pays)

### Subscription Transcription Tools
| Service | Plan Cost | Included Minutes | Effective $/min | Cost for 30-min File | Notes |
|---|---|---|---|---|---|
| Otter.ai Pro | $16.99/mo | 1,200 min/mo | $0.014/min | $0.42 | But most users use <300 min → real cost ~$0.057/min = **$1.70/file** |
| Otter.ai Business | $30/mo | 6,000 min/mo | $0.005/min | $0.15 | Only makes sense at scale |
| Descript Pro | $24/mo | Unlimited | Flat subscription | $24/mo flat | Requires learning a full editing tool — overkill for notes |
| Notta Pro | $13.99/mo | 1,800 min/mo | $0.008/min | $0.23 | Newer player, less trusted |
| Fireflies.ai Pro | $18/mo | Unlimited meetings | Flat subscription | $18/mo flat | Meeting-specific, not general audio |

**Insight**: Subscription tools look cheap per-minute, but the actual cost-per-use is much higher for casual users. Someone who records 3-4 meetings a month pays **$4-8 per actual use** on Otter. The subscription is the pain point — not the per-file cost.

### Pay-Per-Use Services
| Service | Pricing Model | Cost for 30-min File | Turnaround | Notes |
|---|---|---|---|---|
| Rev (human) | $1.50/min | **$45.00** | 12-24 hrs | Gold standard accuracy, expensive |
| Rev (AI) | $0.25/min | **$7.50** | Minutes | Lower accuracy, no summary/actions |
| GoTranscript (human) | $0.72/min | **$21.60** | 12-24 hrs | Budget human option |
| TranscribeMe | $0.79/min | **$23.70** | 12 hrs | Medical/legal focus |
| Scribie | $0.80/min | **$24.00** | 24 hrs | Bulk transcription |

**Insight**: Human transcription runs $20-45 for a 30-min file. AI-only pay-per-use is $5-10. Neither includes summarization or action items. We're selling a **higher-value deliverable** for less than human-only transcript pricing.

### API / DIY Options
| Service | Cost for 30-min File | Requires |
|---|---|---|
| OpenAI Whisper API | $0.18 | Dev setup, cloud account, API key, custom formatting |
| AssemblyAI | $0.36 | API integration |
| Deepgram | $0.30 | API integration |
| Google Speech-to-Text | $0.72 | GCP account, API integration |

**Insight**: APIs are dirt cheap but require technical setup the buyer doesn't have. We're arbitraging the gap between API cost ($0.18) and the **effort cost** of setting up the pipeline, running it, formatting the output, and extracting insights.

---

## 2. Buyer's Alternative Cost (What They Pay By NOT Using Us)

The real competition isn't Otter — it's the buyer doing nothing, or doing it themselves.

### Founder / Operator Profile
- Average hourly value of their time: $100-300/hr (based on opportunity cost, not salary)
- Time to manually process a 30-min recording:
  - Listen back: 30-45 min (1-1.5x realtime)
  - Take notes while listening: +15 min
  - Organize into summary + action items: +10-15 min
  - **Total: 55-75 min = $90-375 of their time**
- Most commonly: they just don't do it. The recording rots.

### Student Profile
- Hourly value of their time: $15-25/hr (tutoring rate as proxy)
- Time to manually process a 60-min lecture recording:
  - Listen + take notes: 60-90 min
  - Organize: 15-20 min
  - **Total: 75-110 min = $19-46 of their time**
- Most commonly: they skip the recording entirely.

### Creator Profile
- Hourly value: $50-150/hr (content creation rate)
- Time to repurpose a 30-min interview:
  - Listen + pull quotes: 45 min
  - Write summary: 20 min
  - **Total: 65 min = $54-163 of their time**

**Insight**: For founders, even a $25 per-file price saves them $65-350 per file. For students, $8-12 is the sweet spot. For creators, $15-25 is an easy yes.

---

## 3. Our Unit Economics (What It Actually Costs Us)

### Cost Structure Per File (30 min audio)
| Item | Cost |
|---|---|
| Compute (local, already owned M3) | $0.00 |
| Electricity (~15 min at ~30W) | ~$0.001 |
| Whisper model download (one-time) | $0.00 |
| Software (all open source) | $0.00 |
| **Total hard cost** | **~$0.00** |

### Time Cost Per File (Current Manual Workflow)
| Step | Estimated Time |
|---|---|
| Receive file + create job folder | 1 min |
| Run pipeline (30-min file, `base` model) | ~15 min (unattended) |
| Review transcript for major errors | 3-5 min |
| Review/edit summary + action items | 2-3 min |
| Package deliverable + send back | 2 min |
| **Total active labor** | **8-11 min** |
| **Total wall-clock** | **~20 min** |

### Effective Hourly Rate by Price Point
| Price/File | Active Labor | Effective $/hr | Verdict |
|---|---|---|---|
| $5 | 10 min | $30/hr | Undervalued. Worse than freelance rate. |
| $10 | 10 min | $60/hr | Decent but leaves margin on the table. |
| $15 | 10 min | $90/hr | Strong. Matches consulting-adjacent rate. |
| $20 | 10 min | $120/hr | Premium. Sustainable and scalable. |
| $25 | 10 min | $150/hr | Top of range for cold outbound. |
| $15 | 5 min (automated) | $180/hr | Phase 2 with automation. |
| $20 | 5 min (automated) | $240/hr | Phase 2 premium tier. |

**Insight**: At $15/file and 10 min labor, we earn $90/hr. At $20 with partial automation, we're over $200/hr. A $10 price *works* only if we're trying to undercut the market to build volume — but we don't have volume infrastructure yet. **We should price for margin, not volume.**

---

## 4. Pricing Psychology

### Why Per-File Beats Subscription
- Subscriptions create commitment anxiety ("will I use it enough?")
- Per-file removes all risk for the buyer ("pay only when I need it")
- Per-file creates urgency ("I have a recording right now")
- Per-file is easier to expense ("$20 for meeting notes" vs "$24/mo for a tool we might not use")

### Anchoring Strategy
- Anchor against human transcription ($45 for 30 min on Rev)
- Our price should feel like a steal compared to human pricing but premium compared to raw AI transcript
- The deliverable (summary + action items) is the differentiator — nobody else does this at per-file pricing

### The Magic Number Test
- "Would you pay $X to turn last week's important meeting into a clean summary?" 
- At $5: "sure, whatever" (too cheap, signals low quality)
- At $10: "fine" (acceptable but forgettable)
- At $15: "yeah, that's fair" (feels proportional to value)
- At $20: "if it's good, yes" (slight hesitation = right at the value edge)
- At $25: "show me a sample first" (need proof, but willing)
- At $30+: "what makes this worth it?" (over the impulse buy threshold)

**Sweet spot for cold outreach: $15-20.** Low enough to say yes without thinking too hard. High enough to signal quality.

---

## 5. Recommended Pricing Structure

### Tiered by Length + Complexity
| Tier | File Length | Deliverable | Price | Rationale |
|---|---|---|---|---|
| **Quick** | Under 15 min | Transcript + summary + actions | **$12** | Impulse-buy range. 5 min labor. Students + quick voice memos. |
| **Standard** | 15-45 min | Transcript + summary + actions | **$18** | Core product. Meetings, lectures, interviews. Best margin. |
| **Deep** | 45-90 min | Transcript + summary + actions + section headers | **$25** | Longer files need more review. Founders, consultants. |
| **Complex** | Multi-speaker or 90+ min | Transcript + summary + actions + speaker labels | **$35** | Requires diarization or manual speaker tagging. Premium. |

### Batch Pricing (Volume Discount)
| Bundle | Files | Price | Per-File | Discount |
|---|---|---|---|---|
| Starter | 1 file | Full price | $12-35 | None |
| Pack of 3 | 3 files | -15% | ~$10-30/file | "Try it out" |
| Weekly (5 files) | 5 files/week | -25% | ~$9-26/file | Regular users |
| Retainer | 10+ files/week | Custom | Negotiated | Enterprise-adjacent |

### Why Not $10 Flat?
- $10 flat for everything from a 3-min voice note to a 90-min multi-speaker recording makes no sense economically
- A 90-min file costs 3x the processing time and 4x the review effort of a 15-min file
- Flat pricing also signals "I haven't thought about this" to sophisticated buyers
- $10 is in no-man's-land: too expensive for commodity transcript, too cheap for a structured deliverable

---

## 6. Validation Plan
- [ ] Price the first 3 outbound messages at $15 (standard tier) and see if anyone pushes back
- [ ] Offer one free "demo file" at each tier to test time-per-file assumptions
- [ ] Track: conversion rate, pushback frequency, willingness-to-pay signals in replies
- [ ] If conversion >30%, test $20 on the next batch
- [ ] If conversion <10%, drop to $12 and add a "first file free" hook

---

## Working Conclusion
**Start at $15 for standard files (15-45 min). $12 for short files. $25 for long/complex files.**

The evidence:
1. Human transcription runs $20-45/file → we're 30-60% cheaper with more value (summary + actions)
2. Subscription tools cost $4-8 per actual use for casual users → we're 2-3x more but zero commitment
3. Buyer's DIY cost is $55-375 in time → we save them 80-95% of that effort
4. Our compute cost is $0 → every dollar is margin
5. At $15 and 10 min labor, we earn $90/hr → strong enough to sustain, room to automate higher
6. $10 flat for all files is economically wrong and signals amateur pricing

---

## Links
### Informs
- [[02-Projects/Project - Local AI Transcription Service]]
- [[05-Monetization/Offer - Local AI Transcription Service]]
- [[03-Research/Research - Local AI Transcription Buyers]]

### Concepts
- [[04-Systems/03-Concepts/Monetization]]
- [[04-Systems/03-Concepts/Arbitrage]]

### Related Systems
- [[04-Systems/01-Core Systems/Local AI Transcription Service]]
