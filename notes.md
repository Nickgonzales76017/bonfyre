I scanned materially farther into the estate. The picture is bigger—and cleaner—than “we have 4k seeds plus Ghost templates.”

The estate is really **several different reservoirs that have been accumulated correctly but are not yet being reduced together**. Some are content/material, some are interaction donors, some are institutional capabilities, some are protocols, some are design/compiler tools, and some are merely citations to things that exist.

That distinction is the missing leverage.

## 1. The “4k seeds” are now 4,891 observed candidate occurrences

The current generated manifest at `estate/generated/external-estate-v4/manifest.json` says:

| Source kind       |     Count |
| ----------------- | --------: |
| Public APIs       | **1,756** |
| MCP               | **1,337** |
| RSS               |   **841** |
| Machine protocols |   **739** |
| x402 deployments  |   **211** |
| x402 specs        |     **7** |
| **Total**         | **4,891** |

All 4,891 retain their raw source line and source-document digest.

And importantly:

```text
truth_level = observed_candidate
```

So this is a legitimate reagent reservoir, not an admitted provider estate.

But the current cross-domain classifier is badly saturated.

### The quantitative problem

Of 4,891 occurrences:

* **4,891** appear in at least 2 candidate Worlds.
* **2,902** appear in 10 or more Worlds.
* Maximum reuse is all **30 Worlds**.
* `community` is assigned to **4,884 / 4,891**.
* `touring-musician` gets **2,722** candidates.
* `school-township` gets **4,428**.
* `community-association` gets **4,057**.
* `civic-services` gets **3,976**.
* `research-laboratory` gets **3,768**.

The domains show the same phenomenon:

```text
community      4,884
computers      3,783
research       3,443
trade          2,460
travel         2,424
film           2,312
models         2,309
sports         2,306
music          2,279
art            2,069
people         1,984
labor          1,963
publishing     1,924
government     1,867
advertising    1,813
media          1,784
...
```

That is **density without discrimination**.

And the compiler explains why.

`architecture/compile_external_estate.py` currently gives every record essentially:

```text
world_phases     = discover / observe / plan
interaction_fits = discover / inspector / work
```

Then Worlds are mostly inferred from broad domain overlap. `touring-musician`, for example, is basically:

```text
music
people
travel
hospitality
finance
insurance
logistics
media
```

and two overlapping domains are enough to nominate an occurrence.

So Figma tooling can plausibly fall into Touring Musician because it touches `art + computers`, or some generic developer MCP can get there through `community + media`.

**2,722 is therefore not “2,722 useful things for a musician.”**

It is:

> 2,722 things whose coarse tags are not incompatible with an eight-domain bag.

That is an enormous difference.

---

# 2. The 4,891 contain several hidden estates

I went through actual occurrence families rather than treating them as one catalogue.

There is already a strong **human/material estate** in there:

```text
Spotify
Bandcamp
SoundCloud
Deezer
Discogs
Genius
Jamendo
Audiomack
Mixcloud
7digital

YouTube
Vimeo
Dailymotion
Twitch
TikTok
Instagram

Reddit
Discord
Pinterest
Tumblr
Flickr
Unsplash
Imgur

Eventbrite
OpenStreetMap
Amadeus
transit/aviation/travel systems

eBay
Etsy
Square
commerce/logistics systems

Google Calendar
Drive
Docs
Sheets
Notion
Trello
Jira
Slack
Mattermost
...
```

That is plenty of material for the musician world we were discussing.

But sitting in the same 4,891 rows are things like:

```text
ActivityPub
WebFinger
WebSub
Webmention
OAuth 2
OIDC
WebAuthn
ATProto / XRPC / firehose / Lexicon
Nostr / NIPs
WebRTC
HLS
WHIP
DASH
```

Those are **carriers/protocol machinery**.

And then there is another whole category:

```text
Figma control bridges
Storybook design-system extraction
shadcn registries
design-token translators
CSS debugging systems
visual-regression systems
screenshot/grid auditing
WordPress template tooling
Elementor template operations
MCP tool gateways
...
```

Those aren't musician content at all.

They are **things Aurekai can use to improve Aurekai**.

That is a major missed leverage point.

---

# 3. We need to stop calling every seed the same kind of thing

The existing `ExternalDescriptor` is quite rich. `external-estate.cue` already carries:

* affordances,
* domains,
* actors,
* temporal scope,
* spatial scope,
* economics,
* carriers,
* world phases,
* interaction fits,
* provenance,
* forbidden inference.

What it does **not** carry is the most important distinction for composition:

> **What role does this thing play in the estate?**

Figma and Spotify are both external occurrences.

But:

```text
Spotify
    → material source
    → media service
    → action adapter
    → interaction reference

Figma
    → design reference
    → authoring adapter
    → compiler/toolchain reagent

ActivityPub
    → transport carrier
    → publication carrier
    → federation protocol

CSS visual regression
    → verification tool

YouTube
    → material source
    → media service
    → watch interaction reference
    → publication target

Eventbrite
    → event material
    → ticket/action adapter
    → publication/discovery target
```

One occurrence can legitimately have several roles.

I would add a **derived, non-lossy role projection**, not alter the raw 4,891:

```text
ExternalReagentFingerprint

occurrence_id

roles[]
subject_domains[]
affordances[]

content_modalities[]
interaction_primitives[]
presentation_signals[]

action_capabilities[]
transport_families[]
verification_capabilities[]

surface_demands[]
world_demands[]

provenance
cannot_infer
```

with roles roughly:

```text
material_source
action_adapter
interaction_reference
design_reference
publication_target
transport_carrier
verification_tool
compiler_tool
infrastructure_adapter
commerce_carrier
dataset
```

Then different parts of Bonfÿre consume different projections.

That immediately stops a Figma MCP from appearing in Touring Musician while still making it hugely useful to the surface compiler.

---

# 4. And Bonfÿre already contains the correct pattern for doing this

This was probably the strongest deeper finding.

The **InternetCensus** is much more mature than the 4,891 global corpus.

`architecture/surface/surface.yaff` currently describes **222 InternetSource rows**:

```text
200 declared families
 18 concrete observed sources
  4 explicit refusals
---
222
```

And its rule is excellent:

> The census is a map, not a walk.

Most sources are intentionally not observed.

Observed sources carry extraction contracts so consumers don't need source-specific branches.

The source itself can declare:

```text
where the items live
title field
image field
body field
time
place
etc.
```

and the adapter normalizes them into:

```text
image
event
note
video
post
```

`_kit.js` even contains the explicit principle that a PeerTube `thumbnailPath` vs another provider's `thumbnail_url` belongs in the adapter—not in every surface consuming it.

This is the architecture the 4,891 should feed.

Not:

```text
4,891 seeds
      ↓
feed cards
```

But:

```text
4,891 ExternalEstate candidates
        ↓
requirement/demand
        ↓
candidate selection
        ↓
promotion/admission
        ↓
InternetSource / provider / carrier / reference role
        ↓
material
```

The project has already learned this lesson once.

---

# 5. Even better: `PackDemand` already exists

We do **not** need to invent another generic relevance language.

`cmd/BonfyreCMS/content-types/pack_demand.json` already has:

```text
demand

source_kind:
    internet
    external
    device
    finance
    service

preferred_sources

item_kinds:
    image
    event
    note
    video
    post

world_subject

temporal_scope:
    past
    present
    future
    durable

renderer_roles:
    supporting-evidence
    hero-document
    live-update
    reference

reheat_when
proof_required
originating_pack
```

And WebAddress already consumes these demands.

The native path has known renderer/source roles and resolves active demands against InternetSource.

So the missing move isn't:

> “make a better Touring Musician algorithm.”

It's:

```text
World / StandingAddress / Pack / OpenWork
                  ↓
              PackDemand
                  ↓
        ┌─────────┼──────────┐
        │         │          │
     internet   external   finance/service/device
        │         │          │
        ▼         ▼          ▼
 InternetCensus  ExternalEstate   native owner
                  +
            reagent role
                  ↓
          appropriate closure
```

That is much more Bonfÿre.

---

# 6. Fleming proves this works at much higher density

The Fleming pack is a very useful stress specimen.

Its source registry isn't merely an enumeration. It explicitly says:

> do not enumerate when a tighter source can eliminate a branch or convert a search into direct lookup.

It has hundreds of tiered source surfaces and distinguishes things such as:

```text
Tier 1   direct identifiers / deep links
Tier 2   structured source surfaces
Tier 3   municipal GIS
Tier 4   county/state/federal
...
```

And its Work graph reasons against the **current opportunity set**.

Its principle is basically:

> rank sources by marginal information against unresolved work.

The pack has already closed work over **820,008 observations from 17 sources**.

That is the mature answer to 4,891 seeds.

You don't ask:

> “Which of my 4,891 sources is kind of related to property?”

You ask:

```text
Current Opportunity
        ↓
What exact fact remains unresolved?
        ↓
Which source can most cheaply/authoritatively close it?
        ↓
Read that source
        ↓
new evidence
        ↓
recompile work
```

Exactly the same should happen for:

```text
Touring Musician
    → need Dallas venue availability

Current Show
    → need ticket inventory

Current Release
    → need an embeddable recording

Current Tour
    → need lodging in Houston

Current Clip
    → need publication distribution

Current Fan
    → need lawful community participation
```

The 4,891 then become enormously valuable because they're a **reagent library**, rather than an enormous feed.

---

# 7. There is also a promoted-partner estate hiding behind the seeds

This is another major correction.

`packs/aurekai/session-reuse-census.yaff` records:

* **41 shipped integration repos**
* **176 partner profiles**
* **1,885 authored role observations**
* **82 deep-promoted specialties**
* **587 explicit non-ownership boundaries**

And the source is extremely explicit about what that means.

A partner isn't a name in a Markdown table.

A partner is its **shipped executable integration plus declared role**.

It also found:

* 146 partners with an authored role,
* 72 participating in ≥2 fabrics,
* 32 in ≥3,
* 9 in four fabrics with five distinct roles each.

The crucial lesson is already written into the pack:

> `Pairs-with` is the compound graph. Program binding is a **JOIN, not a search**.

This is directly relevant to our 4,891.

If an external occurrence says:

```text
Riskfolio-Lib
```

and the estate already has:

```text
Riskfolio-Lib
role = signal correlation / concentration / ensemble risk
cannot_own = final decision law
```

the seed classifier should not rediscover what Riskfolio “might do” from words.

The **promoted role wins**.

Likewise for anything already internalized through one of those 41 shipped integration repos.

So the resolution order should become something like:

```text
external occurrence
       ↓
exact promoted partner-role match?
       ├── yes → use promoted role contract
       │        + preserve external occurrence as provenance/reference
       │
       └── no  → remain candidate and enter capability/admission path
```

That prevents the seed estate from competing with the mature estate.

---

# 8. The Ghost estate is much larger, but its maturity has an important nuance

The current census really does contain **238 named Ghost themes**.

And I found the complete install path.

`install_ghost_marketplace_templates.sh`:

1. compiles all 238,
2. validates each `presentation_template`,
3. installs them through `bonfyre-cms`,
4. queries the resulting template surface,
5. smoke-checks reference names,
6. reports its install verdict/count.

So these aren't merely a conceptual list. **The estate can materialize all 238 into CMS.**

I did **not**, from the checkout alone, establish that the currently running production CMS database contains every one of the 238 right now. The installer exists and is designed to do so; runtime deployment state is a separate fact.

But there is a much more important limitation.

## The 238 are not currently 238 studied visual systems

The source census contains basically:

```text
name
categories
```

Then `ghost_marketplace_templates.py` maps Ghost category into eight design families:

| Ghost category | Bonfÿre family / mix                |
| -------------- | ----------------------------------- |
| Blog           | editorial · `vertical board thread` |
| Magazine       | `vertical board thread market`      |
| News           | `vertical thread board inspector`   |
| Newsletter     | `vertical board`                    |
| Podcast        | `stage vertical thread`             |
| Photography    | `board vertical spatial`            |
| Portfolio      | `board vertical inspector`          |
| Docs           | `inspector work thread`             |

Then it uses SHA-256 bytes of the **theme name** to deterministically select:

```text
density
hero_mode
navigation_mode
media_mode
typography_mode
card_mode
chrome_mode
```

and visual token variations.

Therefore:

```text
238 Ghost identities                   TRUE
238 materializable CMS templates       TRUE
238 deterministic visual variants      TRUE
238 independently extracted designs    FALSE
```

That matters.

The visual sameness we've been seeing isn't just a rendering problem.

**We have not yet extracted most of the actual design information from the Ghost estate.**

---

# 9. The good news: `presentation_template` is already shaped for the upgrade

The CMS type already stores:

```text
reference_source
reference_name
reference_url
reference_author
reference_categories
reference_price

family
renderer_mix
density

hero_mode
navigation_mode
media_mode
typography_mode
card_mode
chrome_mode

territory_affinity

semantic_roles
layout_tokens

acceptance_contract
cannot_infer
```

So we don't need to replace the template system.

We should deepen the extraction to an actual:

```text
DesignFingerprint
```

with observed dimensions such as:

```text
masthead topology
navigation topology
sticky behavior

hero topology
lead-story ratio

information hierarchy depth
card anatomy

image dominance
crop strategy
aspect-ratio language

grid topology
rail topology

article measure
article interruption rhythm

caption treatment
quote treatment
collection treatment

pagination / sequence behavior

membership CTA
comments
author density
metadata density

responsive transformation

negative-space pattern
dark/light handling
motion language
```

Then:

```text
Ghost theme
    ↓
observed DesignFingerprint
    ↓
PresentationTemplate
    ↓
renderer compatibility
    ↓
fragment dressing
```

That's how the 238 become genuinely valuable instead of 238 deterministic skins.

---

# 10. There are actually FOUR different presentation estates already

This is the part that makes the current visual thinness especially wasteful.

## A. 41 semantic SurfacePatterns

The ControlPlane contains **41** patterns.

`.live` includes things such as:

```text
VerticalDiscovery
LiveCreatorRoom
EphemeralMoment
RichContentMoment
OverlayViewer
ActivityTimeline
ExternalHandoff
TravelActionCard
```

`.life` includes:

```text
PersonalDashboard
CollectionBoard
HabitatDashboard
PlaceDetail
NeighborhoodLayer
InterestCommunity
MemoryArchive
PlanWorkspace
PeopleGraph
HomeState
```

`.shop` has 11 more.

`.work` has 12 more, including:

```text
ProfessionalHome
WorkInbox
ChannelThread
IssueBoard
ProjectRoadmap
KnowledgePage
DecisionPage
CRMRecord
CalendarWorkspace
ApprovalQueue
AgentWorkmate
ActivityStream
```

These are semantic interaction patterns.

## B. 19 explicit donor/reference projects

The same ControlPlane declares actual donor lineages:

```text
Pinry
MapLibre
Home Assistant
Fedify
Lemmy
Pixelfed
Immich
Mobilizon

Spree
Next.js Commerce
Hydrogen
Bagisto
Saleor

Zulip
Mattermost
Plane
AppFlowy
Twenty
Cal.com
```

These are interaction/reference physics.

## C. 10 current SurfaceRenderer families

```text
vertical
stage
thread
board
community
market
search
spatial
work
inspector
```

## D. 238 Ghost publication identities/templates

These are publication/design dressing.

And then there's the fifth axis we already found:

## E. 60 ActorPresentationCells

```text
15 grammar situations × 4 actor classes
```

So the actual surface material isn't small at all.

It is more like:

```text
41 semantic patterns
×
19 donor/reference lineages
×
10 current renderer families
×
238 publication references
×
60 actor/context cells
×
territory
×
extent
×
subject
```

Obviously we don't materialize the Cartesian product.

But that's the **potential grammar space already sitting in the estate.**

The problem is that those axes terminate in separate systems.

---

# 11. And then the nine Frappe apps are effectively a sixth presentation estate

The deeper Frappe scan from the prior pass still fits here perfectly.

We have mature interactions hiding inside:

```text
CRM
    Kanban
    record timeline
    correspondence
    calendar
    dashboard

Drive
    list/grid
    preview
    sharing
    uploads
    activity

Insights
    visual query
    joins
    filtering
    tables
    dashboard

LMS
    video
    resume
    progression
    chapters
    discussion

ERPNext
HRMS
Helpdesk
Wiki
Frappe core
...
```

And `BonfyreFrappeCompiler` currently extracts their **business/schema grammar**, not most of their human interaction grammar.

So Ghost isn't even our only huge styling/reference reservoir.

We've got:

```text
GHOST             publication/design grammar
SURFACE PATTERNS  semantic UI grammar
DONORS            interaction physics
FRAPPE APPS       mature application physics
EXTERNAL ESTATE   external design/action/material references
```

All separate.

That is why hand-writing another pretty page would be absurd.

---

# 12. There is also an append-only reference estate

`ReferenceContract` declares roughly **271 reference identities** over the surface/reference world.

This gives us another important distinction:

```text
reference identity
≠
copied code
≠
semantic owner
≠
visual style
≠
interaction implementation
```

That's useful for Ghost too.

A theme can remain an immutable design reference while Aurekai compiles an abstract fingerprint from it.

No need to ingest theme code as runtime dependency.

---

# 13. The old ExternalEstate architecture is showing generation drift

There is an older `ExternalEstateIndex` declaration in `architecture/external/foreign.yaff` describing:

```text
4,294 catalogue resources
269 census rows
65 actually walked
```

The current generated estate says:

```text
4,891 candidate occurrences
```

and current InternetCensus architecture says:

```text
222 census rows
= 200 families + 18 observed + 4 refused
```

The `external_resource` CMS type still exists, so its conceptual owner has not disappeared.

But the counts represent different generations of the system.

This matters because the architecture atlas/textbook has some **historical measurements mixed with newer generated truth**.

We shouldn't destroy the lineage.

We should make generation epoch explicit:

```text
ExternalEstateSnapshot
    edition
    generated_at
    source_digest
    occurrence_count
    classifier_revision
```

Then architectural observations can say which snapshot they measured.

Otherwise we'll keep encountering “4,294 vs 4,891” and thinking one is wrong.

They're probably different estate generations.

---

# 14. There is an older opportunity-seed estate too

I also found recovery material such as:

`evidence/recovery/opportunity-20260803-v2/...`

with roughly **100 explicit opportunity seeds**.

Those records are actually much more business-action oriented than the broad ExternalEstate:

```text
source
setup fit
app fit
opportunity kind
expected USD
recurrence
verification requirements
live status
evidence
```

Examples include nonprofit programs, cloud credits, grants, tax incentives, SaaS programs, etc.

I would **not** promote that recovery corpus back into canonical truth automatically.

But its schema contains useful design lineage:

> External resources aren't merely “interesting”; some can be modeled as opportunities with value, eligibility, recurrence, evidence and next action.

That is useful for the Grant/Funding/Business Worlds.

Again: preserve lineage; extract grammar.

---

# 15. There is even some census drift in the command estate

Another small but meaningful estate-quality issue:

The current generated command inventory says:

```text
command_count = 94
```

while its serialized projection contains only a much smaller set of command entries.

Meanwhile canonical command architecture still repeatedly names:

```text
the 91-command capability surface
```

This doesn't mean the commands disappeared.

It means **the generated census itself needs an integrity/freshness gate** before we use its count as a compiler input.

That's worth fixing because the same lesson applies everywhere:

```text
declared count
generated rows
runtime rows
measured rows
```

should be distinguishable.

---

# 16. So the real estate map now looks like this

| Reservoir                         |        Current size / shape | What it really is                               |
| --------------------------------- | --------------------------: | ----------------------------------------------- |
| ExternalEstate                    |                   **4,891** | observed candidate reagent library              |
| Machine protocols inside it       |                     **739** | carrier/protocol candidates                     |
| MCP inside it                     |                   **1,337** | tool/action/control candidates                  |
| InternetCensus                    |                     **222** | intentionally small active/declared ways-in map |
| Internet inactive candidate graph |               hundreds more | dormant source candidates                       |
| Ghost reference census            |                     **238** | publication/design identities                   |
| SurfacePatterns                   |                      **41** | semantic interaction patterns                   |
| Donor/reference apps              |                      **19** | interaction physics/provenance                  |
| SurfaceRenderers                  |                      **10** | current renderer families                       |
| ActorPresentationCells            |                      **60** | actor-sensitive presentation policy             |
| Frappe products                   |                       **9** | mature business + interaction grammars          |
| Promoted integrations             |                **41 repos** | executable partner capability estate            |
| Partner profiles                  |                     **176** | role metadata                                   |
| Partner role observations         |                   **1,885** | authored semantic roles                         |
| Deep-promoted specialties         |                      **82** | highly developed partner roles                  |
| Negative ownership boundaries     |                     **587** | things integrations must not own                |
| ReferenceContract                 |         ~**271 identities** | append-only reference lineage                   |
| Packs                             | many institutional programs | scoped demand/work/evidence/capital             |
| Command estate                    |             ~90+ identities | native capability organs                        |
| Recovery opportunity seeds        |                    ~**100** | older opportunity/value grammar lineage         |

And this still doesn't count the full local-model/kernel estate as a separate analysis dimension.

So yes—the surface problem is dramatically larger than “add TikTok and YouTube.”

---

# 17. The right compiler is becoming obvious

We should **not** create a monster unified table.

The reservoirs should remain different because their epistemic meaning is different.

The missing layer is a typed reduction:

```text
                 RAW / AUTHORED ESTATES

 ExternalOccurrence ─────┐
 InternetSource ─────────┤
 GhostReference ─────────┤
 SurfacePattern ─────────┤
 DonorReference ─────────┤
 FrappeInteraction ──────┤
 PartnerRole ────────────┤
 ProtocolCandidate ──────┤
 NativeCapability ───────┤
 Pack/OpenWork ──────────┘
                         │
                         ▼
               NON-LOSSY PROJECTIONS

           reagent role / interaction role
           design fingerprint
           capability binding
           material fitness
           temporal fitness
           actor fitness
           evidence/authority standing
                         │
                         ▼
                    PackDemand
                 StandingAddress
                   OpenWork
                         │
                         ▼
                Candidate Closure
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
 ExternalMaterial   ActionAperture   Compiler/Tool
        │                │           (never shown)
        └────────────────┼────────────────┘
                         ▼
                 SurfaceFragmentIR
                         │
                         ▼
               SurfaceCompositionIR
```

And **PackDemand is already there**.

That's the big reduction in implementation risk.

---

# 18. Touring Musician becomes extremely concrete after this scan

Suppose the current addressed subject is:

```text
artist: X
tour: Fall 2026
city: Dallas
actor: fan
```

The compiler doesn't ask the 4,891:

> “what mentions music?”

It derives needs:

```text
hero material
current recording
current performance
Dallas occurrence
fan conversation
tour geography
available offer
related long-form media
```

Then:

### `current recording`

```text
PackDemand
source_kind = external/internet
item_kind = audio/media
renderer role = hero/supporting media
temporal = present
```

Candidate closure can encounter:

```text
Spotify
Bandcamp
SoundCloud
YouTube
...
```

and choose from **admitted/currently usable** representations.

### `Dallas occurrence`

Can pull:

```text
artist schedule
venue
Mobilizon/event material
Eventbrite/ticket system
maps
local publication/event feeds
```

but only because the addressed World needs an upcoming Dallas occurrence.

### `fan conversation`

Can close over:

```text
native community
ActivityPub federation
Reddit
Discord
etc.
```

depending on admission and actor scope.

But the final fragment is:

```text
COMMUNITY
```

not:

```text
Discord API
ActivityPub
Reddit MCP
```

### `visual presentation`

The composer can choose:

```text
SurfacePattern = RichContentMoment
renderer       = vertical
template       = Ghost-derived fingerprint
actor cell     = personal/fan
extent         = immersive
```

while another fragment immediately afterward can use:

```text
SurfacePattern = PlaceDetail
renderer       = spatial
template       = same world family, different scoped fragment dressing
```

Same subject closure.

Different grammar.

---

# 19. And Aurekai itself can consume the seed estate recursively

This is one of the highest-value things the deeper scan exposed.

The 4,891 are not only ingredients for Aurekai users.

They contain ingredients for **Aurekai's own development loop**:

```text
Ghost references
       +
Figma bridges
       +
Storybook extractors
       +
shadcn registries
       +
design-token converters
       +
CSS debugging
       +
visual-regression tools
       +
screenshot/grid auditing
       +
19 donor projects
       +
9 Frappe interaction estates
       ↓
 Surface Design/Interaction Compiler
       ↓
 DesignFingerprint
 InteractionFingerprint
 ResponsiveFingerprint
 AccessibilityFingerprint
       ↓
 SurfaceRenderer variants
       ↓
 generated reference fixtures
       ↓
 visual/semantic gauntlet
       ↓
 promoted presentation grammar
```

That is materially more powerful than manually studying 238 Ghost themes.

**Bonfÿre can use its own external reagent estate to industrialize the extraction.**

And because the system already understands provenance, cannot-infer boundaries, partner roles, tests, packs and generated contracts, this can be done without pretending copied visual inspiration is semantic ownership.

---

# 20. This changes what I would build first

After scanning all of this, I would no longer begin by simply adding `watch`, `listen`, `gallery`, etc.

The first mature pass should be:

1. **Give ExternalEstate a derived reagent-role projection.** Preserve all 4,891 unchanged.
2. **Extend existing `PackDemand source_kind=external` resolution** rather than create a new World relevance subsystem.
3. **Join promoted PartnerRole before heuristic candidate inference.**
4. **Make PackGraph/OpenWork demand the primary selector**; broad World domains become recall hints only.
5. **Extract actual Ghost DesignFingerprints** and stop counting name-hash differences as design extraction.
6. **Extract Frappe InteractionFingerprints** beside existing metadata grammar.
7. **Compile Pattern + Renderer + DesignFingerprint + ActorCell + material into the missing FragmentIR.**
8. Then extract **Watch from LMS/media machinery**, because it already has the deepest implementation substrate.
9. Build **Listen after music subject ownership is canonicalized**.
10. Turn Community back toward human social closure and keep federation mechanics underneath it.

That gets us from:

```text
lots of impressive inventories
```

to:

```text
one machine that can actually exploit its inventories.
```

The deeper estate is therefore not revealing that Aurekai needs hundreds more features. It is revealing something better:

> **Aurekai already possesses an unusually large amount of the raw semantic, interaction, design, integration, institutional and execution material. The severe underuse is in the reduction/join layer. We are repeatedly stopping at “catalogued,” “authored,” “promoted,” or “extracted” one stage before those estates are allowed to compound.**

That is the piece I would attack next.
