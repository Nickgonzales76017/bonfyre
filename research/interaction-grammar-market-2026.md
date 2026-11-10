# Twenty interaction grammars for Bonfyre

Research date: 2026-09-13. This is a donor-grammar portfolio, not a plan to
copy twenty backends or claim compatibility with private APIs. A donor is
useful when its current documented behavior exposes a durable interaction
shape that maps to Bonfyre owners and can be compiled only after a real subject
binding exists.

## Native portfolio

| # | Donor | Reusable grammar | Native owner constellation | Lowerer |
|---:|---|---|---|---|
| 1 | YouTube | creator channel, video/short/live shelves | CreatorChannel, MediaProgram, Community | ChannelSurfaceCompiler |
| 2 | Discord | persistent community, forum/thread, Stage | Community, Stage, Correspondence | CommunitySurfaceCompiler |
| 3 | Microsoft Teams | institution workspace, channels, meetings/files/apps | WorkGraph, Community, ArtifactGraph | WorkspaceCompiler |
| 4 | Slack | workstream, canvas/list/workflow, huddle | WorkGraph, KnowledgeGraph, Correspondence | WorkspaceCompiler |
| 5 | Facebook | social graph, group/page/feed, marketplace aperture | RelationshipGraph, Community, Offer | SocialSurfaceCompiler |
| 6 | Reddit | topic community, durable thread, moderation/vote | Community, Moderation, EvidenceGraph | CommunitySurfaceCompiler |
| 7 | Twitch | creator live stage, chat, clips | CreatorChannel, Stage, MediaProgram | StageCompiler |
| 8 | TikTok | short-media candidate stream, creator/live | CreatorChannel, MediaProgram, AddressPlane | ChannelSurfaceCompiler |
| 9 | Instagram | identity-mediated media, story/reel/community | RelationshipGraph, CreatorChannel, Community | SocialSurfaceCompiler |
| 10 | WhatsApp | private participant-scoped correspondence | Correspondence, ActorClosure, InformationPolicy | CorrespondenceCompiler |
| 11 | Telegram | group/community, broadcast channel, bot aperture | Community, Correspondence, CapabilityProgram | CommunitySurfaceCompiler |
| 12 | GitHub | artifact/change review, issue/PR/discussion | ArtifactGraph, WorkGraph, EvidenceGraph | WorkGraphCompiler |
| 13 | Notion | document/database/wiki workspace | KnowledgeGraph, ArtifactGraph, WorkGraph | KnowledgeCompiler |
| 14 | Linear | issue, project, cycle, roadmap | WorkGraph, TargetObligation | WorkGraphCompiler |
| 15 | Jira | issue/service workflow and triage | WorkGraph, ProtocolContract | WorkGraphCompiler |
| 16 | Figma | design artifact, prototype, reviewed handoff | ArtifactGraph, WorkGraph, SurfaceIR | ArtifactCompiler |
| 17 | Spotify | personal/shared audio library and playlists | MediaLibrary, MediaProgram | MediaLibraryCompiler |
| 18 | Pinterest | visual save, collaborative board, discovery | WorldPublication, Board, Offer | BoardCompiler |
| 19 | Shopify | catalog, collection, cart/checkout/order | Offer, ProtocolContract, Finance | CommerceCompiler |
| 20 | Airbnb | place-rich reservation/trip planning | TargetObligation, ProtocolContract, WorkGraph | ProtocolCompiler |

## Evidence for the grammar choices

Discord distinguishes durable forum discussions from more temporary threads,
and Stage distinguishes audience and speaker roles. This supports separate
Community and Stage shapes rather than a generic chat clone. [Discord Forum
Channels FAQ](https://support.discord.com/hc/en-us/articles/6208479917079-Forum-Channels-FAQ),
[Discord Stage Channels FAQ](https://support.discord.com/hc/en-us/articles/1500005513722-Stage-Channels-FAQ)

Teams distinguishes topic-oriented channels from chat and combines people,
content and tools. Slack combines conversation with tabs, canvas/list/workflow
bodies and huddles, which supports a workspace grammar rather than a message
store clone. [Microsoft Teams navigation](https://learn.microsoft.com/en-us/microsoftteams/navigate-teams),
[Slack channel tabs](https://slack.com/help/articles/32562841868307-Add-and-manage-tabs-in-channels-and-direct-messages),
[Slack workflows](https://slack.com/help/articles/17542172840595-Build-a-workflow--Create-a-workflow-in-Slack)

GitHub’s product communication centers on issues, pull requests and
discussions. Linear documents issues, teams, projects, cycles and views as
distinct concepts, supporting WorkGraph mappings rather than copied records.
[GitHub communication](https://docs.github.com/en/get-started/using-github/communicating-on-github),
[Linear conceptual model](https://linear.app/docs/conceptual-model)

Notion databases are collections of pages with properties and multiple views;
Figma Dev Mode is a reviewed design-to-development handoff surface, not an
artifact identity owner. [Notion databases](https://www.notion.com/help/intro-to-databases),
[Figma Dev Mode](https://help.figma.com/hc/en-us/articles/15023124644247-Guide-to-Dev-Mode)

Pinterest distinguishes public, private and group boards. Shopify separates
catalog/collection organization from checkout, where inventory is rechecked at
the payment boundary. That supports separate Board/Offer and
ProtocolContract/Finance boundaries. [Pinterest boards](https://help.pinterest.com/en/article/boards),
[Shopify collections](https://help.shopify.com/en/manual/products/collections),
[Shopify checkout](https://help.shopify.com/en/manual/checkout-settings)

Telegram distinguishes large groups from broadcast channels and documents
replies, mentions, roles and bot moderation; its grammar maps to Community and
CapabilityProgram rather than Telegram protocol emulation. [Telegram group
chats](https://telegram.org/tour/groups/tr?setln=en)

## Compiler law

`BonfyreInteractionGrammar` persists the closed catalog plus active bindings.
It emits an `interaction_grammar` CandidateAdvertisement only for a binding
with concrete subject, institution (when applicable), exact-owner reference and
visibility. AddressPlane then provides deterministic forwarding only; exact
verification and the declared lowerer remain downstream.

The catalog is not a subject, user, conversation, media work, right, provider,
authority, payment, protocol enactment or effect. A mask match does not prove
that the verifier ran or that the named lowerer is deployed.

## Delivery order

1. Community/Stage: Discord, Reddit, Telegram and Twitch.
2. Workspace/WorkGraph: Teams, Slack, GitHub, Linear and Jira.
3. Channel/MediaLibrary: YouTube, TikTok, Instagram and Spotify.
4. Board/Offer/Protocol: Pinterest, Shopify and Airbnb.
5. Knowledge/artifact: Notion and Figma.

Every live lowering still requires authenticated ActorClosure where relevant,
exact verifier result, real owner worker, and owner receipts for mutation.
