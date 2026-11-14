/* Generated from page-composition.cue. Do not edit. */
#ifndef BONFYRE_NATIVE_PAGE_COMPOSITION_H
#define BONFYRE_NATIVE_PAGE_COMPOSITION_H
#include <stddef.h>
typedef enum { BF_PAGE_MATERIAL_PUBLICATION, BF_PAGE_MATERIAL_VIDEO, BF_PAGE_MATERIAL_PHOTO, BF_PAGE_MATERIAL_PLACE, BF_PAGE_MATERIAL_AUDIO, BF_PAGE_MATERIAL_POST, BF_PAGE_MATERIAL_EVENT, BF_PAGE_MATERIAL_MIX, BF_PAGE_MATERIAL_PASSAGE, BF_PAGE_MATERIAL_ORGANIZATION, BF_PAGE_MATERIAL_CHANNEL, BF_PAGE_MATERIAL_COUNT } BfPageMaterial;
typedef enum { BF_PAGE_TOPOLOGY_LEAD, BF_PAGE_TOPOLOGY_SHELF, BF_PAGE_TOPOLOGY_FIELD, BF_PAGE_TOPOLOGY_MAP, BF_PAGE_TOPOLOGY_INDEX, BF_PAGE_TOPOLOGY_TRACKS, BF_PAGE_TOPOLOGY_THREAD, BF_PAGE_TOPOLOGY_LIST, BF_PAGE_TOPOLOGY_FEED, BF_PAGE_TOPOLOGY_WALL, BF_PAGE_TOPOLOGY_GRID, BF_PAGE_TOPOLOGY_COMMUNITIES, BF_PAGE_TOPOLOGY_TABLE, BF_PAGE_TOPOLOGY_RESULTS, BF_PAGE_TOPOLOGY_AGENDA } BfPageTopology;
typedef struct { const char *role; const char *label; BfPageMaterial material; BfPageTopology topology; const char *language; unsigned min_items; unsigned max_items; } BfPageRegion;
typedef struct { const char *scheme; BfPageMaterial material; } BfPageSubjectScheme;
typedef struct { const char *grammar; const char *label; } BfPageGrammarLabel;
typedef struct { const char *kind; BfPageMaterial material; } BfPageSourceKind;
static const BfPageRegion bf_page_regions[] = {
  {"lead","",BF_PAGE_MATERIAL_PUBLICATION,BF_PAGE_TOPOLOGY_LEAD,"ghost-broadsheet",1,1},
  {"watch","Watch",BF_PAGE_MATERIAL_VIDEO,BF_PAGE_TOPOLOGY_SHELF,"youtube-shelf",1,12},
  {"photos","Photos",BF_PAGE_MATERIAL_PHOTO,BF_PAGE_TOPOLOGY_FIELD,"pixelfed-grid",2,24},
  {"places","Where",BF_PAGE_MATERIAL_PLACE,BF_PAGE_TOPOLOGY_MAP,"maplibre-points",3,50},
  {"latest","Latest",BF_PAGE_MATERIAL_PUBLICATION,BF_PAGE_TOPOLOGY_INDEX,"character",1,12},
  {"stage","",BF_PAGE_MATERIAL_EVENT,BF_PAGE_TOPOLOGY_LEAD,"stage",1,1},
  {"happening","What's on",BF_PAGE_MATERIAL_EVENT,BF_PAGE_TOPOLOGY_AGENDA,"calendar-agenda",1,40},
  {"soon","Starting soon",BF_PAGE_MATERIAL_EVENT,BF_PAGE_TOPOLOGY_AGENDA,"calendar-agenda",1,8},
  {"topics","",BF_PAGE_MATERIAL_PUBLICATION,BF_PAGE_TOPOLOGY_THREAD,"zulip-streams",1,48},
  {"listen","Listen",BF_PAGE_MATERIAL_AUDIO,BF_PAGE_TOPOLOGY_TRACKS,"spotify-tracklist",1,20},
  {"talk","What people are saying",BF_PAGE_MATERIAL_POST,BF_PAGE_TOPOLOGY_THREAD,"zulip-topic",1,20},
  {"more","More",BF_PAGE_MATERIAL_PUBLICATION,BF_PAGE_TOPOLOGY_LIST,"ghost-wire",1,30},
  {"feed","",BF_PAGE_MATERIAL_MIX,BF_PAGE_TOPOLOGY_FEED,"pinterest-home-feed",1,50},
  {"wall","",BF_PAGE_MATERIAL_PHOTO,BF_PAGE_TOPOLOGY_WALL,"pinry-masonry",1,50},
  {"grid","",BF_PAGE_MATERIAL_VIDEO,BF_PAGE_TOPOLOGY_GRID,"youtube-browse-grid",1,50},
  {"communities","",BF_PAGE_MATERIAL_PUBLICATION,BF_PAGE_TOPOLOGY_COMMUNITIES,"lemmy-community-list",1,50},
  {"table","",BF_PAGE_MATERIAL_PUBLICATION,BF_PAGE_TOPOLOGY_TABLE,"linear-issue-list",1,50},
  {"results","",BF_PAGE_MATERIAL_MIX,BF_PAGE_TOPOLOGY_RESULTS,"search-results",1,50},
  {"playlist","",BF_PAGE_MATERIAL_AUDIO,BF_PAGE_TOPOLOGY_TRACKS,"spotify-tracklist",1,50},
  {"parts","In the piece",BF_PAGE_MATERIAL_PASSAGE,BF_PAGE_TOPOLOGY_INDEX,"ghost-table-of-contents",1,12},
  {"named","Named in it",BF_PAGE_MATERIAL_ORGANIZATION,BF_PAGE_TOPOLOGY_INDEX,"wikipedia-infobox-links",1,24},
  {"channels","Where they publish",BF_PAGE_MATERIAL_CHANNEL,BF_PAGE_TOPOLOGY_TABLE,"linear-issue-list",1,12},
};
#define BF_PAGE_REGION_COUNT (sizeof(bf_page_regions)/sizeof(bf_page_regions[0]))
static const BfPageSubjectScheme bf_page_subject_schemes[] = {
  {"community",BF_PAGE_MATERIAL_POST},
  {"recording",BF_PAGE_MATERIAL_AUDIO},
  {"program",BF_PAGE_MATERIAL_VIDEO},
};
#define BF_PAGE_SUBJECT_SCHEME_COUNT (sizeof(bf_page_subject_schemes)/sizeof(bf_page_subject_schemes[0]))
static const BfPageSourceKind bf_page_source_kinds[] = {
  {"video",BF_PAGE_MATERIAL_VIDEO},
  {"image",BF_PAGE_MATERIAL_PHOTO},
  {"audio",BF_PAGE_MATERIAL_AUDIO},
  {"event",BF_PAGE_MATERIAL_EVENT},
};
#define BF_PAGE_SOURCE_KIND_COUNT (sizeof(bf_page_source_kinds)/sizeof(bf_page_source_kinds[0]))
typedef struct { const char *route; const char *territory; const char *host_suffix; const char *const *title_terms; size_t title_term_count; } BfPageRouteSourceContract;
static const char *const bf_page_route_source_contract_0_title_terms[] = {NULL};
static const char *const bf_page_route_source_contract_1_title_terms[] = {"release","available","launch"};
static const char *const bf_page_route_source_contract_2_title_terms[] = {NULL};
static const char *const bf_page_route_source_contract_3_title_terms[] = {NULL};
static const char *const bf_page_route_source_contract_4_title_terms[] = {NULL};
static const char *const bf_page_route_source_contract_5_title_terms[] = {NULL};
static const char *const bf_page_route_source_contract_6_title_terms[] = {NULL};
static const char *const bf_page_route_source_contract_7_title_terms[] = {NULL};
static const char *const bf_page_route_source_contract_8_title_terms[] = {NULL};
static const char *const bf_page_route_source_contract_9_title_terms[] = {NULL};
static const char *const bf_page_route_source_contract_10_title_terms[] = {NULL};
static const char *const bf_page_route_source_contract_11_title_terms[] = {NULL};
static const BfPageRouteSourceContract bf_page_route_source_contracts[] = {
  {"/releases","any","gamespress.com",bf_page_route_source_contract_0_title_terms,0},
  {"/releases","any","stonemaiergames.com",bf_page_route_source_contract_1_title_terms,3},
  {"/advisories","any","threatfilter.dev",bf_page_route_source_contract_2_title_terms,0},
  {"/advisories","any","linuxsecurity.com",bf_page_route_source_contract_3_title_terms,0},
  {"/advisories","any","oss-security.openwall.org",bf_page_route_source_contract_4_title_terms,0},
  {"/advisories","any","mend.io",bf_page_route_source_contract_5_title_terms,0},
  {"/advisories","any","tenable.com",bf_page_route_source_contract_6_title_terms,0},
  {"/","work","threatfilter.dev",bf_page_route_source_contract_7_title_terms,0},
  {"/","work","linuxsecurity.com",bf_page_route_source_contract_8_title_terms,0},
  {"/","work","oss-security.openwall.org",bf_page_route_source_contract_9_title_terms,0},
  {"/","work","mend.io",bf_page_route_source_contract_10_title_terms,0},
  {"/","work","tenable.com",bf_page_route_source_contract_11_title_terms,0},
};
#define BF_PAGE_ROUTE_SOURCE_CONTRACT_COUNT (sizeof(bf_page_route_source_contracts)/sizeof(bf_page_route_source_contracts[0]))
static const BfPageGrammarLabel bf_page_grammar_labels[] = {
  {"discover","Discover"},
  {"stage","Schedule"},
  {"spatial","Map"},
  {"board","Boards"},
  {"community","Community"},
  {"channels","Channels"},
  {"listings","Listings"},
  {"parcels","Properties"},
  {"observations","Updates"},
  {"territory","Area"},
  {"watch","Watch"},
  {"listen","Listen"},
  {"market","Shop"},
  {"inbox","Inbox"},
  {"worlds","Pages"},
  {"packs","Packs"},
  {"link","Accounts"},
  {"studio","Studio"},
  {"console","Console"},
};
#define BF_PAGE_GRAMMAR_LABEL_COUNT (sizeof(bf_page_grammar_labels)/sizeof(bf_page_grammar_labels[0]))
typedef struct { const char *host; const char *path; const char *publisher; } BfPagePublisherHost;
static const BfPagePublisherHost bf_page_publisher_hosts[] = {
  {"hn.algolia.com","","Hacker News"},
  {"hacker-news.firebaseio.com","","Hacker News"},
  {"feeds.bbci.co.uk","","BBC"},
  {"api.wikimedia.org","","Wikipedia"},
  {"wikimedia.org","","Wikipedia"},
  {"public.api.bsky.app","","Bluesky"},
  {"sepiasearch.org","","PeerTube"},
  {"store.steampowered.com","","Steam"},
  {"en.wikipedia.org","","Wikipedia"},
  {"www.wikidata.org","","Wikidata"},
  {"commons.wikimedia.org","","Wikimedia Commons"},
  {"archive.org","","Internet Archive"},
  {"www.inaturalist.org","","iNaturalist"},
  {"api.gbif.org","","iNaturalist via GBIF"},
  {"feeds.podcastindex.org","","Podcasting 2.0"},
  {"qlever.dev","/api/wikidata","Wikidata"},
  {"qlever.dev","/api/wikimedia-commons","Wikimedia Commons"},
};
#define BF_PAGE_PUBLISHER_HOST_COUNT (sizeof(bf_page_publisher_hosts)/sizeof(bf_page_publisher_hosts[0]))
typedef struct { const char *host; const char *path; int hashed; const char *name; const char *page; const char *sized; const char *sizes; int everywhere; } BfPagePictureOrigin;
static const BfPagePictureOrigin bf_page_picture_origins[] = {
  {"upload.wikimedia.org","/wikipedia/commons/",1,"Wikimedia Commons","https://commons.wikimedia.org/wiki/File:","https://commons.wikimedia.org/wiki/Special:FilePath/{name}?width={width}","",0},
  {"thumb.wikimedia.org","/wikipedia/commons/",1,"Wikimedia Commons","https://commons.wikimedia.org/wiki/File:","https://commons.wikimedia.org/wiki/Special:FilePath/{name}?width={width}","",0},
  {"commons.wikimedia.org","/wiki/Special:FilePath/",0,"Wikimedia Commons","https://commons.wikimedia.org/wiki/File:","https://commons.wikimedia.org/wiki/Special:FilePath/{name}?width={width}","",0},
  {"inaturalist-open-data.s3.amazonaws.com","/photos/",0,"iNaturalist","https://www.inaturalist.org/photos/","https://inaturalist-open-data.s3.amazonaws.com/photos/{name}/{size}.{ext}","75:square,240:small,500:medium,1024:large",1},
};
#define BF_PAGE_PICTURE_ORIGIN_COUNT (sizeof(bf_page_picture_origins)/sizeof(bf_page_picture_origins[0]))
typedef struct { const char *host; const char *path; } BfPageWithheldPicture;
static const BfPageWithheldPicture bf_page_withheld_pictures[] = {
  {"upload.wikimedia.org","/wikipedia/en/"},
  {"thumb.wikimedia.org","/wikipedia/en/"},
};
#define BF_PAGE_WITHHELD_PICTURE_COUNT (sizeof(bf_page_withheld_pictures)/sizeof(bf_page_withheld_pictures[0]))
typedef struct { const char *match; const char *words; int commercial; int adapt; int credit; int share_alike; } BfPageLicence;
static const BfPageLicence bf_page_licences[] = {
  {"publicdomain/zero","CC0",1,1,0,0},
  {"cc0","CC0",1,1,0,0},
  {"publicdomain/mark","Public domain",1,1,0,0},
  {"licenses/publicdomain","Public domain",1,1,0,0},
  {"public domain","Public domain",1,1,0,0},
  {"by-nc-nd","CC BY-NC-ND",0,0,1,0},
  {"by-nc-sa","CC BY-NC-SA",0,1,1,1},
  {"by-nc","CC BY-NC",0,1,1,0},
  {"by-nd","CC BY-ND",1,0,1,0},
  {"by-sa","CC BY-SA",1,1,1,1},
  {"licenses/by/","CC BY",1,1,1,0},
  {"cc by ","CC BY",1,1,1,0},
};
#define BF_PAGE_LICENCE_COUNT (sizeof(bf_page_licences)/sizeof(bf_page_licences[0]))
typedef struct { const char *host; const char *path; const char *embed; } BfPageEmbedHost;
static const BfPageEmbedHost bf_page_embed_hosts[] = {
  {"archive.org","/details/","https://archive.org/embed/{id}"},
};
#define BF_PAGE_EMBED_HOST_COUNT (sizeof(bf_page_embed_hosts)/sizeof(bf_page_embed_hosts[0]))
static const unsigned char bf_page_world_regions[] = {0,1,2,3,4,9,10,11};
#define BF_PAGE_WORLD_REGION_COUNT (sizeof(bf_page_world_regions)/sizeof(bf_page_world_regions[0]))
typedef struct { const char *route; const char *territory; const char *title; const char *lede; const char *character; const char *const *families; size_t family_count; unsigned source_limit; const unsigned char *regions; size_t region_count; const char *const *sections; size_t section_count; const char *cannot_infer; } BfPageProgram;
static const char *const bf_page_program_0_families[] = {"local-news"};
static const unsigned char bf_page_program_0_regions[] = {0};
static const char *const bf_page_program_0_sections[] = {"/local","/food","/whats-on","/ideas","/home","/health","/travel","/books"};
static const char *const bf_page_program_1_families[] = {"entertainment"};
static const unsigned char bf_page_program_1_regions[] = {0};
static const char *const bf_page_program_1_sections[] = {"/whats-on","/film","/music","/sports","/art","/watch","/listen","/stage"};
static const char *const bf_page_program_2_families[] = {"shopping"};
static const unsigned char bf_page_program_2_regions[] = {0};
static const char *const bf_page_program_2_sections[] = {"/deals","/picks","/style","/recalls","/retail"};
static const char *const bf_page_program_3_families[] = {"today"};
static const unsigned char bf_page_program_3_regions[] = {0};
static const char *const bf_page_program_3_sections[] = {"/today","/jobs","/business","/money"};
static const char *const bf_page_program_4_families[] = {"deals"};
static const unsigned char bf_page_program_4_regions[] = {16,11};
static const char *const bf_page_program_4_sections[] = {NULL};
static const char *const bf_page_program_5_families[] = {"deals","drops"};
static const unsigned char bf_page_program_5_regions[] = {16,11};
static const char *const bf_page_program_5_sections[] = {NULL};
static const char *const bf_page_program_6_families[] = {"drops"};
static const unsigned char bf_page_program_6_regions[] = {16,11};
static const char *const bf_page_program_6_sections[] = {NULL};
static const char *const bf_page_program_7_families[] = {"jobs"};
static const unsigned char bf_page_program_7_regions[] = {16,11};
static const char *const bf_page_program_7_sections[] = {NULL};
static const char *const bf_page_program_8_families[] = {"shopping","drops"};
static const unsigned char bf_page_program_8_regions[] = {0,13,4,11};
static const char *const bf_page_program_8_sections[] = {NULL};
static const char *const bf_page_program_9_families[] = {"style"};
static const unsigned char bf_page_program_9_regions[] = {0,13,4,11};
static const char *const bf_page_program_9_sections[] = {NULL};
static const char *const bf_page_program_10_families[] = {"recalls"};
static const unsigned char bf_page_program_10_regions[] = {16,11};
static const char *const bf_page_program_10_sections[] = {NULL};
static const char *const bf_page_program_11_families[] = {"retail"};
static const unsigned char bf_page_program_11_regions[] = {16,11};
static const char *const bf_page_program_11_sections[] = {NULL};
static const char *const bf_page_program_12_families[] = {"film","entertainment"};
static const unsigned char bf_page_program_12_regions[] = {0,13,4,11};
static const char *const bf_page_program_12_sections[] = {NULL};
static const char *const bf_page_program_13_families[] = {"music"};
static const unsigned char bf_page_program_13_regions[] = {0,13,4,11};
static const char *const bf_page_program_13_sections[] = {NULL};
static const char *const bf_page_program_14_families[] = {"sports"};
static const unsigned char bf_page_program_14_regions[] = {0,4,11};
static const char *const bf_page_program_14_sections[] = {NULL};
static const char *const bf_page_program_15_families[] = {"art"};
static const unsigned char bf_page_program_15_regions[] = {13,4,11};
static const char *const bf_page_program_15_sections[] = {NULL};
static const char *const bf_page_program_16_families[] = {"local-news","going-out"};
static const unsigned char bf_page_program_16_regions[] = {0,4,13,11};
static const char *const bf_page_program_16_sections[] = {NULL};
static const char *const bf_page_program_17_families[] = {"food"};
static const unsigned char bf_page_program_17_regions[] = {0,13,4,11};
static const char *const bf_page_program_17_sections[] = {NULL};
static const char *const bf_page_program_18_families[] = {"culture"};
static const unsigned char bf_page_program_18_regions[] = {0,4,11};
static const char *const bf_page_program_18_sections[] = {NULL};
static const char *const bf_page_program_19_families[] = {"home"};
static const unsigned char bf_page_program_19_regions[] = {13,4,11};
static const char *const bf_page_program_19_sections[] = {NULL};
static const char *const bf_page_program_20_families[] = {"health"};
static const unsigned char bf_page_program_20_regions[] = {4,11};
static const char *const bf_page_program_20_sections[] = {NULL};
static const char *const bf_page_program_21_families[] = {"travel"};
static const unsigned char bf_page_program_21_regions[] = {0,13,4,11};
static const char *const bf_page_program_21_sections[] = {NULL};
static const char *const bf_page_program_22_families[] = {"books"};
static const unsigned char bf_page_program_22_regions[] = {4,13,11};
static const char *const bf_page_program_22_sections[] = {NULL};
static const char *const bf_page_program_23_families[] = {"today"};
static const unsigned char bf_page_program_23_regions[] = {0,4,11};
static const char *const bf_page_program_23_sections[] = {NULL};
static const char *const bf_page_program_24_families[] = {"jobs"};
static const unsigned char bf_page_program_24_regions[] = {16,11};
static const char *const bf_page_program_24_sections[] = {NULL};
static const char *const bf_page_program_25_families[] = {"small-business","retail"};
static const unsigned char bf_page_program_25_regions[] = {0,4,11};
static const char *const bf_page_program_25_sections[] = {NULL};
static const char *const bf_page_program_26_families[] = {"money"};
static const unsigned char bf_page_program_26_regions[] = {4,11};
static const char *const bf_page_program_26_sections[] = {NULL};
static const char *const bf_page_program_27_families[] = {"news","attention","wikimedia","books","space","history","institutions","datasets","live-music","commerce","gaming","vulnerabilities","food","science","sports"};
static const unsigned char bf_page_program_27_regions[] = {0,13,1,9,8,4,16,11,12};
static const char *const bf_page_program_27_sections[] = {NULL};
static const char *const bf_page_program_28_families[] = {"video","films"};
static const unsigned char bf_page_program_28_regions[] = {14,1,13,4,11};
static const char *const bf_page_program_28_sections[] = {NULL};
static const char *const bf_page_program_29_families[] = {"news","attention","podcasts","commons"};
static const unsigned char bf_page_program_29_regions[] = {4};
static const char *const bf_page_program_29_sections[] = {NULL};
static const char *const bf_page_program_30_families[] = {"news","institutions","commons"};
static const unsigned char bf_page_program_30_regions[] = {4};
static const char *const bf_page_program_30_sections[] = {NULL};
static const char *const bf_page_program_31_families[] = {"forums","hacker-attention"};
static const unsigned char bf_page_program_31_regions[] = {8,4,11,12};
static const char *const bf_page_program_31_sections[] = {NULL};
static const char *const bf_page_program_32_families[] = {"fediverse","atproto"};
static const unsigned char bf_page_program_32_regions[] = {15,10,4,11,12};
static const char *const bf_page_program_32_sections[] = {NULL};
static const char *const bf_page_program_33_families[] = {"commons","museums","images"};
static const unsigned char bf_page_program_33_regions[] = {13,2,14,4,11};
static const char *const bf_page_program_33_sections[] = {NULL};
static const char *const bf_page_program_34_families[] = {"hazards","nature","mobility"};
static const unsigned char bf_page_program_34_regions[] = {3,2,4,11};
static const char *const bf_page_program_34_sections[] = {NULL};
static const char *const bf_page_program_35_families[] = {"packages","ai-models","gaming","github"};
static const unsigned char bf_page_program_35_regions[] = {16,4,11,12};
static const char *const bf_page_program_35_sections[] = {NULL};
static const char *const bf_page_program_36_families[] = {"vulnerabilities","security","standards","rulemaking","recalls"};
static const unsigned char bf_page_program_36_regions[] = {0,16,4,11};
static const char *const bf_page_program_36_sections[] = {NULL};
static const char *const bf_page_program_37_families[] = {"literature","government-spending","local-government"};
static const unsigned char bf_page_program_37_regions[] = {4,16,11,12};
static const char *const bf_page_program_37_sections[] = {NULL};
static const char *const bf_page_program_38_families[] = {"music","live-music","podcasts"};
static const unsigned char bf_page_program_38_regions[] = {18,9,13,4,11};
static const char *const bf_page_program_38_sections[] = {NULL};
static const char *const bf_page_program_39_families[] = {"podcasts"};
static const unsigned char bf_page_program_39_regions[] = {18,9,4,11};
static const char *const bf_page_program_39_sections[] = {NULL};
static const char *const bf_page_program_40_families[] = {"history","commons","attention"};
static const unsigned char bf_page_program_40_regions[] = {0,2,13,4,11};
static const char *const bf_page_program_40_sections[] = {NULL};
static const char *const bf_page_program_41_families[] = {"events"};
static const unsigned char bf_page_program_41_regions[] = {7,6,2,4,11};
static const char *const bf_page_program_41_sections[] = {NULL};
static const char *const bf_page_program_42_families[] = {"events"};
static const unsigned char bf_page_program_42_regions[] = {5,6,7};
static const char *const bf_page_program_42_sections[] = {NULL};
static const char *const bf_page_program_43_families[] = {"places","events","local-voices","local-photos","local-nature"};
static const unsigned char bf_page_program_43_regions[] = {3,7,2,13,4,11,12};
static const char *const bf_page_program_43_sections[] = {NULL};
static const char *const bf_page_program_44_families[] = {"news","attention","wikimedia","books","space","video","forums","hacker-attention","fediverse","atproto","museums","images","hazards","nature","packages","ai-models","gaming","vulnerabilities","security","literature","history","films","music","commons","places","local-photos","live-music","podcasts","local-nature","local-voices","institutions","events","food","science","sports","recalls","rulemaking"};
static const unsigned char bf_page_program_44_regions[] = {17};
static const char *const bf_page_program_44_sections[] = {NULL};
static const BfPageProgram bf_page_programs[] = {
  {"/","life","Today","Dallas, food, home and the ideas worth your evening.","scrapbook",bf_page_program_0_families,1,1,bf_page_program_0_regions,1,bf_page_program_0_sections,8,"a front page is an arrangement by time, not a ranking of importance or an endorsement"},
  {"/","live","Live","What's on, and what everyone is watching, hearing and playing.","journal",bf_page_program_1_families,1,1,bf_page_program_1_regions,1,bf_page_program_1_sections,8,"a report shown here is what its source published, not a warning, advice or a verified event"},
  {"/","shop","Shop","Deals worth your time, things worth buying, and what has been recalled.","ticker",bf_page_program_2_families,1,1,bf_page_program_2_regions,1,bf_page_program_2_sections,5,"a release listed here is not an offer, a price, a licence or a recommendation to buy"},
  {"/","work","Work","Jobs, running a business, money, and the day's news that touches them.","dossier",bf_page_program_3_families,1,1,bf_page_program_3_regions,1,bf_page_program_3_sections,4,"an advisory or rule listed here does not say whether it applies to you; nothing here is legal, security or financial advice"},
  {"/deals","any","Deals","Prices worth acting on, as the deal hunters post them.","ticker",bf_page_program_4_families,1,8,bf_page_program_4_regions,2,bf_page_program_4_sections,0,"a deal is the store's price when posted"},
  {"/market","shop","Market","Everything listed for sale across the shop, with the price each listing states.","ticker",bf_page_program_5_families,2,12,bf_page_program_5_regions,2,bf_page_program_5_sections,0,"a listed price is the seller's when posted, not a quote, a stock check or a recommendation"},
  {"/listings","shop","New listings","Releases, launches and drops as the stores list them.","ticker",bf_page_program_6_families,1,12,bf_page_program_6_regions,2,bf_page_program_6_sections,0,"a listing is the store's; it is not stock, a reservation or a price promise"},
  {"/listings","work","Listings","Open roles and contracts, as the people hiring list them.","dossier",bf_page_program_7_families,1,12,bf_page_program_7_regions,2,bf_page_program_7_sections,0,"a listing is the employer's; it is not an offer of work"},
  {"/picks","any","Worth buying","What reviewers and editors think is worth the money.","scrapbook",bf_page_program_8_families,2,12,bf_page_program_8_regions,4,bf_page_program_8_sections,0,"a pick is the publisher's opinion; a drop is the store's listing"},
  {"/style","any","Style","What people are wearing and buying.","scrapbook",bf_page_program_9_families,1,8,bf_page_program_9_regions,4,bf_page_program_9_sections,0,"style coverage is the publisher's"},
  {"/recalls","any","Recalls","Products and food pulled from shelves.","dossier",bf_page_program_10_families,1,8,bf_page_program_10_regions,2,bf_page_program_10_sections,0,"a recall is as the agency announced it"},
  {"/retail","any","Retail","How stores and brands are changing.","journal",bf_page_program_11_families,1,8,bf_page_program_11_regions,2,bf_page_program_11_sections,0,"retail news is the publisher's"},
  {"/film","any","Film & TV","What is opening, streaming and worth watching.","journal",bf_page_program_12_families,2,12,bf_page_program_12_regions,4,bf_page_program_12_sections,0,"coverage is the publisher's"},
  {"/music","any","Music","New records, tours and the people making them.","journal",bf_page_program_13_families,1,12,bf_page_program_13_regions,4,bf_page_program_13_sections,0,"coverage is the publisher's"},
  {"/sports","any","Sports","Scores, trades and the stories behind them.","ticker",bf_page_program_14_families,1,12,bf_page_program_14_regions,3,bf_page_program_14_sections,0,"coverage is the publisher's"},
  {"/art","any","Art","Exhibitions, artists and work worth seeing.","scrapbook",bf_page_program_15_families,1,8,bf_page_program_15_regions,3,bf_page_program_15_sections,0,"coverage is the publisher's"},
  {"/local","any","Dallas","What is happening in Dallas and across Texas, and where to go.","journal",bf_page_program_16_families,2,12,bf_page_program_16_regions,4,bf_page_program_16_sections,0,"local coverage is the publisher's"},
  {"/food","any","Food","Something to cook tonight, and where to eat.","scrapbook",bf_page_program_17_families,1,12,bf_page_program_17_regions,4,bf_page_program_17_sections,0,"a recipe is its creator's"},
  {"/ideas","any","Ideas","The essays and arguments people are reading.","journal",bf_page_program_18_families,1,8,bf_page_program_18_regions,3,bf_page_program_18_sections,0,"an essay is its author's"},
  {"/home","any","Home","Rooms, gardens and making a place your own.","scrapbook",bf_page_program_19_families,1,8,bf_page_program_19_regions,3,bf_page_program_19_sections,0,"coverage is the publisher's"},
  {"/health","any","Health","Living well, explained.","journal",bf_page_program_20_families,1,8,bf_page_program_20_regions,2,bf_page_program_20_sections,0,"health coverage is not medical advice"},
  {"/travel","any","Travel","Places worth the trip.","scrapbook",bf_page_program_21_families,1,8,bf_page_program_21_regions,4,bf_page_program_21_sections,0,"coverage is the publisher's"},
  {"/books","any","Books","What to read next.","journal",bf_page_program_22_families,1,8,bf_page_program_22_regions,3,bf_page_program_22_sections,0,"a review is its critic's"},
  {"/today","any","The day's news","What happened today, briefly.","ticker",bf_page_program_23_families,1,8,bf_page_program_23_regions,3,bf_page_program_23_sections,0,"news is the publisher's"},
  {"/jobs","any","Jobs","Open roles you can apply for now.","dossier",bf_page_program_24_families,1,8,bf_page_program_24_regions,2,bf_page_program_24_sections,0,"a listing is the employer's"},
  {"/business","any","Small business","Starting, running and growing one.","journal",bf_page_program_25_families,2,12,bf_page_program_25_regions,3,bf_page_program_25_sections,0,"coverage is the publisher's"},
  {"/money","any","Money","Saving, spending and what it costs.","journal",bf_page_program_26_families,1,8,bf_page_program_26_regions,2,bf_page_program_26_sections,0,"money coverage is not financial advice"},
  {"/discover","any","Discover","Stories, the day's most-read pages, books and the sky, in one stream as they arrive.","scrapbook",bf_page_program_27_families,15,8,bf_page_program_27_regions,9,bf_page_program_27_sections,0,"a story shown here is not endorsed, verified or ranked by importance; order is by time"},
  {"/watch","any","Watch","Video published on people's own servers, and films old enough to belong to everyone, playing right here.","journal",bf_page_program_28_families,2,64,bf_page_program_28_regions,5,bf_page_program_28_sections,0,"a video listed here is not hosted, licensed or endorsed by this estate; it plays from its own server"},
  {"/people","any","People","Everyone this estate has read: the names publishers printed beside the things they published, and what else each one made.","dossier",bf_page_program_29_families,4,8,bf_page_program_29_regions,1,bf_page_program_29_sections,0,"a name here is what a publisher printed, never an account on this estate, a verified identity, or proof that two identical names are one person"},
  {"/organisations","any","Institutions","The publishers whose channels this estate actually reads, and how much of each one it has taken in.","dossier",bf_page_program_30_families,3,8,bf_page_program_30_regions,1,bf_page_program_30_sections,0,"being listed is this estate's own decision to read a publisher; it is never a relationship either party agreed to, nor an endorsement"},
  {"/channels","any","Channels","Public threads, grouped by where they are happening. Pick a place to read its topics.","journal",bf_page_program_31_families,2,64,bf_page_program_31_regions,4,bf_page_program_31_sections,0,"a public thread is not local speech, membership or moderation by this estate"},
  {"/community","any","Community","People and communities posting in the open, gathered by who is posting.","scrapbook",bf_page_program_32_families,2,64,bf_page_program_32_regions,5,bf_page_program_32_sections,0,"a post from another community is not a post in this one"},
  {"/board","any","Boards","Pictures from museums, agencies and the photographers of Wikimedia Commons, pinned wall to wall. Open one to see it large and to use it.","scrapbook",bf_page_program_33_families,3,96,bf_page_program_33_regions,5,bf_page_program_33_sections,0,"a picture shown here is not a copy, a licence or a claim about who is depicted"},
  {"/parcels","any","Map","Where things are being reported: earthquakes, species sightings and bike networks, placed where their sources say.","atlas",bf_page_program_34_families,3,16,bf_page_program_34_regions,4,bf_page_program_34_sections,0,"a point is where a source placed a report, not a verified location, a warning or advice"},
  {"/releases","any","Releases","What registries, studios and model hubs just published, one line each.","ticker",bf_page_program_35_families,4,20,bf_page_program_35_regions,4,bf_page_program_35_sections,0,"a release listed here is not reviewed, safe to install or endorsed"},
  {"/advisories","any","Advisories","Vulnerabilities, security writing, standards and rules as they were filed.","dossier",bf_page_program_36_families,5,16,bf_page_program_36_regions,4,bf_page_program_36_sections,0,"an advisory does not say whether you are affected; nothing here is security or legal advice"},
  {"/research","any","Research","Papers, preprints and datasets registered in the last hours, with where each can be cited from.","journal",bf_page_program_37_families,3,18,bf_page_program_37_regions,4,bf_page_program_37_sections,0,"a preprint listed here is not peer reviewed unless its index says so"},
  {"/listen","any","Listen","Music anyone may hear and recordings the area's own publishers put out — symphonies, 78 rpm blues, concerts taped in Dallas, talk and reporting — playing here one after another, with where each is kept.","journal",bf_page_program_38_families,3,96,bf_page_program_38_regions,5,bf_page_program_38_sections,0,"a recording listed here is not hosted, licensed or endorsed by this estate; it plays from Wikimedia Commons under the terms its own page states"},
  {"/podcasts","any","Podcasts","Shows that publish their chapters, transcripts and guests, playing right here.","journal",bf_page_program_39_families,1,16,bf_page_program_39_regions,4,bf_page_program_39_sections,0,"an episode is its show's own statement; a guest named or a place mentioned is not verified here"},
  {"/on-this-day","any","On this day","The article Wikipedia's editors chose for today, the picture of the day, what happened on this date in other years, and what people are reading.","almanac",bf_page_program_40_families,3,12,bf_page_program_40_regions,5,bf_page_program_40_sections,0,"an event listed for this date is what an encyclopedia records, not a verified account; the picture of the day is Wikimedia's choice, not this estate's"},
  {"/whats-on","any","What's on","Events the organizers themselves publish: what starts soon, what is on today, and what is coming, with where each one is.","almanac",bf_page_program_41_families,1,96,bf_page_program_41_regions,5,bf_page_program_41_sections,0,"an event is listed as its organizer published it: a time, a place or a ticket may have changed since, and nothing here is a reservation"},
  {"/stage","any","Stage","What is happening around this event now: where it is and what its organizers have published.","journal",bf_page_program_42_families,1,96,bf_page_program_42_regions,3,bf_page_program_42_sections,0,"a stage is a live composition around organizer-published events, not proof of attendance, capacity, availability, a reservation or an invitation"},
  {"/nearby","any","Around Fort Worth and Dallas","Landmarks, neighbourhoods and the month's wildlife sightings on the map, the best photographs of the two cities, and what their cities, schools and institutions are saying.","atlas",bf_page_program_43_families,5,96,bf_page_program_43_regions,7,bf_page_program_43_sections,0,"a place on this map is where Wikidata or a photographer placed it, not a survey, a boundary, a property record or a recommendation to visit"},
  {"/search","any","Search","Everything Aurekai is reading right now, searched by what it says and who made it.","journal",bf_page_program_44_families,37,64,bf_page_program_44_regions,1,bf_page_program_44_sections,0,"a search result is a match on retained public text, not a ranking by truth or importance"},
};
#define BF_PAGE_PROGRAM_COUNT (sizeof(bf_page_programs)/sizeof(bf_page_programs[0]))
static const char *const bf_page_home_phase_0_sections[] = {"/local","/food","/health","/whats-on","/ideas","/home","/travel","/books"};
static const char *const bf_page_home_phase_1_sections[] = {"/local","/food","/whats-on","/ideas","/home","/travel","/books","/health"};
static const char *const bf_page_home_phase_2_sections[] = {"/whats-on","/food","/local","/ideas","/books","/home","/travel","/health"};
static const char *const bf_page_home_phase_3_sections[] = {"/ideas","/books","/food","/local","/travel","/home","/health","/whats-on"};
typedef struct { const char *territory; unsigned from_hour; const char *title; const char *lede; const char *const *sections; unsigned section_count; } BfPageHomePhase;
static const BfPageHomePhase bf_page_home_phases[] = {
  {"life",5,"This morning","Dallas this morning, something to cook, and what to read.",bf_page_home_phase_0_sections,8},
  {"life",11,"Today","Pictures, video, stories and the conversations around them, from the people and institutions publishing today.",bf_page_home_phase_1_sections,8},
  {"life",17,"Tonight","What is on tonight, what to cook, and what Dallas is talking about.",bf_page_home_phase_2_sections,8},
  {"life",23,"Still open","What is still happening at this hour, what plays without anyone else awake, and what changed while the city went quiet.",bf_page_home_phase_3_sections,8},
};
#define BF_PAGE_HOME_PHASE_COUNT (sizeof(bf_page_home_phases)/sizeof(bf_page_home_phases[0]))
typedef struct { const char *name; unsigned region; BfPageMaterial material; unsigned at_least; unsigned take; unsigned weight; const char *label; const char *says; } BfPageSceneGrammar;
static const BfPageSceneGrammar bf_page_scene_grammars[] = {
  {"stage",5,BF_PAGE_MATERIAL_EVENT,1,1,95,"","one thing is happening now, so it leads"},
  {"agenda",6,BF_PAGE_MATERIAL_EVENT,2,24,90,"What's on","several things happen at stated times, which is a day to read forwards"},
  {"map",3,BF_PAGE_MATERIAL_PLACE,3,40,80,"Where","several things carry a point, which is a place to see at once"},
  {"field",2,BF_PAGE_MATERIAL_PHOTO,6,24,72,"Pictures","many pictures of one subject read as a field seen at once"},
  {"wall",13,BF_PAGE_MATERIAL_PHOTO,1,12,66,"Pictures","even one picture of a subject belongs beside its words"},
  {"tracklist",18,BF_PAGE_MATERIAL_AUDIO,2,20,75,"Listen","recordings that play belong in a list you can play from"},
  {"shelf",1,BF_PAGE_MATERIAL_VIDEO,2,12,65,"Watch","video with pictures reads as a shelf"},
  {"threads",8,BF_PAGE_MATERIAL_POST,3,30,55,"Talk","public conversation is threads, not headlines"},
  {"broadsheet",0,BF_PAGE_MATERIAL_PUBLICATION,1,1,50,"","the newest thing published about it leads when nothing is happening"},
  {"index",4,BF_PAGE_MATERIAL_PUBLICATION,2,12,45,"Latest","what has been published about it, newest first"},
  {"more",11,BF_PAGE_MATERIAL_PUBLICATION,4,24,20,"More","everything else it has, compactly"},
  {"contents",19,BF_PAGE_MATERIAL_PASSAGE,2,12,62,"In the piece","a piece its publisher divided into parts reads as those parts"},
  {"cast",20,BF_PAGE_MATERIAL_ORGANIZATION,2,24,40,"Named in it","a piece that names several organizations is read beside who they are"},
  {"masthead",21,BF_PAGE_MATERIAL_CHANNEL,1,12,30,"Where they publish","who published it, and every way they put things out, is part of reading it"},
};
#define BF_PAGE_SCENE_GRAMMAR_COUNT (sizeof(bf_page_scene_grammars)/sizeof(bf_page_scene_grammars[0]))
typedef struct { const char *route; const char *label; } BfPageNavItem;
static const BfPageNavItem bf_page_nav[] = {
  {"/discover","Discover"},
  {"/whats-on","What's on"},
  {"/watch","Watch"},
  {"/listen","Listen"},
  {"/board","Collections"},
};
#define BF_PAGE_NAV_COUNT (sizeof(bf_page_nav)/sizeof(bf_page_nav[0]))
#endif
