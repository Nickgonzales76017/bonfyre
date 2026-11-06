/* Generated from schemas/control/native-surface-programs.cue. Do not edit. */
#ifndef BONFYRE_NATIVE_SURFACE_PROGRAMS_H
#define BONFYRE_NATIVE_SURFACE_PROGRAMS_H

typedef enum { BF_SURFACE_TEXT, BF_SURFACE_DATE, BF_SURFACE_TERRITORY_NAV, BF_SURFACE_WORLD_NAV, BF_SURFACE_WORLD_LINK, BF_SURFACE_WORLD_FRAGMENTS, BF_SURFACE_COMMUNITY_POSTS, BF_SURFACE_COMMUNITY_TITLE, BF_SURFACE_COMMUNITY_LEDE, BF_SURFACE_COMMUNITY_STANDING, BF_SURFACE_COMMUNITY_LIMIT, BF_SURFACE_OFFER_STATUS, BF_SURFACE_OFFER_EMPTY, BF_SURFACE_NIGHT_SUBJECTS, BF_SURFACE_PUBLICATIONS, BF_SURFACE_PACK_DEMANDS, BF_SURFACE_CAPABILITY_PROGRAMS, BF_SURFACE_WORLD_CATALOGUE, BF_SURFACE_SUBJECT_APERTURES } BfSurfaceOpcode;
typedef enum { BF_SURFACE_NAV_PLAIN, BF_SURFACE_NAV_STREAM, BF_SURFACE_NAV_COMMUNITY, BF_SURFACE_NAV_BOARD, BF_SURFACE_NAV_MARKET } BfSurfaceNavPresentation;
typedef enum { BF_SURFACE_LINK_STREAM, BF_SURFACE_LINK_STAGE, BF_SURFACE_LINK_COMMUNITY, BF_SURFACE_LINK_BOARD, BF_SURFACE_LINK_MARKET } BfSurfaceLinkPresentation;
typedef enum { BF_SURFACE_SUBJECT_STREAM, BF_SURFACE_SUBJECT_STAGE } BfSurfaceSubjectPresentation;
typedef enum { BF_SURFACE_DOCUMENT_SLOTS, BF_SURFACE_DOCUMENT_NATIVE } BfSurfaceDocumentMode;
typedef struct { const char *renderer; BfSurfaceNavPresentation territory_nav,world_nav; BfSurfaceLinkPresentation world_link; BfSurfaceSubjectPresentation subjects; } BfSurfaceRendererPresentation;
typedef struct { BfSurfaceOpcode opcode; const char *text,*cut; } BfSurfaceProgramOp;
typedef struct { unsigned short first, count; } BfSurfaceProgramSlot;
typedef struct { const char *program,*route,*template_name; unsigned char world_route,world_navigation; BfSurfaceDocumentMode document_mode; const BfSurfaceRendererPresentation *presentation; const BfSurfaceProgramOp *ops; const BfSurfaceProgramSlot *slots; unsigned short slot_count; } BfNativeSurfaceProgram;

static const BfSurfaceRendererPresentation bf_surface_renderer_presentations[] = {
  {"pixelfed_stream",1,1,0,0},
  {"mobilizon_stage",0,0,1,1},
  {"lemmy_community",0,2,2,0},
  {"pinry_board",0,3,3,0},
  {"commerce_market",0,4,4,0},
  {"zulip_channels",0,0,0,0},
  {"maplibre_spatial",0,0,0,0},
  {"frappe_inbox",0,0,0,0},
  {"territory_inspector",0,0,0,0},
  {"observation_stream",1,1,0,0},
  {"world_shelf",0,0,0,0},
  {"pack_graph",0,0,0,0},
  {"link_inspector",0,0,0,0},
  {"studio_workspace",0,0,0,0},
  {"operator_console",0,0,0,0},
};
typedef enum { BF_SURFACE_PRIMARY_SUMMARY, BF_SURFACE_PRIMARY_TONIGHT, BF_SURFACE_PRIMARY_OFFER, BF_SURFACE_PRIMARY_OPERATIONS } BfSurfacePrimaryField;
typedef struct { const char *territory; BfSurfacePrimaryField field; } BfSurfaceTerritoryPrimary;
static const BfSurfaceTerritoryPrimary bf_surface_territory_primary[] = {
  {"life",0},
  {"live",1},
  {"shop",2},
  {"work",3},
  {"independent",0},
};
#define BF_SURFACE_TERRITORY_PRIMARY_COUNT (sizeof(bf_surface_territory_primary)/sizeof(bf_surface_territory_primary[0]))

static const char *const bf_surface_renderer_families[] = {"vertical","stage","thread","board","community","market","search","spatial","work","inspector","watch","listen","library","analysis"};
#define BF_SURFACE_RENDERER_FAMILY_COUNT (sizeof(bf_surface_renderer_families)/sizeof(bf_surface_renderer_families[0]))
static const char *const bf_surface_extents[] = {"chip","card","section","page","workspace","immersive"};
#define BF_SURFACE_EXTENT_COUNT (sizeof(bf_surface_extents)/sizeof(bf_surface_extents[0]))

static const BfSurfaceProgramOp bf_ops_discover[] = {
  {BF_SURFACE_TERRITORY_NAV, NULL, NULL},
  {BF_SURFACE_WORLD_LINK, "Open the page", NULL},
  {BF_SURFACE_WORLD_NAV, NULL, NULL},
  {BF_SURFACE_NIGHT_SUBJECTS, NULL, NULL},
  {BF_SURFACE_PUBLICATIONS, NULL, NULL},
  {BF_SURFACE_SUBJECT_APERTURES, NULL, NULL},
};
static const BfSurfaceProgramSlot bf_slots_discover[] = {
  {0, 1},
  {1, 2},
  {3, 2},
  {5, 1},
};

static const BfSurfaceProgramOp bf_ops_stage[] = {
  {BF_SURFACE_TERRITORY_NAV, NULL, NULL},
  {BF_SURFACE_WORLD_NAV, NULL, NULL},
  {BF_SURFACE_NIGHT_SUBJECTS, NULL, NULL},
  {BF_SURFACE_TEXT, "", NULL},
  {BF_SURFACE_DATE, "weekday", NULL},
  {BF_SURFACE_DATE, "day", NULL},
  {BF_SURFACE_DATE, "month", NULL},
  {BF_SURFACE_DATE, "year", NULL},
  {BF_SURFACE_TEXT, "Tonight in this city", NULL},
  {BF_SURFACE_TEXT, "What is happening around here.", NULL},
  {BF_SURFACE_TEXT, "Notices, not bookings", NULL},
  {BF_SURFACE_WORLD_LINK, "Open the page", NULL},
  {BF_SURFACE_WORLD_NAV, NULL, NULL},
  {BF_SURFACE_TEXT, "No event is on the schedule yet. What follows are notices that have been posted; they are not performances, bookings or attendance.", NULL},
  {BF_SURFACE_TEXT, "A room status, a capacity note, an offer or a safety note is not an event, a ticket or permission to attend.", NULL},
  {BF_SURFACE_TEXT, "Notices", NULL},
  {BF_SURFACE_NIGHT_SUBJECTS, NULL, NULL},
  {BF_SURFACE_TEXT, "The schedule appears once an event is published.", NULL},
  {BF_SURFACE_SUBJECT_APERTURES, NULL, NULL},
};
static const BfSurfaceProgramSlot bf_slots_stage[] = {
  {0, 1},
  {1, 1},
  {2, 1},
  {3, 1},
  {4, 1},
  {5, 1},
  {6, 1},
  {7, 1},
  {8, 1},
  {9, 1},
  {10, 1},
  {11, 2},
  {13, 1},
  {14, 1},
  {15, 1},
  {16, 1},
  {17, 1},
  {18, 1},
};

static const BfSurfaceProgramOp bf_ops_community[] = {
  {BF_SURFACE_WORLD_LINK, "AUREKAI", NULL},
  {BF_SURFACE_WORLD_NAV, NULL, NULL},
  {BF_SURFACE_COMMUNITY_TITLE, "Community", NULL},
  {BF_SURFACE_COMMUNITY_LEDE, "Conversations people have started here. Posts from other networks are not brought in as if they were written here.", NULL},
  {BF_SURFACE_WORLD_LINK, "Open the page", NULL},
  {BF_SURFACE_COMMUNITY_POSTS, NULL, NULL},
  {BF_SURFACE_SUBJECT_APERTURES, NULL, NULL},
  {BF_SURFACE_COMMUNITY_STANDING, NULL, NULL},
  {BF_SURFACE_COMMUNITY_LIMIT, "Posts from other networks are shown on their own pages.", NULL},
};
static const BfSurfaceProgramSlot bf_slots_community[] = {
  {0, 1},
  {1, 1},
  {2, 1},
  {3, 1},
  {4, 1},
  {5, 1},
  {6, 1},
  {7, 1},
  {8, 1},
};

static const BfSurfaceProgramOp bf_ops_board[] = {
  {BF_SURFACE_WORLD_LINK, "AUREKAI / WORLD", NULL},
  {BF_SURFACE_WORLD_NAV, NULL, NULL},
  {BF_SURFACE_WORLD_NAV, NULL, NULL},
  {BF_SURFACE_WORLD_FRAGMENTS, NULL, NULL},
  {BF_SURFACE_SUBJECT_APERTURES, NULL, NULL},
};
static const BfSurfaceProgramSlot bf_slots_board[] = {
  {0, 1},
  {1, 1},
  {2, 1},
  {3, 1},
  {4, 1},
};

static const BfSurfaceProgramOp bf_ops_listings[] = {
  {BF_SURFACE_WORLD_LINK, "AUREKAI", NULL},
  {BF_SURFACE_WORLD_NAV, NULL, NULL},
  {BF_SURFACE_TEXT, "What is on offer here. ", NULL},
  {BF_SURFACE_TEXT, "An offer shows once it is published. ", NULL},
  {BF_SURFACE_TEXT, "A note about capacity or a supplier is not a listing, a price or a reservation.", NULL},
  {BF_SURFACE_WORLD_LINK, "This address", NULL},
  {BF_SURFACE_WORLD_LINK, "Boards", "board"},
  {BF_SURFACE_OFFER_STATUS, NULL, NULL},
  {BF_SURFACE_TEXT, "Offers", NULL},
  {BF_SURFACE_TEXT, "Nothing is on offer here yet.", NULL},
  {BF_SURFACE_OFFER_EMPTY, NULL, NULL},
  {BF_SURFACE_SUBJECT_APERTURES, NULL, NULL},
  {BF_SURFACE_TEXT, "Availability", NULL},
  {BF_SURFACE_TEXT, "Price", NULL},
  {BF_SURFACE_TEXT, "Terms", NULL},
};
static const BfSurfaceProgramSlot bf_slots_listings[] = {
  {0, 1},
  {1, 1},
  {2, 3},
  {5, 2},
  {7, 1},
  {8, 1},
  {9, 1},
  {10, 2},
  {12, 3},
};

static const BfSurfaceProgramOp bf_ops_channels[] = {
  {BF_SURFACE_TERRITORY_NAV, NULL, NULL},
  {BF_SURFACE_WORLD_NAV, NULL, NULL},
  {BF_SURFACE_SUBJECT_APERTURES, NULL, NULL},
  {BF_SURFACE_COMMUNITY_POSTS, NULL, NULL},
};
static const BfSurfaceProgramSlot bf_slots_channels[] = {
  {0, 2},
  {2, 1},
  {3, 1},
};

static const BfSurfaceProgramOp bf_ops_spatial[] = {
  {BF_SURFACE_TERRITORY_NAV, NULL, NULL},
  {BF_SURFACE_WORLD_NAV, NULL, NULL},
  {BF_SURFACE_NIGHT_SUBJECTS, NULL, NULL},
  {BF_SURFACE_SUBJECT_APERTURES, NULL, NULL},
};
static const BfSurfaceProgramSlot bf_slots_spatial[] = {
  {0, 2},
  {2, 1},
  {3, 1},
};

static const BfSurfaceProgramOp bf_ops_inbox[] = {
  {BF_SURFACE_TERRITORY_NAV, NULL, NULL},
  {BF_SURFACE_WORLD_NAV, NULL, NULL},
  {BF_SURFACE_CAPABILITY_PROGRAMS, NULL, NULL},
  {BF_SURFACE_SUBJECT_APERTURES, NULL, NULL},
};
static const BfSurfaceProgramSlot bf_slots_inbox[] = {
  {0, 2},
  {2, 1},
  {3, 1},
};

static const BfSurfaceProgramOp bf_ops_territory[] = {
  {BF_SURFACE_TERRITORY_NAV, NULL, NULL},
  {BF_SURFACE_WORLD_LINK, "Open current World", NULL},
  {BF_SURFACE_SUBJECT_APERTURES, NULL, NULL},
};
static const BfSurfaceProgramSlot bf_slots_territory[] = {
  {0, 2},
  {2, 1},
};

static const BfSurfaceProgramOp bf_ops_observations[] = {
  {BF_SURFACE_TERRITORY_NAV, NULL, NULL},
  {BF_SURFACE_WORLD_NAV, NULL, NULL},
  {BF_SURFACE_PUBLICATIONS, NULL, NULL},
  {BF_SURFACE_SUBJECT_APERTURES, NULL, NULL},
};
static const BfSurfaceProgramSlot bf_slots_observations[] = {
  {0, 2},
  {2, 1},
  {3, 1},
};

static const BfSurfaceProgramOp bf_ops_worlds[] = {
  {BF_SURFACE_TERRITORY_NAV, NULL, NULL},
  {BF_SURFACE_WORLD_CATALOGUE, NULL, NULL},
};
static const BfSurfaceProgramSlot bf_slots_worlds[] = {
  {0, 1},
  {1, 1},
};

static const BfSurfaceProgramOp bf_ops_packs[] = {
  {BF_SURFACE_TERRITORY_NAV, NULL, NULL},
  {BF_SURFACE_WORLD_NAV, NULL, NULL},
  {BF_SURFACE_PACK_DEMANDS, NULL, NULL},
  {BF_SURFACE_SUBJECT_APERTURES, NULL, NULL},
};
static const BfSurfaceProgramSlot bf_slots_packs[] = {
  {0, 2},
  {2, 1},
  {3, 1},
};

static const BfSurfaceProgramOp bf_ops_link[] = {
  {BF_SURFACE_TERRITORY_NAV, NULL, NULL},
  {BF_SURFACE_WORLD_LINK, "Current World", NULL},
  {BF_SURFACE_CAPABILITY_PROGRAMS, NULL, NULL},
  {BF_SURFACE_SUBJECT_APERTURES, NULL, NULL},
};
static const BfSurfaceProgramSlot bf_slots_link[] = {
  {0, 2},
  {2, 1},
  {3, 1},
};

static const BfSurfaceProgramOp bf_ops_studio[] = {
  {BF_SURFACE_TERRITORY_NAV, NULL, NULL},
  {BF_SURFACE_WORLD_CATALOGUE, NULL, NULL},
  {BF_SURFACE_PACK_DEMANDS, NULL, NULL},
  {BF_SURFACE_CAPABILITY_PROGRAMS, NULL, NULL},
  {BF_SURFACE_SUBJECT_APERTURES, NULL, NULL},
};
static const BfSurfaceProgramSlot bf_slots_studio[] = {
  {0, 1},
  {1, 3},
  {4, 1},
};

static const BfSurfaceProgramOp bf_ops_console[] = {
  {BF_SURFACE_TERRITORY_NAV, NULL, NULL},
  {BF_SURFACE_WORLD_CATALOGUE, NULL, NULL},
  {BF_SURFACE_PACK_DEMANDS, NULL, NULL},
  {BF_SURFACE_CAPABILITY_PROGRAMS, NULL, NULL},
  {BF_SURFACE_PUBLICATIONS, NULL, NULL},
  {BF_SURFACE_SUBJECT_APERTURES, NULL, NULL},
};
static const BfSurfaceProgramSlot bf_slots_console[] = {
  {0, 1},
  {1, 4},
  {5, 1},
};

static const BfNativeSurfaceProgram bf_native_surface_programs[] = {
  {"discover","/discover","discover",0,1,0, &bf_surface_renderer_presentations[0], bf_ops_discover, bf_slots_discover, sizeof(bf_slots_discover) / sizeof(bf_slots_discover[0])},
  {"stage","/stage","stage",1,1,0, &bf_surface_renderer_presentations[1], bf_ops_stage, bf_slots_stage, sizeof(bf_slots_stage) / sizeof(bf_slots_stage[0])},
  {"community","/community","community",1,1,0, &bf_surface_renderer_presentations[2], bf_ops_community, bf_slots_community, sizeof(bf_slots_community) / sizeof(bf_slots_community[0])},
  {"board","/board","board",1,1,0, &bf_surface_renderer_presentations[3], bf_ops_board, bf_slots_board, sizeof(bf_slots_board) / sizeof(bf_slots_board[0])},
  {"listings","/listings","listings",1,1,0, &bf_surface_renderer_presentations[4], bf_ops_listings, bf_slots_listings, sizeof(bf_slots_listings) / sizeof(bf_slots_listings[0])},
  {"channels","/channels","channels",1,1,1, &bf_surface_renderer_presentations[5], bf_ops_channels, bf_slots_channels, sizeof(bf_slots_channels) / sizeof(bf_slots_channels[0])},
  {"spatial","/parcels","parcels",1,1,1, &bf_surface_renderer_presentations[6], bf_ops_spatial, bf_slots_spatial, sizeof(bf_slots_spatial) / sizeof(bf_slots_spatial[0])},
  {"inbox","/inbox","inbox",1,1,1, &bf_surface_renderer_presentations[7], bf_ops_inbox, bf_slots_inbox, sizeof(bf_slots_inbox) / sizeof(bf_slots_inbox[0])},
  {"territory","/territory","territory",0,0,1, &bf_surface_renderer_presentations[8], bf_ops_territory, bf_slots_territory, sizeof(bf_slots_territory) / sizeof(bf_slots_territory[0])},
  {"observations","/sources","sources",1,1,1, &bf_surface_renderer_presentations[9], bf_ops_observations, bf_slots_observations, sizeof(bf_slots_observations) / sizeof(bf_slots_observations[0])},
  {"worlds","/w","world",0,0,1, &bf_surface_renderer_presentations[10], bf_ops_worlds, bf_slots_worlds, sizeof(bf_slots_worlds) / sizeof(bf_slots_worlds[0])},
  {"packs","/packs","packs",1,1,1, &bf_surface_renderer_presentations[11], bf_ops_packs, bf_slots_packs, sizeof(bf_slots_packs) / sizeof(bf_slots_packs[0])},
  {"link","/link","link",0,0,1, &bf_surface_renderer_presentations[12], bf_ops_link, bf_slots_link, sizeof(bf_slots_link) / sizeof(bf_slots_link[0])},
  {"studio","/studio","studio",0,0,1, &bf_surface_renderer_presentations[13], bf_ops_studio, bf_slots_studio, sizeof(bf_slots_studio) / sizeof(bf_slots_studio[0])},
  {"console","/console","console",0,0,1, &bf_surface_renderer_presentations[14], bf_ops_console, bf_slots_console, sizeof(bf_slots_console) / sizeof(bf_slots_console[0])},
};
#define BF_NATIVE_SURFACE_PROGRAM_COUNT (sizeof(bf_native_surface_programs) / sizeof(bf_native_surface_programs[0]))
#endif
