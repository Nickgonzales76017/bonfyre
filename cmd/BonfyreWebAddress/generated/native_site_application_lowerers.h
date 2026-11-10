/* Generated from site-application-lowerers.cue. Do not edit. */
#ifndef BONFYRE_NATIVE_SITE_APPLICATION_LOWERERS_H
#define BONFYRE_NATIVE_SITE_APPLICATION_LOWERERS_H
#include <stddef.h>
typedef enum { BF_SITE_LAYOUT_RIVER, BF_SITE_LAYOUT_STATE, BF_SITE_LAYOUT_THREAD, BF_SITE_LAYOUT_BOARD, BF_SITE_LAYOUT_AUDIO, BF_SITE_LAYOUT_VIDEO } BfSiteApplicationLayout;
typedef enum { BF_SITE_OP_HEADER, BF_SITE_OP_LEAD, BF_SITE_OP_STREAM, BF_SITE_OP_THREAD, BF_SITE_OP_BOARD, BF_SITE_OP_MEDIA, BF_SITE_OP_TRANSCRIPT } BfSiteApplicationOpcode;
typedef enum { BF_SITE_MATERIAL_READER_PUBLICATION, BF_SITE_MATERIAL_READER_NONE, BF_SITE_MATERIAL_READER_COMMUNITY, BF_SITE_MATERIAL_READER_WORLD_FRAGMENT, BF_SITE_MATERIAL_READER_RECORDING, BF_SITE_MATERIAL_READER_MEDIA_PROGRAM } BfSiteMaterialReader;
typedef struct { const char *application_kind,*presentation_class,*material_owner,*query_key,*style_asset,*empty_character; BfSiteMaterialReader reader; BfSiteApplicationLayout layout; const BfSiteApplicationOpcode *ops; size_t op_count; } BfSiteApplicationLowerer;
static const BfSiteApplicationOpcode bf_site_application_PublicationFeed_ops[] = {BF_SITE_OP_HEADER,BF_SITE_OP_LEAD,BF_SITE_OP_STREAM};
static const BfSiteApplicationOpcode bf_site_application_Newsroom_ops[] = {BF_SITE_OP_HEADER,BF_SITE_OP_LEAD,BF_SITE_OP_STREAM};
static const BfSiteApplicationOpcode bf_site_application_Weather_ops[] = {};
static const BfSiteApplicationOpcode bf_site_application_Community_ops[] = {BF_SITE_OP_HEADER,BF_SITE_OP_THREAD};
static const BfSiteApplicationOpcode bf_site_application_Board_ops[] = {BF_SITE_OP_HEADER,BF_SITE_OP_BOARD};
static const BfSiteApplicationOpcode bf_site_application_Listen_ops[] = {BF_SITE_OP_HEADER,BF_SITE_OP_MEDIA,BF_SITE_OP_TRANSCRIPT};
static const BfSiteApplicationOpcode bf_site_application_Watch_ops[] = {BF_SITE_OP_HEADER,BF_SITE_OP_MEDIA,BF_SITE_OP_TRANSCRIPT};
static const BfSiteApplicationLowerer bf_site_application_lowerers[] = {
  {"PublicationFeed","publication","PublicationOccurrence","source_collection","/assets/donor-pixelfed.css","No admitted publication is available for this part of the World.",BF_SITE_MATERIAL_READER_PUBLICATION,0,bf_site_application_PublicationFeed_ops,3},
  {"Newsroom","publication","PublicationOccurrence","source_collection","/assets/donor-pixelfed.css","No admitted reporting is available for this part of the World.",BF_SITE_MATERIAL_READER_PUBLICATION,0,bf_site_application_Newsroom_ops,3},
  {"Weather","state","WeatherObservation","none",NULL,"No admitted weather observation is available for this part of the World.",BF_SITE_MATERIAL_READER_NONE,1,bf_site_application_Weather_ops,0},
  {"Community","community","HumanCommunityBody","community","/assets/donor-lemmy.css","No published human community body is available for this part of the World.",BF_SITE_MATERIAL_READER_COMMUNITY,2,bf_site_application_Community_ops,2},
  {"Board","board","WorldFragment","world","/assets/donor-pinry.css","No published Board arrangement is available for this World.",BF_SITE_MATERIAL_READER_WORLD_FRAGMENT,3,bf_site_application_Board_ops,2},
  {"Listen","listen","MusicRecording","recording","/assets/donor-pixelfed.css","No published recording is available for listening in this part of the World.",BF_SITE_MATERIAL_READER_RECORDING,4,bf_site_application_Listen_ops,3},
  {"Watch","watch","MediaProgram","program","/assets/donor-pixelfed.css","No published video program is available for this part of the World.",BF_SITE_MATERIAL_READER_MEDIA_PROGRAM,5,bf_site_application_Watch_ops,3},
};
#define BF_SITE_APPLICATION_LOWERER_COUNT (sizeof(bf_site_application_lowerers)/sizeof(bf_site_application_lowerers[0]))
#endif
