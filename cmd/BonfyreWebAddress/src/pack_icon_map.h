#ifndef BONFYRE_PACK_ICON_MAP_H
#define BONFYRE_PACK_ICON_MAP_H
/* Native semantic icon registry for published PackGraph identities.
 * All renderers (WORK, setup, signed-in applications) use this one table.
 * IDs are the CMS canonical pack identities; geometry is licensed Tabler
 * from glyph_set.h, not remote URL, emojis, inline demo assets or CSS icons. */
typedef struct {const char *pack,*glyph;} BfPackIcon;
static const BfPackIcon bf_pack_icons[]={
    {"experience.source-campaign.workspace","share"},
    {"experience.shipment-archive.workspace","receipt"},
    {"experience.connection-studio.workspace","link"},
    {"experience.source-library.workspace","source-library"},
    {"experience.skill-instruments.workspace","tool"},
    {"experience.connected-instruments.workspace","research"},
    {"experience.finance-workbooks.workspace","finance"},
    {"experience.native-creator-studio.workspace","narration"},
    {"experience.source-programs.workspace","calendar"},
    {"experience.multi-merchant-catalogue.workspace","shop"},
    {"experience.retail-procurement.workspace","store"},
    {"experience.itad-disposition-exchange","receipt"},
    {"experience.studio-member-operations","group"},
    {"experience.frappe-enterprise-workbench","apps"},
    {"experience.native-creator-studio","narration"},
    {"experience.multi-merchant-catalogue","shop"},
    {"experience.protocol-commerce-exchange","finance"},
    {"experience.retail-procurement","store"},
    {"experience.acquisition-brief-studio","text"},
    {"experience.foreign-twin-import-clip","feed"},
    {"experience.verification-network","file-check"}
};
#define BF_PACK_ICON_COUNT (sizeof(bf_pack_icons)/sizeof(bf_pack_icons[0]))
static const char *bf_pack_icon(const char *pack,const char *kind){
    if(pack)for(size_t i=0;i<BF_PACK_ICON_COUNT;i++)
        if(!strcmp(pack,bf_pack_icons[i].pack))return bf_pack_icons[i].glyph;
    return kind&&strstr(kind,"Workflow")?"board":"apps";
}
#endif
