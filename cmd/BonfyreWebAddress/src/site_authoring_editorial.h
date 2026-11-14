#ifndef BONFYRE_SITE_AUTHORING_EDITORIAL_H
#define BONFYRE_SITE_AUTHORING_EDITORIAL_H
/* C projection of the exact Editor.js block grammar retained under
 * semantic_tags.authoring_v1.editorial. This emits only escaped, bounded
 * published material. It never treats editor data as raw HTML or effects. */
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
static void bf_aeb_text(FILE *out,const char *s) {
    if(!s)return;
    for(const unsigned char *p=(const unsigned char *)s;*p;p++){
        switch(*p){
        case '&':fputs("&amp;",out);break;
        case '<':fputs("&lt;",out);break;
        case '>':fputs("&gt;",out);break;
        case '"':fputs("&quot;",out);break;
        case 39:fputs("&#39;",out);break;
        default:if(*p>=32||*p=='\n'||*p=='\t')fputc(*p,out);
        }
    }
}
static int bf_aeb_list(FILE *out,sqlite3 *db,const char *items,int ordered){
    if(!items||strlen(items)>100000)return -1;
    sqlite3_stmt *st=NULL;int step,seen=0;
    const char *sql="SELECT CASE WHEN type='object' THEN json_extract(value,'$.text') "
                    "ELSE value END FROM json_each(?1)";
    if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_text(st,1,items,-1,SQLITE_TRANSIENT);
    fputs(ordered?"<ol>":"<ul>",out);
    while((step=sqlite3_step(st))==SQLITE_ROW){
        if(++seen>128){sqlite3_finalize(st);return -1;}
        if(sqlite3_column_type(st,0)!=SQLITE_TEXT){sqlite3_finalize(st);return -1;}
        fputs("<li>",out);bf_aeb_text(out,(const char*)sqlite3_column_text(st,0));fputs("</li>",out);
    }
    fputs(ordered?"</ol>":"</ul>",out);
    sqlite3_finalize(st);return step==SQLITE_DONE?0:-1;
}
static int bf_aeb_table(FILE *out,sqlite3 *db,const char *rows){
    if(!rows||strlen(rows)>100000)return -1;
    sqlite3_stmt *st=NULL;int step,prior=-1,count=0,cols=0;
    const char *sql="SELECT CAST(r.key AS INTEGER),c.value "
                    "FROM json_each(?1) AS r JOIN json_each(r.value) AS c "
                    "ORDER BY CAST(r.key AS INTEGER),CAST(c.key AS INTEGER)";
    if(sqlite3_prepare_v2(db,sql,-1,&st,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_text(st,1,rows,-1,SQLITE_TRANSIENT);fputs("<div class=\"site-authored-table\"><table><tbody>",out);
    while((step=sqlite3_step(st))==SQLITE_ROW){
        int row=sqlite3_column_int(st,0);
        if(row<0||row>=25||++count>300||sqlite3_column_type(st,1)!=SQLITE_TEXT){sqlite3_finalize(st);return -1;}
        if(row!=prior){
            if(prior>=0)fputs("</tr>",out);
            fputs("<tr>",out);prior=row;cols=0;
        }
        if(++cols>12){sqlite3_finalize(st);return -1;}
        fputs("<td>",out);bf_aeb_text(out,(const char*)sqlite3_column_text(st,1));fputs("</td>",out);
    }
    if(prior>=0)fputs("</tr>",out);
    fputs("</tbody></table></div>",out);
    sqlite3_finalize(st);return step==SQLITE_DONE?0:-1;
}
/* 1=rendered exact structured blocks, 0=no blocks, -1=invalid.
 * A failed draft projection must never emit partial HTML. */
static int bf_site_editorial_blocks(FILE *out,sqlite3 *db,const char *tags){
    if(!out||!db||!tags||!*tags)return 0;
    if(strlen(tags)>160000)return -1;
    sqlite3_stmt *st=NULL;int okay=0,step=SQLITE_DONE,count=0;
    if(sqlite3_prepare_v2(db,"SELECT CASE WHEN json_valid(?1) "
      "THEN json_type(?1,'$.authoring_v1.editorial')='array' ELSE 0 END",
      -1,&st,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_text(st,1,tags,-1,SQLITE_TRANSIENT);
    if(sqlite3_step(st)==SQLITE_ROW)okay=sqlite3_column_int(st,0);
    sqlite3_finalize(st);
    if(!okay)return 0;
    const char *query="SELECT json_extract(value,'$.type'),"
      "json_extract(value,'$.data.text'),json_extract(value,'$.data.level'),"
      "json_extract(value,'$.data.caption'),json_extract(value,'$.data.code'),"
      "json_extract(value,'$.data.style'),json_extract(value,'$.data.items'),"
      "json_extract(value,'$.data.content') "
      "FROM json_each(?1,'$.authoring_v1.editorial')";
    if(sqlite3_prepare_v2(db,query,-1,&st,NULL)!=SQLITE_OK)return -1;
    sqlite3_bind_text(st,1,tags,-1,SQLITE_TRANSIENT);
    char *buffer=NULL;size_t length=0;FILE *render=open_memstream(&buffer,&length);
    if(!render){sqlite3_finalize(st);return -1;}
    int invalid=0;
    while((step=sqlite3_step(st))==SQLITE_ROW){
        if(++count>256){invalid=1;break;}
        const char *type=(const char*)sqlite3_column_text(st,0);
        const char *body=(const char*)sqlite3_column_text(st,1);
        const char *caption=(const char*)sqlite3_column_text(st,3);
        if(!type){invalid=1;break;}
        if(!strcmp(type,"paragraph")){
            fputs("<p>",render);bf_aeb_text(render,body);fputs("</p>",render);
        }else if(!strcmp(type,"header")){
            int level=sqlite3_column_type(st,2)==SQLITE_INTEGER?sqlite3_column_int(st,2):2;
            if(level<2)level=2;if(level>6)level=6;
            fprintf(render,"<h%d>",level);bf_aeb_text(render,body);fprintf(render,"</h%d>",level);
        }else if(!strcmp(type,"quote")){
            fputs("<blockquote><p>",render);bf_aeb_text(render,body);
            fputs("</p>",render);
            if(caption&&*caption){fputs("<cite>",render);bf_aeb_text(render,caption);fputs("</cite>",render);}
            fputs("</blockquote>",render);
        }else if(!strcmp(type,"code")){
            fputs("<pre><code>",render);bf_aeb_text(render,(const char*)sqlite3_column_text(st,4));
            fputs("</code></pre>",render);
        }else if(!strcmp(type,"delimiter"))fputs("<hr>",render);
        else if(!strcmp(type,"list")||!strcmp(type,"checklist")){
            const char *items=(const char*)sqlite3_column_text(st,6);
            const char *style=(const char*)sqlite3_column_text(st,5);
            if(bf_aeb_list(render,db,items,style&&!strcmp(style,"ordered"))){invalid=1;break;}
        }else if(!strcmp(type,"table")){
            if(bf_aeb_table(render,db,(const char*)sqlite3_column_text(st,7))){invalid=1;break;}
        }else{invalid=1;break;}
        if(length>250000){invalid=1;break;}
    }
    int closed=fclose(render);sqlite3_finalize(st);
    if(invalid||step!=SQLITE_DONE||closed||length>250000){free(buffer);return -1;}
    if(!count){free(buffer);return 0;}
    fwrite(buffer,1,length,out);free(buffer);return 1;
}
#endif
