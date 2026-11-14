/* Generated from sections/*.mjs. This registry is not a publication right. */
#ifndef BF_NATIVE_SITE_SECTION_CATALOGUE_H
#define BF_NATIVE_SITE_SECTION_CATALOGUE_H
#include <stddef.h>
#include <string.h>
typedef struct {const char *kind;const char *required;const char *source_digest;} BfNativeSection;
static const BfNativeSection bf_native_sections[] = {
    {"amounts","title","dc974851744b09f9335d44a05c6181a2914841c361d80a168c8069fd3fe8b9dc"},
    {"ask","form","066260f303986418883cba54bc1f7c9a1c905d83a8cf854da05453beb0588f32"},
    {"calculator","title","3baeefd4111051e158523ab67a2bba0bfa485690374aa304546f66d430c48f21"},
    {"call","title","f35fb23f4a86ab61ed336e5d3a521d811fa74292bdbba063b8753bc4f8d8c069"},
    {"cases","title,items","9f384e5c18ece2ff32571d50c66d8bede0046e67eef010c51ca0d0de7f918d30"},
    {"catalogue","title,entries,stores","39216a0f30870820ffaeb71cfa2d56b36fa7712aced4ec7a4e5c0f8065c0c3ee"},
    {"compare","title,rows,other","cb5af3389a16614bc3655f9d3c330dacd40fd55d32e1e6e1f2969815a8c8ed69"},
    {"creator","title,sample","622b956c46322eba97d7064f79fb6efeca9e9f40d2b1e9e3f917b8804f0ed877"},
    {"door","title,action","41c10a16f13b5a47fd4e7ccd55ace1a29d50ed7148663286297ff9228a7dfad2"},
    {"figures","title","4cccac1328086a3e231f0b2d43dace352750ce1d352caaddd9a162ca9af5c573"},
    {"hero","headline","58d37f0235cef618b17c53890a1512d00b94a23050a6a970d9f637dee8f68ddf"},
    {"library","title","bda95edca834adcc7df0bea958cc35395ee47aa39fd688235bcf2e0d8dcaa0a5"},
    {"listings","title","f3693f1fea24955692baa963a15674db124585381080701321d47997ce2dc1f4"},
    {"opportunity","title,sample","224c4a071b25759c1e6fb97f64dc7afb18f45d93db0b208b53f42ac81d27c4af"},
    {"person","text","42e5766d9242a41b682d460b65907dc4dee8b1faccadf57f33602dbb3bbebeec"},
    {"plan","title,sample","d35ac6cb75df22a38d959ce9cfb22bb7250e51add121e8bf663fe11c03ee2e30"},
    {"planner","title,catalogue","656af3f0812b11b9db62c2320863fa4ca748fcce5f46740d9809c4b650a0c8fc"},
    {"pricing","title,sample","c69f8611902387e53177e2cfe98bcecd5e2153c7856fb29b9821b5dae61b4802"},
    {"proof","items","bbf8a1bc1c945ad8039c39232c35ef3f326eb314b57a5f57552a7718f2605e48"},
    {"prose","title,items","072ef05834e705aa28f621ec772cf942d9d6c1ad14a74e7dbce8625d97264a78"},
    {"reviews","","8795c23e5c173c2999ef93f2c925af62a8f860c05614429f272b359e5a4c1414"},
    {"schema","title,sample","ec8bef3d4f0c63c6e7f50f7d4cf2f0e05ed0477574b0f7440cf1c6c75a5b8df6"},
    {"shipments","title,sample","a802adbdb049bdc43d30f5d293f843044b7d69eb6d304852ff89e18a9f73d5bc"},
    {"simulation","title,sample","e49d11572ef99c1bedfc770aba7dc175b51d63101000ea2cd07d794bbf950764"},
    {"steps","title,items","e3d6f31415cab67af585994ffbb73481cce99b1997d47dfff5187fabb2076591"},
    {"synthesis","title,sample","51b50a9f5f81b88b98d91b1083e6c9e5c35466472f29bda5022a911ccd97f1f1"},
    {"workbook","title,data","9d12ed1b82795e1b8d762b2dca5b04ed0031898d4a7c1bdcc5f229620556060c"},
};
#define BF_NATIVE_SECTION_COUNT (sizeof(bf_native_sections)/sizeof(bf_native_sections[0]))
static inline const BfNativeSection *bf_native_section(const char *kind) {
 if(!kind)return NULL;
 for(size_t i=0;i<BF_NATIVE_SECTION_COUNT;i++)if(!strcmp(bf_native_sections[i].kind,kind))return &bf_native_sections[i];
 return NULL;
}
#endif
