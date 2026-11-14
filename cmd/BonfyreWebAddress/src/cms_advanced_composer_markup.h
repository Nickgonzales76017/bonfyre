#ifndef BONFYRE_CMS_ADVANCED_COMPOSER_MARKUP_H
#define BONFYRE_CMS_ADVANCED_COMPOSER_MARKUP_H
/* Authentication and namespace selection belong to serve_cms_composer.
 * The advanced browser editor does not introduce a new write route. */
static const char *cms_advanced_composer_markup=
"<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
"<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
"<meta name=\"referrer\" content=\"same-origin\">"
"<title>Aurekai · Visual Authoring</title>"
"<link rel=\"stylesheet\" href=\"/assets/authoring-editor.css\">"
"</head><body class=\"authoring-page\">"
"<header class=\"authoring-top\"><a href=\"/cms/compose\">&#8592; Page compositions</a>"
"<strong>Aurekai · Visual Authoring</strong><span>Native CMS draft workspace</span>"
"<form action=\"/cms/compose/advanced\" method=\"get\"><label>Page address "
"<input name=\"route\" maxlength=\"127\" required pattern=\"/[^?#]*\" value=\"/\"></label>"
"<button type=\"submit\">Open</button></form></header>"
"<main data-aurekai-authoring role=\"main\"><p>Opening your account-owned composition...</p></main>"
"<script type=\"module\" src=\"/assets/authoring-editor.js\"></script>"
"</body></html>";
#endif
