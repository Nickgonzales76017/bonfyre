/* glyph_set.h -- the estate's one icon vocabulary, as literals.
 *
 * Lifted out of main.c unchanged so the projections compiled as their own
 * units (people, source depth, detail depth) draw the same Tabler geometry
 * instead of rendering bare text where a page reserves an icon. */
#ifndef BONFYRE_GLYPH_SET_H
#define BONFYRE_GLYPH_SET_H
/* Tabler Icons subset — MIT License, Copyright (c) 2020-2026 Paweł Kuna.
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE. */
#define BF_ICON_OPEN "<svg class=\"glyph ak-icon\" viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" aria-hidden=\"true\">"
#define BF_ICON_CLOSE "</svg>"
#define BF_ICON_WORLD BF_ICON_OPEN "<path d=\"M3 12a9 9 0 1 0 18 0a9 9 0 0 0 -18 0\"/><path d=\"M3.6 9h16.8\"/><path d=\"M3.6 15h16.8\"/><path d=\"M11.5 3a17 17 0 0 0 0 18\"/><path d=\"M12.5 3a17 17 0 0 1 0 18\"/>" BF_ICON_CLOSE
#define BF_ICON_HEART BF_ICON_OPEN "<path d=\"M19.5 12.572l-7.5 7.428l-7.5 -7.428a5 5 0 1 1 7.5 -6.566a5 5 0 1 1 7.5 6.572\"/>" BF_ICON_CLOSE
#define BF_ICON_SHOP BF_ICON_OPEN "<path d=\"M6.331 8h11.339a2 2 0 0 1 1.977 2.304l-1.255 8.152a3 3 0 0 1 -2.966 2.544h-6.852a3 3 0 0 1 -2.965 -2.544l-1.255 -8.152a2 2 0 0 1 1.977 -2.304\"/><path d=\"M9 11v-5a3 3 0 0 1 6 0v5\"/>" BF_ICON_CLOSE
#define BF_ICON_BRIEFCASE BF_ICON_OPEN "<path d=\"M3 9a2 2 0 0 1 2 -2h14a2 2 0 0 1 2 2v9a2 2 0 0 1 -2 2h-14a2 2 0 0 1 -2 -2l0 -9\"/><path d=\"M8 7v-2a2 2 0 0 1 2 -2h4a2 2 0 0 1 2 2v2\"/><path d=\"M12 12l0 .01\"/><path d=\"M3 13a20 20 0 0 0 18 0\"/>" BF_ICON_CLOSE
#define BF_ICON_SEARCH BF_ICON_OPEN "<path d=\"M3 10a7 7 0 1 0 14 0a7 7 0 1 0 -14 0\"/><path d=\"M21 21l-6 -6\"/>" BF_ICON_CLOSE
#define BF_ICON_LOGIN BF_ICON_OPEN "<path d=\"M15 8v-2a2 2 0 0 0 -2 -2h-7a2 2 0 0 0 -2 2v12a2 2 0 0 0 2 2h7a2 2 0 0 0 2 -2v-2\"/><path d=\"M21 12h-13l3 -3\"/><path d=\"M11 15l-3 -3\"/>" BF_ICON_CLOSE
#define BF_ICON_COMPASS BF_ICON_OPEN "<path d=\"M8 16l2 -6l6 -2l-2 6l-6 2\"/><path d=\"M3 12a9 9 0 1 0 18 0a9 9 0 1 0 -18 0\"/><path d=\"M12 3l0 2\"/><path d=\"M12 19l0 2\"/><path d=\"M3 12l2 0\"/><path d=\"M19 12l2 0\"/>" BF_ICON_CLOSE
#define BF_ICON_CALENDAR BF_ICON_OPEN "<path d=\"M4 7a2 2 0 0 1 2 -2h12a2 2 0 0 1 2 2v12a2 2 0 0 1 -2 2h-12a2 2 0 0 1 -2 -2l0 -12\"/><path d=\"M16 3l0 4\"/><path d=\"M8 3l0 4\"/><path d=\"M4 11l16 0\"/><path d=\"M8 15h2v2h-2l0 -2\"/>" BF_ICON_CLOSE
#define BF_ICON_PLAY BF_ICON_OPEN "<path d=\"M7 4v16l13 -8l-13 -8\"/>" BF_ICON_CLOSE
#define BF_ICON_HEADPHONES BF_ICON_OPEN "<path d=\"M4 15a2 2 0 0 1 2 -2h1a2 2 0 0 1 2 2v3a2 2 0 0 1 -2 2h-1a2 2 0 0 1 -2 -2l0 -3\"/><path d=\"M15 15a2 2 0 0 1 2 -2h1a2 2 0 0 1 2 2v3a2 2 0 0 1 -2 2h-1a2 2 0 0 1 -2 -2l0 -3\"/><path d=\"M4 15v-3a8 8 0 0 1 16 0v3\"/>" BF_ICON_CLOSE
#define BF_ICON_BOARD BF_ICON_OPEN "<path d=\"M4 6a2 2 0 0 1 2 -2h12a2 2 0 0 1 2 2v12a2 2 0 0 1 -2 2h-12a2 2 0 0 1 -2 -2l0 -12\"/><path d=\"M4 9h8\"/><path d=\"M12 15h8\"/><path d=\"M12 4v16\"/>" BF_ICON_CLOSE
#define BF_ICON_LIST_CHECK BF_ICON_OPEN "<path d=\"M3.5 5.5l1.5 1.5l2.5 -2.5\"/><path d=\"M3.5 11.5l1.5 1.5l2.5 -2.5\"/><path d=\"M3.5 17.5l1.5 1.5l2.5 -2.5\"/><path d=\"M11 6l9 0\"/><path d=\"M11 12l9 0\"/><path d=\"M11 18l9 0\"/>" BF_ICON_CLOSE
#define BF_ICON_COINS BF_ICON_OPEN "<path d=\"M9 14c0 1.657 2.686 3 6 3s6 -1.343 6 -3s-2.686 -3 -6 -3s-6 1.343 -6 3\"/><path d=\"M9 14v4c0 1.656 2.686 3 6 3s6 -1.344 6 -3v-4\"/><path d=\"M3 6c0 1.072 1.144 2.062 3 2.598s4.144 .536 6 0c1.856 -.536 3 -1.526 3 -2.598c0 -1.072 -1.144 -2.062 -3 -2.598s-4.144 -.536 -6 0c-1.856 .536 -3 1.526 -3 2.598\"/><path d=\"M3 6v10c0 .888 .772 1.45 2 2\"/><path d=\"M3 11c0 .888 .772 1.45 2 2\"/>" BF_ICON_CLOSE
#define BF_ICON_SPARKLES BF_ICON_OPEN "<path d=\"M16 18a2 2 0 0 1 2 2a2 2 0 0 1 2 -2a2 2 0 0 1 -2 -2a2 2 0 0 1 -2 2m0 -12a2 2 0 0 1 2 2a2 2 0 0 1 2 -2a2 2 0 0 1 -2 -2a2 2 0 0 1 -2 2m-7 12a6 6 0 0 1 6 -6a6 6 0 0 1 -6 -6a6 6 0 0 1 -6 6a6 6 0 0 1 6 6\"/>" BF_ICON_CLOSE
#define BF_ICON_BOOK BF_ICON_OPEN "<path d=\"M3 19a9 9 0 0 1 9 0a9 9 0 0 1 9 0\"/><path d=\"M3 6a9 9 0 0 1 9 0a9 9 0 0 1 9 0\"/><path d=\"M3 6l0 13\"/><path d=\"M12 6l0 13\"/><path d=\"M21 6l0 13\"/>" BF_ICON_CLOSE
#define BF_ICON_USER BF_ICON_OPEN "<path d=\"M3 12a9 9 0 1 0 18 0a9 9 0 1 0 -18 0\"/><path d=\"M9 10a3 3 0 1 0 6 0a3 3 0 1 0 -6 0\"/><path d=\"M6.168 18.849a4 4 0 0 1 3.832 -2.849h4a4 4 0 0 1 3.834 2.855\"/>" BF_ICON_CLOSE
#define BF_ICON_BOOKMARK BF_ICON_OPEN "<path d=\"M18 7v14l-6 -4l-6 4v-14a4 4 0 0 1 4 -4h4a4 4 0 0 1 4 4\"/>" BF_ICON_CLOSE
#define BF_ICON_SHARE BF_ICON_OPEN "<path d=\"M3 12a3 3 0 1 0 6 0a3 3 0 1 0 -6 0\"/><path d=\"M15 6a3 3 0 1 0 6 0a3 3 0 1 0 -6 0\"/><path d=\"M15 18a3 3 0 1 0 6 0a3 3 0 1 0 -6 0\"/><path d=\"M8.7 10.7l6.6 -3.4\"/><path d=\"M8.7 13.3l6.6 3.4\"/>" BF_ICON_CLOSE
#define BF_ICON_PIN BF_ICON_OPEN "<path d=\"M9 11a3 3 0 1 0 6 0a3 3 0 0 0 -6 0\"/><path d=\"M17.657 16.657l-4.243 4.243a2 2 0 0 1 -2.827 0l-4.244 -4.243a8 8 0 1 1 11.314 0\"/>" BF_ICON_CLOSE
#define BF_ICON_LIST BF_ICON_OPEN "<path d=\"M9 6l11 0\"/><path d=\"M9 12l11 0\"/><path d=\"M9 18l11 0\"/><path d=\"M5 6l0 .01\"/><path d=\"M5 12l0 .01\"/><path d=\"M5 18l0 .01\"/>" BF_ICON_CLOSE
#define BF_ICON_PHOTO BF_ICON_OPEN "<path d=\"M15 8h.01\"/><path d=\"M3 6a3 3 0 0 1 3 -3h12a3 3 0 0 1 3 3v12a3 3 0 0 1 -3 3h-12a3 3 0 0 1 -3 -3v-12\"/><path d=\"M3 16l5 -5c.928 -.893 2.072 -.893 3 0l5 5\"/><path d=\"M14 14l1 -1c.928 -.893 2.072 -.893 3 0l3 3\"/>" BF_ICON_CLOSE
#define BF_ICON_FILE_TEXT BF_ICON_OPEN "<path d=\"M14 3v4a1 1 0 0 0 1 1h4\"/><path d=\"M17 21h-10a2 2 0 0 1 -2 -2v-14a2 2 0 0 1 2 -2h7l5 5v11a2 2 0 0 1 -2 2\"/><path d=\"M9 9l1 0\"/><path d=\"M9 13l6 0\"/><path d=\"M9 17l6 0\"/>" BF_ICON_CLOSE
#define BF_ICON_PAUSE BF_ICON_OPEN "<path d=\"M6 6a1 1 0 0 1 1 -1h2a1 1 0 0 1 1 1v12a1 1 0 0 1 -1 1h-2a1 1 0 0 1 -1 -1l0 -12\"/><path d=\"M14 6a1 1 0 0 1 1 -1h2a1 1 0 0 1 1 1v12a1 1 0 0 1 -1 1h-2a1 1 0 0 1 -1 -1l0 -12\"/>" BF_ICON_CLOSE
#define BF_ICON_PREV BF_ICON_OPEN "<path d=\"M21 5v14l-8 -7l8 -7\"/><path d=\"M10 5v14l-8 -7l8 -7\"/>" BF_ICON_CLOSE
#define BF_ICON_NEXT BF_ICON_OPEN "<path d=\"M3 5v14l8 -7l-8 -7\"/><path d=\"M14 5v14l8 -7l-8 -7\"/>" BF_ICON_CLOSE
#define BF_ICON_EXTERNAL BF_ICON_OPEN "<path d=\"M12 6h-6a2 2 0 0 0 -2 2v10a2 2 0 0 0 2 2h10a2 2 0 0 0 2 -2v-6\"/><path d=\"M11 13l9 -9\"/><path d=\"M15 4h5v5\"/>" BF_ICON_CLOSE
#define BF_ICON_SETTINGS BF_ICON_OPEN "<path d=\"M10.325 4.317c.426 -1.756 2.924 -1.756 3.35 0a1.724 1.724 0 0 0 2.573 1.066c1.543 -.94 3.31 .826 2.37 2.37a1.724 1.724 0 0 0 1.065 2.572c1.756 .426 1.756 2.924 0 3.35a1.724 1.724 0 0 0 -1.066 2.573c.94 1.543 -.826 3.31 -2.37 2.37a1.724 1.724 0 0 0 -2.572 1.065c-.426 1.756 -2.924 1.756 -3.35 0a1.724 1.724 0 0 0 -2.573 -1.066c-1.543 .94 -3.31 -.826 -2.37 -2.37a1.724 1.724 0 0 0 -1.065 -2.572c-1.756 -.426 -1.756 -2.924 0 -3.35a1.724 1.724 0 0 0 1.066 -2.573c-.94 -1.543 .826 -3.31 2.37 -2.37c1 .608 2.296 .07 2.572 -1.065\"/><path d=\"M9 12a3 3 0 1 0 6 0a3 3 0 0 0 -6 0\"/>" BF_ICON_CLOSE
#define BF_ICON_APPS BF_ICON_OPEN "<path d=\"M4 5a1 1 0 0 1 1 -1h4a1 1 0 0 1 1 1v4a1 1 0 0 1 -1 1h-4a1 1 0 0 1 -1 -1l0 -4\"/><path d=\"M4 15a1 1 0 0 1 1 -1h4a1 1 0 0 1 1 1v4a1 1 0 0 1 -1 1h-4a1 1 0 0 1 -1 -1l0 -4\"/><path d=\"M14 15a1 1 0 0 1 1 -1h4a1 1 0 0 1 1 1v4a1 1 0 0 1 -1 1h-4a1 1 0 0 1 -1 -1l0 -4\"/><path d=\"M14 7l6 0\"/><path d=\"M17 4l0 6\"/>" BF_ICON_CLOSE
#define BF_ICON_BELL BF_ICON_OPEN "<path d=\"M10 5a2 2 0 1 1 4 0a7 7 0 0 1 4 6v3a4 4 0 0 0 2 3h-16a4 4 0 0 0 2 -3v-3a7 7 0 0 1 4 -6\"/><path d=\"M9 17v1a3 3 0 0 0 6 0v-1\"/>" BF_ICON_CLOSE
#define BF_ICON_ROBOT BF_ICON_OPEN "<path d=\"M6 6a2 2 0 0 1 2 -2h8a2 2 0 0 1 2 2v4a2 2 0 0 1 -2 2h-8a2 2 0 0 1 -2 -2l0 -4\"/><path d=\"M12 2v2\"/><path d=\"M9 12v9\"/><path d=\"M15 12v9\"/><path d=\"M5 16l4 -2\"/><path d=\"M15 14l4 2\"/><path d=\"M9 18h6\"/><path d=\"M10 8v.01\"/><path d=\"M14 8v.01\"/>" BF_ICON_CLOSE
#define BF_ICON_LOGOUT BF_ICON_OPEN "<path d=\"M14 8v-2a2 2 0 0 0 -2 -2h-7a2 2 0 0 0 -2 2v12a2 2 0 0 0 2 2h7a2 2 0 0 0 2 -2v-2\"/><path d=\"M9 12h12l-3 -3\"/><path d=\"M18 15l3 -3\"/>" BF_ICON_CLOSE
#define BF_ICON_LINK BF_ICON_OPEN "<path d=\"M9 15l6 -6\"/><path d=\"M11 6l.463 -.536a5 5 0 0 1 7.071 7.072l-.534 .464\"/><path d=\"M13 18l-.397 .534a5.068 5.068 0 0 1 -7.127 0a4.972 4.972 0 0 1 0 -7.071l.524 -.463\"/>" BF_ICON_CLOSE
#define BF_ICON_GROUP BF_ICON_OPEN "<path d=\"M10 13a2 2 0 1 0 4 0a2 2 0 0 0 -4 0\"/><path d=\"M8 21v-1a2 2 0 0 1 2 -2h4a2 2 0 0 1 2 2v1\"/><path d=\"M15 5a2 2 0 1 0 4 0a2 2 0 0 0 -4 0\"/><path d=\"M17 10h2a2 2 0 0 1 2 2v1\"/><path d=\"M5 5a2 2 0 1 0 4 0a2 2 0 0 0 -4 0\"/><path d=\"M3 13v-1a2 2 0 0 1 2 -2h2\"/>" BF_ICON_CLOSE
#define BF_ICON_WORLD_WWW BF_ICON_OPEN "<path d=\"M19.5 7a9 9 0 0 0 -7.5 -4a8.991 8.991 0 0 0 -7.484 4\"/><path d=\"M11.5 3a16.989 16.989 0 0 0 -1.826 4\"/><path d=\"M12.5 3a16.989 16.989 0 0 1 1.828 4\"/><path d=\"M19.5 17a9 9 0 0 1 -7.5 4a8.991 8.991 0 0 1 -7.484 -4\"/><path d=\"M11.5 21a16.989 16.989 0 0 1 -1.826 -4\"/><path d=\"M12.5 21a16.989 16.989 0 0 0 1.828 -4\"/><path d=\"M2 10l1 4l1.5 -4l1.5 4l1 -4\"/><path d=\"M17 10l1 4l1.5 -4l1.5 4l1 -4\"/><path d=\"M9.5 10l1 4l1.5 -4l1.5 4l1 -4\"/>" BF_ICON_CLOSE
#define BF_ICON_TOOL BF_ICON_OPEN "<path d=\"M7 10h3v-3l-3.5 -3.5a6 6 0 0 1 8 8l6 6a2 2 0 0 1 -3 3l-6 -6a6 6 0 0 1 -8 -8l3.5 3.5\"/>" BF_ICON_CLOSE
#define BF_ICON_FILE_CHECK BF_ICON_OPEN "<path d=\"M14 3v4a1 1 0 0 0 1 1h4\"/><path d=\"M17 21h-10a2 2 0 0 1 -2 -2v-14a2 2 0 0 1 2 -2h7l5 5v11a2 2 0 0 1 -2 2\"/><path d=\"M9 15l2 2l4 -4\"/>" BF_ICON_CLOSE
#define BF_ICON_WARNING BF_ICON_OPEN "<path d=\"M12 9v4\"/><path d=\"M10.363 3.591l-8.106 13.534a1.914 1.914 0 0 0 1.636 2.871h16.214a1.914 1.914 0 0 0 1.636 -2.87l-8.106 -13.536a1.914 1.914 0 0 0 -3.274 0\"/><path d=\"M12 16h.01\"/>" BF_ICON_CLOSE
#define BF_ICON_CLOCK BF_ICON_OPEN "<path d=\"M3 12a9 9 0 1 0 18 0a9 9 0 0 0 -18 0\"/><path d=\"M12 7v5l3 3\"/>" BF_ICON_CLOSE
#define BF_ICON_ACTIVITY BF_ICON_OPEN "<path d=\"M3 12h4l3 8l4 -16l3 8h4\"/>" BF_ICON_CLOSE
#define BF_ICON_PLUS BF_ICON_OPEN "<path d=\"M12 5l0 14\"/><path d=\"M5 12l14 0\"/>" BF_ICON_CLOSE
#define BF_ICON_EDIT BF_ICON_OPEN "<path d=\"M7 7h-1a2 2 0 0 0 -2 2v9a2 2 0 0 0 2 2h9a2 2 0 0 0 2 -2v-1\"/><path d=\"M20.385 6.585a2.1 2.1 0 0 0 -2.97 -2.97l-8.415 8.385v3h3l8.385 -8.415\"/><path d=\"M16 5l3 3\"/>" BF_ICON_CLOSE
#define BF_ICON_EYE BF_ICON_OPEN "<path d=\"M10 12a2 2 0 1 0 4 0a2 2 0 0 0 -4 0\"/><path d=\"M21 12c-2.4 4 -5.4 6 -9 6c-3.6 0 -6.6 -2 -9 -6c2.4 -4 5.4 -6 9 -6c3.6 0 6.6 2 9 6\"/>" BF_ICON_CLOSE
#define BF_ICON_RECEIPT BF_ICON_OPEN "<path d=\"M5 21v-16a2 2 0 0 1 2 -2h10a2 2 0 0 1 2 2v16l-3 -2l-2 2l-2 -2l-2 2l-2 -2l-3 2m4 -14h6m-6 4h6m-2 4h2\"/>" BF_ICON_CLOSE
#define BF_ICON_CHECK BF_ICON_OPEN "<path d=\"M3 12a9 9 0 1 0 18 0a9 9 0 1 0 -18 0\"/><path d=\"M9 12l2 2l4 -4\"/>" BF_ICON_CLOSE
#define BF_ICON_CHEF BF_ICON_OPEN "<path d=\"M12 3c1.918 0 3.52 1.35 3.91 3.151a4 4 0 0 1 2.09 7.723l0 7.126h-12v-7.126a4 4 0 1 1 2.092 -7.723a4 4 0 0 1 3.908 -3.151\"/><path d=\"M6.161 17.009l11.839 -.009\"/>" BF_ICON_CLOSE
#define BF_ICON_SHIRT BF_ICON_OPEN "<path d=\"M15 4l6 2v5h-3v8a1 1 0 0 1 -1 1h-10a1 1 0 0 1 -1 -1v-8h-3v-5l6 -2a3 3 0 0 0 6 0\"/>" BF_ICON_CLOSE
#define BF_ICON_TAG BF_ICON_OPEN "<path d=\"M6.5 7.5a1 1 0 1 0 2 0a1 1 0 1 0 -2 0\"/><path d=\"M3 6v5.172a2 2 0 0 0 .586 1.414l7.71 7.71a2.41 2.41 0 0 0 3.408 0l5.592 -5.592a2.41 2.41 0 0 0 0 -3.408l-7.71 -7.71a2 2 0 0 0 -1.414 -.586h-5.172a3 3 0 0 0 -3 3\"/>" BF_ICON_CLOSE
#define BF_ICON_STORE BF_ICON_OPEN "<path d=\"M3 21l18 0\"/><path d=\"M3 7v1a3 3 0 0 0 6 0v-1m0 1a3 3 0 0 0 6 0v-1m0 1a3 3 0 0 0 6 0v-1h-18l2 -4h14l2 4\"/><path d=\"M5 21l0 -10.15\"/><path d=\"M19 21l0 -10.15\"/><path d=\"M9 21v-4a2 2 0 0 1 2 -2h2a2 2 0 0 1 2 2v4\"/>" BF_ICON_CLOSE
#define BF_ICON_MIC BF_ICON_OPEN "<path d=\"M15 12.9a5 5 0 1 0 -3.902 -3.9\"/><path d=\"M15 12.9l-3.902 -3.899l-7.513 8.584a2 2 0 1 0 2.827 2.83l8.588 -7.515\"/>" BF_ICON_CLOSE
#define BF_ICON_RSS BF_ICON_OPEN "<path d=\"M4 19a1 1 0 1 0 2 0a1 1 0 1 0 -2 0\"/><path d=\"M4 4a16 16 0 0 1 16 16\"/><path d=\"M4 11a9 9 0 0 1 9 9\"/>" BF_ICON_CLOSE
#endif
