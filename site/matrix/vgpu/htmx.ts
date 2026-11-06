/* HTMX is bundled locally: server-authored fragments, no CDN dependency. */
import htmx from "htmx.org";

(globalThis as typeof globalThis & { htmx?: typeof htmx }).htmx = htmx;
