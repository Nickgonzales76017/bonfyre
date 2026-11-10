# The Tonight prototype, as it was served from Cloudflare Pages

Kept because it was the only copy. Between 2026-09-18 and 2026-09-25 a Pages
project named `aurekai` held `aurekai.life` and `www.aurekai.life` as custom
domains. A Pages custom domain outranks a Workers route, so that project — not
the estate — answered the whole apex, returning this one page with HTTP 200 for
every address on the territory, including addresses that do not exist.

Its own links gave it away: it points at `/discover`, `/board`, `/channels`,
`/listen`, `/nearby` and `/o/...`, and loads `/assets/world.css`. It was
authored as a page of the estate and deployed somewhere the estate could not
serve it, so every link on it resolved back to itself.

The apex now belongs to the estate worker, which serves the composed Tonight
from `hosts/aurekai.life/index.html`. This file is the older, richer prototype
(2026-09-19, 43,708 bytes, 85 links, six sections the composer does not yet
build: Listen, Watch, Around Fort Worth and Dallas, Discover, Boards, On this
day). It is evidence of what was once served, not a page to serve again.
