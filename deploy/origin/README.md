# Estate origin

Serves every page the estate has published for a publisher or member address,
from this machine, behind the tunnel. It runs `deploy/worker/estate-worker.mjs`
unchanged and reads the same object store, so an address answers the same way
it did at the edge, without the edge Worker's daily request cap.

| | |
|---|---|
| Runs as | launchd `com.bonfyre.estate-origin` (`scripts/run_estate_origin.sh`) |
| Listens on | `127.0.0.1:28931` |
| Log | `~/.bonfyre/estate-local/estate-origin.log` |
| Reaches readers through | the tunnel rule `*.aurekai.life` and the zone's wildcard record |
| Still on the Worker | `/search`, `/mcp`, `/.well-known/oauth-protected-resource` |

Check it:

```sh
curl -sI -H 'Host: 9to5mac-com.aurekai.life' http://127.0.0.1:28931/ | head -1
```

Put the Worker back in front (it outranks the tunnel as soon as the route exists):

```sh
# with a wrangler login
curl -X POST "https://api.cloudflare.com/client/v4/zones/$ZONE_ID/workers/routes" \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"pattern":"*.aurekai.life/*","script":"aurekai-estate"}'
```

A page changes here within five minutes of being published; nothing needs
restarting. The address is down when this machine or its tunnel is.
