# The estate, as a thing that can be put down anywhere.
#
# Oracle disabled the host this estate lived on, without warning, and took its
# address with it.  Nothing about that was recoverable by us, and the capacity
# to replace it in kind did not exist: every launch for hours answered "out of
# host capacity".  An estate that only exists on one machine in one region is
# an estate that is one stranger's decision away from being gone.
#
# So the estate builds here, from its own source, and carries its state on a
# volume.  What it needs from the world is a disk and a port.
FROM debian:bookworm-slim AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config libsqlite3-dev zlib1g-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
# Only what the public estate needs to compose a page and keep itself fed.
COPY engine/ engine/
COPY lib/ lib/
COPY cmd/BonfyreLex/ cmd/BonfyreLex/
COPY cmd/BonfyreWebAddress/ cmd/BonfyreWebAddress/
COPY cmd/BonfyreCMS/ cmd/BonfyreCMS/
COPY cmd/BonfyreSourceFrontier/ cmd/BonfyreSourceFrontier/
COPY components/BonfyreProject/ components/BonfyreProject/

RUN set -eu; \
    for organ in cmd/BonfyreWebAddress cmd/BonfyreCMS cmd/BonfyreSourceFrontier; do \
        echo "building $organ"; make -C "$organ" -s; \
    done; \
    make -C components/BonfyreProject -s || echo "project organ not built; the estate still serves"

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        libsqlite3-0 zlib1g ca-certificates curl tzdata python3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/cmd/BonfyreWebAddress/bonfyre-web-address /usr/local/bin/
COPY --from=build /src/cmd/BonfyreCMS/bonfyre-cms /usr/local/bin/
COPY --from=build /src/cmd/BonfyreSourceFrontier/bonfyre-source-frontier /usr/local/bin/
COPY --from=build /src/components/BonfyreProject/bonfyre-project /usr/local/bin/
# The authored visual character every page is dressed in.
COPY site/matrix/ /opt/bonfyre/site/matrix/
COPY scripts/ /opt/bonfyre/scripts/
COPY cmd/BonfyreWebAddress/tools/ /opt/bonfyre/cmd/BonfyreWebAddress/tools/
COPY cmd/BonfyreSourceFrontier/tools/ /opt/bonfyre/cmd/BonfyreSourceFrontier/tools/
COPY cmd/BonfyreCMS/content-types/ /opt/bonfyre/cmd/BonfyreCMS/content-types/
COPY deploy/fly/start-estate.sh /usr/local/bin/start-estate
# The estate this image is born with: its census, its ledger, everything it has
# read, and the bodies it retained.  Unpacked once onto an empty volume and
# ignored ever after -- the volume is the estate, this is only its first copy.
COPY deploy/fly/seed/estate.tar.gz /opt/bonfyre/seed/estate.tar.gz

# State lives on the volume, never in the image: the image is disposable and
# the estate is not.
ENV BONFYRE_STATE=/var/lib/bonfyre \
    BONFYRE_MATRIX_DB=/var/lib/bonfyre/matrix/bonfyre_matrix.db \
    BONFYRE_CMS_DB=/var/lib/bonfyre/cms/bonfyre_cms.db \
    BONFYRE_FOREIGN_ROOT=/var/lib/bonfyre/foreign \
    BONFYRE_SOURCE_FRONTIER_DB=/var/lib/bonfyre/frontier/source_frontier.db \
    BONFYRE_ITEM_ARCHIVE_DB=/var/lib/bonfyre/archive/items.db \
    BONFYRE_PRICES_DB=/var/lib/bonfyre/economics/prices.db \
    BONFYRE_PUBLIC_PROJECTION_DB=/var/lib/bonfyre/cache/public_projection.db \
    BONFYRE_SITE_ROOT=/opt/bonfyre/site/matrix \
    BONFYRE_LOCAL_ZONE=America/Chicago \
    BONFYRE_WEB_ADDRESS_PORT=8787

RUN chmod +x /usr/local/bin/start-estate
EXPOSE 8787
CMD ["/usr/local/bin/start-estate"]
