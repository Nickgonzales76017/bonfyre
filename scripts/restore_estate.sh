#!/bin/sh
# Rebuild the estate on a bare host from a rescue disk and this repository.
#
# Oracle disabled the old instance without warning; nothing about it could be
# restarted, and the only asset left was its disk.  An estate that takes a
# person a day to rebuild by hand is an estate that goes down for a day, so
# this is the rebuild, written once and kept: attach the old disk, take the
# state that is genuinely irreplaceable, build the organs here, and start the
# services.
#
# What is taken from the rescue disk (state, never binaries -- the old host was
# x86 and this one is aarch64):
#   /var/lib/bonfyre    the census, the ledger, retained bodies, the archive
#   /etc/bonfyre        each service's environment
#   /etc/letsencrypt    certificates that are still valid, so TLS survives
#
# What is built here: every organ, from the repository, for this architecture.
#
# Run it on the new host:
#   restore_estate.sh --rescue /dev/oracleoci/oraclevdb --repo /opt/bonfyre
#
# It is safe to run twice: the disk is mounted read-only, copies do not clobber
# newer files, and services are restarted rather than duplicated.
set -eu

RESCUE=""
REPO="/opt/bonfyre"
MOUNT="/mnt/rescue"
DO_BUILD=1
DO_SERVICES=1

while [ $# -gt 0 ]; do
    case "$1" in
        --rescue) RESCUE=$2; shift 2 ;;
        --repo) REPO=$2; shift 2 ;;
        --mount) MOUNT=$2; shift 2 ;;
        --no-build) DO_BUILD=0; shift ;;
        --no-services) DO_SERVICES=0; shift ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

say() { echo "==> $*"; }

say "packages"
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential gcc make pkg-config \
    libsqlite3-dev sqlite3 zlib1g-dev libcurl4-openssl-dev \
    python3 python3-venv nginx nodejs npm rsync curl ca-certificates >/dev/null

if [ -n "$RESCUE" ]; then
    say "rescue disk"
    # The disk holds a whole root filesystem; only three trees are taken from
    # it, and it is mounted read-only so a mistake here cannot damage the one
    # copy of the estate's state that exists.
    sudo mkdir -p "$MOUNT"
    if ! mountpoint -q "$MOUNT"; then
        part=$(lsblk -nrpo NAME,FSTYPE "$RESCUE" | awk '$2=="ext4"{print $1}' | tail -1)
        [ -n "$part" ] || { echo "no ext4 filesystem on $RESCUE" >&2; exit 1; }
        sudo mount -o ro "$part" "$MOUNT"
    fi
    for tree in var/lib/bonfyre etc/bonfyre etc/letsencrypt; do
        if [ -d "$MOUNT/$tree" ]; then
            say "  taking /$tree"
            sudo mkdir -p "/$tree"
            # Never overwrite something newer here with something older there.
            sudo rsync -a --update "$MOUNT/$tree/" "/$tree/"
        else
            say "  /$tree is not on the rescue disk"
        fi
    done
    sudo chown -R ubuntu:ubuntu /var/lib/bonfyre 2>/dev/null || true
    sudo umount "$MOUNT" || true
fi

if [ "$DO_BUILD" = "1" ]; then
    say "organs (built here, for this architecture)"
    [ -d "$REPO" ] || { echo "no repository at $REPO" >&2; exit 1; }
    # Only what the public estate needs to answer and to keep itself fed.
    for organ in BonfyreWebAddress BonfyreCMS BonfyreSourceFrontier BonfyreLatentAtlas; do
        if [ -d "$REPO/cmd/$organ" ]; then
            say "  $organ"
            ( cd "$REPO/cmd/$organ" && make -s ) || { echo "$organ did not build" >&2; exit 1; }
        fi
    done
    if [ -d "$REPO/components/BonfyreProject" ]; then
        say "  BonfyreProject"
        ( cd "$REPO/components/BonfyreProject" && make -s ) || true
    fi
    say "installing binaries"
    for binary in \
        "$REPO/cmd/BonfyreWebAddress/bonfyre-web-address" \
        "$REPO/cmd/BonfyreCMS/bonfyre-cms" \
        "$REPO/cmd/BonfyreSourceFrontier/bonfyre-source-frontier" \
        "$REPO/cmd/BonfyreLatentAtlas/bonfyre-latent-atlas" \
        "$REPO/components/BonfyreProject/bonfyre-project"; do
        [ -x "$binary" ] && sudo install -m 0755 "$binary" /usr/local/bin/
    done
fi

if [ "$DO_SERVICES" = "1" ]; then
    say "services"
    sudo mkdir -p /etc/bonfyre /var/lib/bonfyre
    # The environment each service reads.  An existing file is left alone: it
    # came off the rescue disk and is the estate's own.
    if [ ! -f /etc/bonfyre/web-address.env ]; then
        say "  writing a first web-address.env"
        sudo tee /etc/bonfyre/web-address.env >/dev/null <<ENV
BONFYRE_MATRIX_DB=/var/lib/bonfyre/matrix/bonfyre_matrix.db
BONFYRE_CMS_DB=/var/lib/bonfyre/cms/bonfyre_cms.db
BONFYRE_FOREIGN_ROOT=/var/lib/bonfyre/foreign
BONFYRE_SOURCE_FRONTIER_DB=/var/lib/bonfyre/frontier/source_frontier.db
BONFYRE_ITEM_ARCHIVE_DB=/var/lib/bonfyre/archive/items.db
BONFYRE_PRICES_DB=/var/lib/bonfyre/economics/prices.db
BONFYRE_PUBLIC_PROJECTION_DB=/var/lib/bonfyre/cache/public_projection.db
BONFYRE_LOCAL_ZONE=America/Chicago
BONFYRE_SITE_ROOT=$REPO/site/matrix
BONFYRE_WEB_ADDRESS_PORT=8787
BONFYRE_WEB_ADDRESS_WORKERS=4
ENV
    fi
    for directory in matrix cms foreign frontier archive economics cache; do
        sudo mkdir -p "/var/lib/bonfyre/$directory"
    done
    sudo chown -R ubuntu:ubuntu /var/lib/bonfyre

    if [ -d "$REPO/deploy/systemd" ]; then
        for unit in bonfyre-web-address.service bonfyre-reheat.timer bonfyre-reheat.service \
                    bonfyre-automate-daily.timer bonfyre-automate-daily.service; do
            [ -f "$REPO/deploy/systemd/$unit" ] && sudo install -m 0644 "$REPO/deploy/systemd/$unit" /etc/systemd/system/
        done
    fi
    sudo systemctl daemon-reload
    sudo systemctl enable --now bonfyre-web-address.service || true
    sudo systemctl enable --now bonfyre-reheat.timer 2>/dev/null || true
    sudo systemctl enable --now bonfyre-automate-daily.timer 2>/dev/null || true

    say "the front door"
    if [ -d "$REPO/deploy/nginx" ]; then
        sudo install -m 0644 "$REPO/deploy/nginx/00-compression.conf" /etc/nginx/conf.d/ 2>/dev/null || true
        sudo install -m 0644 "$REPO/deploy/nginx/aurekai-surfaces.conf" /etc/nginx/sites-available/ 2>/dev/null || true
        sudo ln -sf /etc/nginx/sites-available/aurekai-surfaces.conf /etc/nginx/sites-enabled/ 2>/dev/null || true
        sudo rm -f /etc/nginx/sites-enabled/default
        # nginx refuses to start on a config it cannot parse; say so plainly
        # rather than leaving a dead front door behind a green exit code.
        if sudo nginx -t 2>/dev/null; then
            sudo systemctl enable --now nginx
            sudo systemctl reload nginx
        else
            say "  nginx config did not validate; the estate answers on :8787 and the front door is not up"
            sudo nginx -t || true
        fi
    fi
fi

say "what is answering"
sleep 2
curl -fsS -o /dev/null -w '  local :8787 -> %{http_code}\n' -H 'Host: aurekai.life' http://127.0.0.1:8787/ || echo "  local :8787 did not answer"
systemctl is-active bonfyre-web-address >/dev/null 2>&1 && echo "  bonfyre-web-address: active" || echo "  bonfyre-web-address: NOT active"
echo
echo "The public address still has to be pointed at this host before anyone"
echo "outside can reach it, and nothing here changes DNS."
