#!/bin/sh
# Claim an Always Free Ampere host the moment Oracle has one.
#
# Oracle disabled this estate's instance without warning and took its address.
# Replacing it in kind needs capacity that does not exist on demand: every
# launch for hours answers "Out of host capacity", and the only way anyone gets
# an A1 is to keep asking until a host frees up.
#
# Asking is not work that needs judgement -- it is one API call in a loop -- so
# it belongs in a cron job rather than in a person's afternoon or a model's
# context.  This runs from launchd every few minutes, forever, and stops the
# day it wins.
#
# It creates exactly one thing, and only on success: an instance.  It never
# deletes, resizes or reboots anything, and it stops for good on any refusal
# that is not about capacity, because a limit or a permission problem will not
# fix itself by being asked again.
set -u

STATE="${BONFYRE_CLAIM_STATE:-$HOME/.bonfyre/claim-a1}"
mkdir -p "$STATE"
LOG="$STATE/claim.log"
WON="$STATE/instance.ocid"
STOP="$STATE/stop"

[ -f "$WON" ] && exit 0
[ -f "$STOP" ] && exit 0

TENANCY=${ORACLE_TENANCY:-ocid1.tenancy.oc1..aaaaaaaah5nramzzmtaou7gixinnu6jemcsd7oukjv5x4bg7eszdbu327yhq}
PINNED_IMAGE=ocid1.image.oc1.us-chicago-1.aaaaaaaarfvncnx66uvxfcj6s52ghvqnrqjkdk4ik6koxrecvxxghrtswvpa
IMAGE=${ORACLE_IMAGE:-}
SUBNET=${ORACLE_SUBNET:-ocid1.subnet.oc1.us-chicago-1.aaaaaaaa2k5picmu6xti4acd5djrjuegl4234usgxsioyosk33bvzh4o2kma}
KEY=${ORACLE_SSH_KEY:-$HOME/.ssh/bonfyre_oracle.pub}
NAME=${ORACLE_INSTANCE_NAME:-bonfyre-runtime-02}
OCI=${OCI_BIN:-/opt/homebrew/bin/oci}

[ -x "$OCI" ] || { echo "$(date -u +%FT%TZ) no oci cli at $OCI" >> "$LOG"; exit 1; }

note() { echo "$(date -u +%FT%TZ) $*" >> "$LOG"; }

# Ask for the newest Ampere Ubuntu the region publishes, not the one that was
# current the week this was written; fall back to the pinned image if the
# listing itself fails.
if [ -z "$IMAGE" ]; then
    IMAGE=$("$OCI" compute image list -c "$TENANCY" --operating-system "Canonical Ubuntu" \
        --operating-system-version "24.04" --shape "VM.Standard.A1.Flex" \
        --sort-by TIMECREATED --sort-order DESC --limit 1 \
        --query 'data[0].id' --raw-output 2>/dev/null)
    case "$IMAGE" in ocid1.image.*) ;; *) IMAGE=$PINNED_IMAGE ;; esac
fi

# The small machine first, on every run. Eight hundred asks for the full one
# were all refused; a smaller ask lands more often, the estate's organs are
# native and light enough to start on one core, and an A1 Flex can be grown to
# the tenancy's whole allowance (2 cores, 12 GB) once it exists. AD-1 is asked
# first because the old estate's disk lives there and a volume attaches only
# within its own availability domain.
for want in "1 6" "2 12"; do
  set -- $want
  SHAPE_CONFIG="{\"ocpus\":$1,\"memoryInGBs\":$2}"; WANTED="$1/$2"
  for n in 1 2 3; do
    AD="Ehfh:US-CHICAGO-1-AD-$n"
    answer=$("$OCI" compute instance launch \
        -c "$TENANCY" --availability-domain "$AD" \
        --shape "VM.Standard.A1.Flex" --shape-config "$SHAPE_CONFIG" \
        --image-id "$IMAGE" --subnet-id "$SUBNET" \
        --display-name "$NAME" --assign-public-ip true \
        --ssh-authorized-keys-file "$KEY" --boot-volume-size-in-gbs 50 2>&1)

    if printf '%s' "$answer" | grep -q '"id": "ocid1.instance'; then
        printf '%s' "$answer" | grep -m1 '"id": "ocid1.instance' \
            | sed 's/.*"\(ocid1.instance[^"]*\)".*/\1/' > "$WON"
        note "WON $WANTED in $AD -> $(cat "$WON")"
        printf '%s\n' "$AD" > "$STATE/instance.ad"
        [ "$n" = 1 ] || note "the old estate disk is in AD-1 and cannot attach here; restore its state from the boot volume backup"
        # Say so where a person will actually see it.
        /usr/bin/osascript -e 'display notification "An Always Free Ampere host is ours. The estate can move onto it." with title "Oracle A1 claimed"' 2>/dev/null || true
        exit 0
    fi

    why=$(printf '%s' "$answer" | grep -m1 '"message"' | sed 's/.*"message": "\([^"]*\)".*/\1/' | cut -c1-90)
    note "$WANTED $AD: ${why:-refused}"

    case "$answer" in
        *LimitExceeded*|*QuotaExceeded*|*NotAuthorized*|*NotAuthenticated*)
            note "stopping: this is not a capacity problem, and asking again will not fix it"
            date -u +%FT%TZ > "$STOP"
            exit 2 ;;
    esac
  done
done
exit 1
