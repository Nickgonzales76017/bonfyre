#!/bin/sh
# Registers the native-messaging host with Chrome for one extension id. This
# writes into the browser's own configuration directory, so it is run by the
# person whose browser it is and by nothing else.
#   usage: install.sh <extension-id> [foreign-root]
set -eu
id=${1:?extension id, from chrome://extensions after loading integrations/browser/extension}
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
root=${2:-$HOME/.bonfyre/foreign}
target="$HOME/Library/Application Support/Google/Chrome/NativeMessagingHosts"
[ "$(uname)" = Darwin ] || target="$HOME/.config/google-chrome/NativeMessagingHosts"
bun=$(command -v bun) || { echo "bun is not on PATH" >&2; exit 1; }
mkdir -p "$target" "$root/receipts" "$root/bodies"
launcher="$here/bonfyre-browser-host.launch"
# Chrome starts the host with an empty environment, so what it reads is stated here.
cat > "$launcher" <<LAUNCH
#!/bin/sh
export BONFYRE_FOREIGN_ROOT="$root" BONFYRE_FOREIGN_RECEIPTS="$root/receipts" BONFYRE_BROWSER_QUEUE_DB="$root/held.db"
exec "$bun" "$here/bonfyre-browser-host.mjs"
LAUNCH
chmod +x "$launcher"
sed -e "s|@HOST_LAUNCHER@|$launcher|" -e "s|@EXTENSION_ID@|$id|" "$here/com.bonfyre.browser_host.json.template" > "$target/com.bonfyre.browser_host.json"
echo "registered for extension $id; observations are kept under $root"
