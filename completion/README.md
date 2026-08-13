# Bonfyre Completion Controller

`tests/unified_fabric_acceptance.sh` reads `requirements.yaff`, executes every
requirement command, and writes append-only evidence beneath `$BONFYRE_STATE_DIR/completion`.
The scripts in `scripts/` derive status and the next dependency-ready failure from that evidence.
