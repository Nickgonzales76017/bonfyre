# BonfyreTel — FreeSWITCH Telephony Adapter

Pure C binary that connects to FreeSWITCH via Event Socket (plain TCP),
giving Bonfyre full telephony capabilities without any Twilio dependency.

**Built-in mock server** — test the entire pipeline without FreeSWITCH,
without a SIP trunk, and without buying a phone number. Zero cost to try.

## Zero-Cost Test (30 seconds)

```bash
# 1. Build
make -C cmd/BonfyreTel

# 2. Terminal 1 — start mock ESL server (replaces FreeSWITCH)
./cmd/BonfyreTel/bonfyre-tel mock

# 3. Terminal 2 — start listener in dry-run mode
./cmd/BonfyreTel/bonfyre-tel listen --dry-run

# 4. Terminal 3 — inject events
./cmd/BonfyreTel/bonfyre-tel sim-call --from +15551234567 --to +15559876543
./cmd/BonfyreTel/bonfyre-tel sim-sms --from +15551234567 --to +15559876543 --body "hello world"

# 5. Watch Terminal 2 light up:
#   tel: [DRY-RUN] would trigger: bonfyre-pipeline run /tmp/bonfyre-sim/rec_2026-04-07.wav
#   tel: [DRY-RUN] would trigger: bonfyre-ingest --text "hello world"

# 6. Test phone verification (Twilio Verify replacement)
./cmd/BonfyreTel/bonfyre-tel verify-send --to +15559876543 --dry-run
#   verify: [DRY-RUN] code for +15559876543: 847291 (expires ...)
./cmd/BonfyreTel/bonfyre-tel verify-check --phone +15559876543 --code 847291
#   verified

# 7. Check the database
./cmd/BonfyreTel/bonfyre-tel status
```

No FreeSWITCH install needed. No SIP trunk. No phone number. No money.

## Architecture

```
Phone Network → SIP Trunk ($1/mo) → FreeSWITCH (MIT, self-hosted)
                                          ↓ Event Socket (TCP :8021)
                                     bonfyre-tel listen
                                          ↓
                           ┌──────────────┼──────────────┐
                     Recording done    SMS received    Call hangup
                           ↓              ↓              ↓
                   bonfyre-pipeline   bonfyre-ingest   SQLite log
                     (async fork)     (async fork)

Testing (no network needed):
  bonfyre-tel mock     → fake ESL server on :8021
  bonfyre-tel sim-call → inject fake call events
  bonfyre-tel sim-sms  → inject fake SMS events
  --dry-run            → log pipeline triggers without forking
```

## Production Quick Start

```bash
# 1. Build
make -C cmd/BonfyreTel

# 2. Install FreeSWITCH (macOS)
brew install freeswitch

# 3. Copy configs
cp cmd/BonfyreTel/deploy/dialplan-bonfyre.xml /usr/local/freeswitch/conf/dialplan/default/
cp cmd/BonfyreTel/deploy/sip-trunk.xml /usr/local/freeswitch/conf/sip_profiles/external/
# Edit sip-trunk.xml with your provider credentials

# 4. Start FreeSWITCH
freeswitch -nonat

# 5. Start listening
./cmd/BonfyreTel/bonfyre-tel listen
```

## Commands

### Production
| Command | Description |
|---------|-------------|
| `listen` | Connect to FreeSWITCH ESL, listen for call/SMS events |
| `send-sms` | Send SMS via FreeSWITCH SIP MESSAGE |
| `send-mms` | Send MMS via carrier REST API (curl) |
| `call` | Originate outbound call (optional `--record`) |
| `hangup` | Kill active call by UUID |
| `status` | Show call/message stats from SQLite |

### Testing (zero cost)
| Command | Description |
|---------|-------------|
| `mock` | Start fake ESL server — replaces FreeSWITCH entirely |
| `sim-call` | Inject a fake inbound call event into any listener |
| `sim-sms` | Inject a fake inbound SMS event into any listener |
| `mock --auto` | Auto-generate events every 5 seconds |
| `listen --dry-run` | Log pipeline triggers without actually forking |

### Verify (Twilio Verify replacement)
| Command | Description |
|---------|-------------|
| `verify-send` | Generate 6-digit code, send via SMS (10 min TTL) |
| `verify-check` | Validate code (max 5 attempts, auto-expiry) |

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--host` | `127.0.0.1` | FreeSWITCH ESL host |
| `--port` | `8021` | FreeSWITCH ESL port |
| `--password` | `ClueCon` | ESL password |
| `--db` | `~/.local/share/bonfyre/tel.db` | SQLite database path |
| `--from` | — | Caller/sender number |
| `--to` | — | Destination number |
| `--body` | — | Message text |
| `--media` | — | MMS media file/URL |
| `--record` | — | Record outbound call |
| `--uuid` | — | Call UUID (for hangup) |

## Event Flow

**Inbound call:**
1. SIP trunk routes call to FreeSWITCH
2. Dialplan answers, records to `.wav`
3. On record completion → ESL event `CHANNEL_EXECUTE_COMPLETE`
4. `bonfyre-tel` catches event → forks `bonfyre-pipeline run <file.wav>`
5. Pipeline: media-prep → transcribe → embed → store

**Inbound SMS:**
1. SIP MESSAGE arrives at FreeSWITCH
2. ESL fires `CUSTOM sms::recv` event
3. `bonfyre-tel` catches event → forks `bonfyre-ingest --text "..."`

**Outbound SMS:**
```bash
bonfyre-tel send-sms --from +15551234567 --to +15559876543 --body "Your transcript is ready"
```

## SIP Trunk Providers

| Provider | Voice $/min | SMS $/msg | DID $/mo | Notes |
|----------|-------------|-----------|----------|-------|
| Telnyx | $0.002 | $0.004 | $1.00 | Best API, SIP + REST |
| Bandwidth | $0.005 | $0.004 | $1.00 | Enterprise-grade |
| VoIP.ms | $0.01 | $0.01 | $0.85 | Budget option |
| SignalWire | $0.01 | $0.01 | $1.00 | FreeSWITCH creators |

## MMS Configuration

MMS requires carrier REST API (SIP doesn't support MMS natively):

```bash
export BONFYRE_TEL_MMS_ENDPOINT="https://api.telnyx.com/v2/messages"
export BONFYRE_TEL_API_KEY="your-api-key"

bonfyre-tel send-mms --from +15551234567 --to +15559876543 \
    --body "See attached" --media /path/to/file.pdf
```

## Cost Comparison

| | Twilio | BonfyreTel + Telnyx |
|---|--------|---------------------|
| Voice | $0.013/min | $0.002/min (85% less) |
| SMS | $0.0079/msg | $0.004/msg (49% less) |
| DID | $1.15/mo | $1.00/mo |
| MMS | $0.02/msg | $0.01/msg (50% less) |
| Vendor lock-in | Total | Zero |
| Code ownership | None | 100% |
| Test without paying | No | Yes (mock server) |
| Phone verification | $0.05/verify | Free (built-in) |

## Twilio Feature Parity

| Twilio Feature | BonfyreTel Equivalent | Status |
|----------------|----------------------|--------|
| Programmable Voice | `listen` + FreeSWITCH dialplan | Done |
| Programmable SMS | `send-sms` / `sim-sms` | Done |
| Programmable MMS | `send-mms` via carrier REST | Done |
| Verify (2FA) | `verify-send` / `verify-check` | Done |
| Call recording | FreeSWITCH `record` → pipeline | Done |
| Webhooks | ESL events → async fork | Done |
| Studio (flow builder) | FreeSWITCH XML dialplan | Native |
| Lookup (number info) | libphonenumber (open source) | Planned |
| TaskRouter (call center) | FreeSWITCH mod_callcenter | Native |
| Conversations | SIP MESSAGE threading | Planned |

Everything Twilio charges for, BonfyreTel does with open protocols and
commodity SIP trunks. The mock server means anyone can test the full
pipeline before investing a single dollar.
