#!/usr/bin/env bash
# "Agent with undo, memory and forget" — goli in ~40 commands.
# One server, one URL (a local dir here; s3://... works the same).
set -euo pipefail
GOLI=${GOLI:-goli}
DATA=${DATA:-./agent-demo-data}
PORT=${PORT:-7071}
rm -rf "$DATA"
step() { printf '\n== %s\n' "$*"; }
lsn()  { jq -r .lsn; }

$GOLI serve --db "$DATA" --listen 127.0.0.1:$PORT 2>/tmp/goli-demo-server.log &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT
export GOLI_ADDR=http://127.0.0.1:$PORT
for i in $(seq 50); do curl -sf $GOLI_ADDR/v1/verify >/dev/null && break; sleep 0.1; done

step "1. Memory: the agent remembers things about alice (KV + doc + vector), tagged subject=alice"
$GOLI put user/alice/pref "oat milk, no sugar" --subject alice | jq -c .
$GOLI doc index memories user | jq -c .
$GOLI doc put memories m1 '{"user":"alice","text":"alice is allergic to peanuts"}' --subject alice | jq -c .
$GOLI doc put memories m2 '{"user":"bob","text":"bob prefers window seats"}' --subject bob | jq -c .
$GOLI vec put memvec m1 0.9,0.1,0.0 --subject alice | jq -c .
$GOLI vec put memvec m2 0.1,0.9,0.0 --subject bob | jq -c .
echo "-- recall: memories about alice";  $GOLI doc find memories user alice | jq -c '.[]'
echo "-- recall: nearest memory to [1,0,0]"; $GOLI vec search memvec 1,0,0 -k 2 | jq -c '.[]'

step "2. Tasks: a topic + consumer group; the result and the ack commit atomically in ONE transaction"
$GOLI pub tasks '{"id":"t1","do":"order lunch for alice"}' | jq -c .
$GOLI pub tasks '{"id":"t2","do":"book bob a flight"}' | jq -c .
$GOLI tx '[{"op":"put","key":"result/t1","value":"ordered salad (no peanuts)","subject":"alice"},
           {"op":"ack","topic":"tasks","group":"agent","offset":0}]' | jq -c .
echo "-- next unprocessed task for group 'agent' (t1 is already acked; this fetch also acks t2):"
$GOLI sub tasks --group agent --max 1 | jq -c '.[]'
GOOD=$($GOLI put checkpoint "before-mistake" | lsn)
echo "-- checkpoint at LSN $GOOD"

step "3. Undo: the agent goes rogue on main..."
$GOLI put user/alice/pref "ESPRESSO x4" --subject alice | jq -c .
$GOLI del result/t1 | jq -c .
echo "-- main now says: $($GOLI get user/alice/pref)"; $GOLI get result/t1 || true
echo "-- time travel: at LSN $GOOD alice's pref was: $($GOLI get user/alice/pref --at $GOOD)"
echo "-- rewind main to LSN $GOOD (old head is kept in the reflog, nothing is deleted)"
$GOLI rewind main $GOOD | jq -c .
echo "-- main says: $($GOLI get user/alice/pref) / result/t1 = $($GOLI get result/t1)"

step "4. Try before you buy: a risky plan on a branch, then promote it"
$GOLI branch create plan-b --from main | jq -c .
$GOLI put result/t2 "booked bob seat 12A" --branch plan-b | jq -c .
echo "-- main sees result/t2? "; $GOLI get result/t2 || true
echo "-- plan-b sees: $($GOLI get result/t2 --branch plan-b)"
$GOLI branch promote plan-b main | jq -c .
echo "-- after promote, main sees: $($GOLI get result/t2)"

step "5. Forget: alice invokes her right to erasure — crypto-shred subject 'alice'"
$GOLI shred alice | jq -c '{valid_signature, shred_lsn: .certificate.shred_lsn, dek_deleted: .certificate.dek_deleted, chain_head: (.certificate.chain_head[0:16]+"..."), signature: (.certificate.signature[0:16]+"...")}'
echo "-- KV:";            $GOLI get user/alice/pref || true
echo "-- time travel:";   $GOLI get user/alice/pref --at $GOOD || true
echo "-- old branch:";    $GOLI get result/t1 --branch plan-b || true
echo "-- docs about alice:"; $GOLI doc find memories user alice | jq -c '.'
echo "-- vector recall [1,0,0]:"; $GOLI vec search memvec 1,0,0 -k 2 | jq -c '.[]'
echo "-- bob is untouched: $($GOLI doc get memories m2)"
echo "-- new writes for alice are refused:"; $GOLI put user/alice/x y --subject alice || true

step "6. Verify the whole history (CRCs, hash chains, fencing, shred proofs)"
$GOLI verify | jq -c '{ok, logs, segments, entries, encrypted_values, unreadable_shredded_values, problems}'
kill $SERVER; wait $SERVER 2>/dev/null || true
echo "-- grep the raw bucket for alice's plaintext:"
if grep -rqa -e "peanuts" -e "oat milk" -e "ESPRESSO" "$DATA"; then echo "FOUND PLAINTEXT (bad)"; exit 1; else echo "no plaintext found in $DATA"; fi
