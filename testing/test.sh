#!/usr/bin/env bash
# End-to-end test: starts the server, runs the send and subscribe clients, and checks what the subscribers received.
set -u

cd "$(dirname "$0")/.."

ADDR=localhost:5672
TMP=$(mktemp -d)
PIDS=()
FAILED=0

cleanup() {
	for pid in ${PIDS[@]+"${PIDS[@]}"}; do kill "$pid" 2>/dev/null; done
	wait 2>/dev/null
	rm -rf "$TMP"
}
trap cleanup EXIT

pass() { echo "PASS: $1"; }
fail() { echo "FAIL: $1"; FAILED=1; }

count() { grep -c "received MESSAGE" "$1" 2>/dev/null || true; }

# wait_for FILE N: wait up to 5 seconds until FILE has N received messages.
wait_for() {
	for _ in $(seq 50); do
		[ "$(count "$1")" -ge "$2" ] && return 0
		sleep 0.1
	done
	return 1
}

send() { "$TMP/send" -addr "$ADDR" -body "$1" 2>/dev/null; }

subscribe() {
	"$TMP/subscribe" -addr "$ADDR" -id "$1" >"$TMP/$1.log" 2>&1 &
	PIDS+=($!)
}

if lsof -nP -iTCP:5672 -sTCP:LISTEN >/dev/null; then
	echo "port 5672 is already in use, stop the other server first"
	exit 1
fi

go build -o "$TMP/minimq" . || exit 1
go build -o "$TMP/send" ./testing || exit 1
go build -o "$TMP/subscribe" ./testing/subscribe || exit 1
go build -o "$TMP/unsubscribe" ./testing/unsubscribe || exit 1

"$TMP/minimq" >"$TMP/server.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 50); do
	nc -z localhost 5672 2>/dev/null && break
	sleep 0.1
done

# Runs first: it removes its own subscribers when done, so the queue is empty again for the next tests.
echo "Test 0: UNSUBSCRIBE"
"$TMP/unsubscribe" -addr "$ADDR" || FAILED=1

echo "Test 1: messages sent before anyone subscribes wait in the queue"
send "early-1"
send "early-2"
subscribe sub-a
if wait_for "$TMP/sub-a.log" 2 && grep -q '"early-1"' "$TMP/sub-a.log" && grep -q '"early-2"' "$TMP/sub-a.log"; then
	pass "sub-a got both waiting messages"
else
	fail "sub-a got $(count "$TMP/sub-a.log") of 2 waiting messages"
fi

echo "Test 2: messages sent after subscribing are delivered right away"
for i in 1 2 3; do send "live-$i"; done
if wait_for "$TMP/sub-a.log" 5; then
	pass "sub-a got all 3 new messages"
else
	fail "sub-a got $(($(count "$TMP/sub-a.log") - 2)) of 3 new messages"
fi

echo "Test 3: two subscribers share the messages, none lost and none doubled"
subscribe sub-b
sleep 0.3
for i in $(seq 10); do send "shared-$i"; done
sleep 1
a=$(grep -c '"shared-' "$TMP/sub-a.log" || true)
b=$(grep -c '"shared-' "$TMP/sub-b.log" || true)
if [ $((a + b)) -eq 10 ]; then
	pass "10 messages split as sub-a=$a, sub-b=$b"
else
	fail "expected 10 messages in total, got sub-a=$a, sub-b=$b"
fi

echo
echo "--- server log ---"
cat "$TMP/server.log"
echo "--- sub-a log ---"
cat "$TMP/sub-a.log"
echo "--- sub-b log ---"
cat "$TMP/sub-b.log"

exit $FAILED
