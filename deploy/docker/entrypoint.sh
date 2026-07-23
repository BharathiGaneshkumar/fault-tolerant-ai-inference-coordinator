#!/bin/sh
POD_NAME=$(hostname)
POD_INDEX=$(echo "$POD_NAME" | grep -o '[0-9]*$')
NODE_ID=$((POD_INDEX + 1))

PEERS=""
for i in 0 1 2; do
	if [ "$i" != "$POD_INDEX" ]; then
		PEER_ID=$((i + 1))
		if [ -n "$PEERS" ]; then
			PEERS="$PEERS,"
		fi
		PEERS="$PEERS$PEER_ID=coordinator-$i.coordinator:50051"
	fi
done

exec /app/coordinator -id="$NODE_ID" -port=50051 -peers="$PEERS" -clustersize=3 -replicas="replica-0.replica:9001,replica-1.replica:9001,replica-2.replica:9001"