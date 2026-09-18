#!/bin/bash
CLOUDFLARED="/usr/local/bin/cloudflared"
LOG="/tmp/zeus-tunnel.log"

$CLOUDFLARED tunnel --url http://localhost:8000 > "$LOG" 2>&1 &

# Wait for URL
for i in $(seq 1 15); do
    URL=$(grep -o 'https://[^[:space:]]*trycloudflare\.com' "$LOG" 2>/dev/null | head -1)
    if [ -n "$URL" ]; then
        echo "$URL" > /tmp/zeus-url.txt
        echo "ZEUS tunnel: $URL"
        break
    fi
    sleep 1
done

wait
