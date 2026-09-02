#!/bin/bash
# Remote half of `run-monitor-health` — runs on the node under test, fed to
# `bash -s` over ssh. Kept as its own file so the awk below needs no escaping.
#
# Env: MODE=once|loop, INTERVAL, P2P_PORT, PROM_PORT.
# Emits CSV (header + rows) in loop mode, one CSV row in once mode.

MODE=${MODE:-once}
INTERVAL=${INTERVAL:-15}
P2P_PORT=${P2P_PORT:-30333}
PROM_PORT=${PROM_PORT:-9615}

PID=$(pgrep -x polkadot | head -1)
if [ -z "$PID" ]; then
    echo "polkadot is not running" >&2
    exit 1
fi

FIELDS="epoch_ms,cpu_jiffies,rss_kb,fds,conns,light_ok,light_fail_fullchan,light_fail_rejected,warp_ok,warp_fail_fullchan,best,finalized,sync_target,peers,grandpa_round,import_sum,import_count,verify_sum,verify_count,conn_in_opened,conn_in_closed,tc_local_hits_node,tc_local_att_node,tc_shared_hits_node,tc_shared_att_node,tc_local_hits_value,tc_local_att_value,tc_shared_hits_value,tc_shared_att_value,state_cache_bytes"

snapshot() {
    local now_ms cpu rss fds conns
    now_ms=$(date +%s%3N)

    # utime+stime are fields 14+15 of /proc/pid/stat. Raw jiffies, not a
    # percentage: diff two samples and divide by the elapsed time and CLK_TCK.
    # (`ps %cpu` is a lifetime average and is useless on a long-lived process.)
    read -r -a stat < "/proc/$PID/stat"
    cpu=$(( stat[13] + stat[14] ))
    rss=$(awk '/VmRSS/ { print $2 }' "/proc/$PID/status")
    fds=$(ls "/proc/$PID/fd" 2>/dev/null | wc -l)
    conns=$(ss -tnH state established "( sport = :$P2P_PORT )" | wc -l)

    curl -s --max-time 5 "localhost:$PROM_PORT/metrics" | awk \
        -v now="$now_ms" -v cpu="$cpu" -v rss="$rss" -v fds="$fds" -v conns="$conns" '
      function num() { return $NF }
      # The light/2 and sync/warp protocol labels embed the genesis hash, so
      # match on the suffix instead of a hardcoded protocol name.
      /^substrate_sub_libp2p_requests_in_success_total_count/ {
          if ($0 ~ /\/light\/2/)   light_ok = num()
          if ($0 ~ /\/sync\/warp/) warp_ok  = num()
      }
      /^substrate_sub_libp2p_requests_in_failure_total/ {
          if ($0 ~ /\/light\/2/   && $0 ~ /full channel/) light_full = num()
          if ($0 ~ /\/light\/2/   && $0 ~ /rejected/)     light_rej  = num()
          if ($0 ~ /\/sync\/warp/ && $0 ~ /full channel/) warp_full  = num()
      }
      /^substrate_block_height/ {
          if ($0 ~ /status="best"/)        best = num()
          if ($0 ~ /status="finalized"/)   fin  = num()
          if ($0 ~ /status="sync_target"/) tgt  = num()
      }
      /^substrate_sub_libp2p_peers_count/                   { peers = num() }
      /^substrate_finality_grandpa_round/                   { gr    = num() }
      /^substrate_block_verification_and_import_time_sum/   { imp_s = num() }
      /^substrate_block_verification_and_import_time_count/ { imp_c = num() }
      /^substrate_block_verification_time_sum/              { ver_s = num() }
      /^substrate_block_verification_time_count/            { ver_c = num() }
      /^substrate_sub_libp2p_connections_opened_total/ { if ($0 ~ /direction="in"/) c_in_o = num() }
      /^substrate_sub_libp2p_connections_closed_total/ { if ($0 ~ /direction="in"/) c_in_c = num() }
      /^trie_cache_local_fetch_attempts/  { if ($0 ~ /"node"/) tlan = num(); else tlav = num() }
      /^trie_cache_local_hits/            { if ($0 ~ /"node"/) tlhn = num(); else tlhv = num() }
      /^trie_cache_shared_fetch_attempts/ { if ($0 ~ /"node"/) tsan = num(); else tsav = num() }
      /^trie_cache_shared_hits/           { if ($0 ~ /"node"/) tshn = num(); else tshv = num() }
      /^substrate_state_cache_bytes/      { scb = num() }
      END {
          printf "%s,%s,%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.4f,%d,%.4f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
              now, cpu, rss, fds, conns,
              light_ok, light_full, light_rej, warp_ok, warp_full,
              best, fin, tgt, peers, gr,
              imp_s, imp_c, ver_s, ver_c, c_in_o, c_in_c,
              tlhn, tlan, tshn, tsan, tlhv, tlav, tshv, tsav, scb
      }'
}

if [ "$MODE" = once ]; then
    # key=value, so a caller can diff two snapshots without tracking columns.
    row=$(snapshot)
    i=1
    while IFS= read -r f; do
        echo "$f=$(echo "$row" | cut -d, -f$i)"
        i=$((i + 1))
    done < <(echo "$FIELDS" | tr ',' '\n')
    exit 0
fi

echo "$FIELDS"
while kill -0 "$PID" 2>/dev/null; do
    snapshot
    sleep "$INTERVAL"
done
