#!/usr/bin/env bash
while read -r pid; do kill "$pid"; done < stream_pids.txt
rm -f stream_pids.txt