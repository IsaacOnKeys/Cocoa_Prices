#!/usr/bin/env bash
python3 local_cocoa_stream.py   > cocoa.log   2>&1 & echo $! >  stream_pids.txt
python3 local_oil_stream.py     > oil.log     2>&1 & echo $! >> stream_pids.txt
python3 local_weather_stream.py > weather.log 2>&1 & echo $! >> stream_pids.txt


