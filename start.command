#!/bin/bash
cd "$(dirname "$0")"
python3 run_web.py &
sleep 2
open "http://127.0.0.1:8000"
wait
