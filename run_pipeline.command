#!/bin/bash
cd /Users/max/morningsignal-research
rm -f .git/HEAD.lock .git/index.lock
/Users/max/morningsignal-research/.venv/bin/python3 run_daily.py 2>&1 | tee logs/openai_run_$(date +%Y%m%d_%H%M%S).log
echo ""
echo "=== DONE ==="
