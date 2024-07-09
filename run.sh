#!/bin/bash
source myenv/bin/activate
python3 extract.py 2>&1 | gawk '{print strftime("%Y-%m-%d %H:%M:%S"), $0; fflush() }' | tee -a output.log
