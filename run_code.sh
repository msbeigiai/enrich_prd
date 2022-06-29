#!/bin/bash
# shellcheck disable=SC2164
cd
source devenv/bin/activate
cd
cd dev
python header_run.py
cd
cd dev
python run.py
cd
# shellcheck disable=SC2035
echo *** running header_run and run for HEADER and DETAIL ***
exec bash