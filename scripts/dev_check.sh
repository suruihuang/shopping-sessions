#!/bin/sh
# Runs inside a Linux dev container (g++, python3 + polars): builds the binary and runs every
# fixture under several chunk sizes, thread counts and partition counts. All must match.
set -eu
cd "$(dirname "$0")/.."
mkdir -p /tmp/ssbuild
g++ -O2 -g -march=x86-64-v3 -std=c++20 -pthread -Wall -Wextra -o /tmp/ssbuild/pipeline src/main.cpp
for cfg in "NCPU=8" "NCPU=1" "NCPU=8 SS_CHUNK=1" "NCPU=3 SS_CHUNK=7 SS_PARTS=1" "NCPU=5 SS_CHUNK=64 SS_PARTS=64" "NCPU=4 SS_NOMMAP=1 SS_NOWC=1 SS_NOTHP=1 SS_NOFAST=1" "NCPU=8 SS_INTERN=1"; do
  echo "== $cfg"
  env $cfg python3 scripts/run_fixtures.py fixtures -- /tmp/ssbuild/pipeline 2>&1 | grep -v '^\[\|^user_daily rows\|^threads' | tail -4
done
