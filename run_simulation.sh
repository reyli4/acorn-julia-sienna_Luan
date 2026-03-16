#!/usr/bin/env bash
# run_simulation.sh — Parse and simulate each year sequentially.
# Usage: ./run_simulation.sh [config_path]
set -euo pipefail

CONFIG="${1:-config/simulation.json}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Read sim_years from config and expand to a list
RAW=$(grep '"sim_years"' "$CONFIG" | sed 's/.*"sim_years"[[:space:]]*:[[:space:]]*//' | tr -d ' ' | tr -d ',')

if echo "$RAW" | grep -q ':'; then
    # Range syntax: "1982:2018"
    START=$(echo "$RAW" | tr -d '"' | cut -d: -f1)
    END=$(echo "$RAW"   | tr -d '"' | cut -d: -f2)
    SIM_YEARS=$(seq "$START" "$END")
elif echo "$RAW" | grep -q '\['; then
    # Array syntax: [1989, 1994, 1997]
    SIM_YEARS=$(echo "$RAW" | tr -d '[]' | tr ',' '\n')
else
    # Single year: 2012
    SIM_YEARS=$(echo "$RAW" | tr -d '"')
fi

echo "Simulation years: $SIM_YEARS"
echo ""

for YEAR in $SIM_YEARS; do
    echo "=== Year $YEAR: parsing ==="
    julia --project src/SystemParsing.jl "$CONFIG" "$YEAR"
    echo ""
    echo "=== Year $YEAR: simulation ==="
    julia --project src/SystemSimulation.jl "$CONFIG" "$YEAR"
    echo ""
done

echo "=== All years complete ==="
