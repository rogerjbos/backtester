#!/bin/bash
# ./run_all.sh crypto > run.log 2>&1

# Usage: ./run_all.sh [UNIVERSE...]
# Examples:
#   ./run_all.sh            # runs all universes (SC MC LC Micro Crypto)
#   ./run_all.sh SC MC      # runs SC and MC only
#   ./run_all.sh all        # runs all universes
#   ./run_all.sh SC,MC      # comma-separated also supported
#   ./run_all.sh --help     # show this help

show_help() {
    sed -n '1,120p' "$0" | sed -n '2,12p'
}

# Define available universes in the order you want them run
ALL_UNIVERSES=(SC MC LC Micro Crypto)

# If no args provided, run all
if [ "$#" -eq 0 ]; then
    SELECTED=("${ALL_UNIVERSES[@]}")
else
    # Collect requested universes (allow comma-separated values)
    SELECTED=()
    for a in "$@"; do
        case "$a" in
            -h|--help)
                show_help
                exit 0
                ;;
            *,*)
                IFS=',' read -ra parts <<< "$a"
                for p in "${parts[@]}"; do
                    SELECTED+=("$p")
                done
                ;;
            *)
                SELECTED+=("$a")
                ;;
        esac
    done
fi

# Normalize and validate selected universes
NORMALIZED=()
for u in "${SELECTED[@]}"; do
    # Trim whitespace and normalize to lowercase for case-insensitive matching
    u_trimmed=$(echo "$u" | tr -d '[:space:]')
    u_lc=$(echo "$u_trimmed" | tr '[:upper:]' '[:lower:]')

    # Allow 'all' to mean all universes
    if [ "$u_lc" = "all" ]; then
        NORMALIZED=("${ALL_UNIVERSES[@]}")
        break
    fi

    # Match against lowercase values and map to canonical names
    case "$u_lc" in
        sc)
            NORMALIZED+=(SC)
            ;;
        mc)
            NORMALIZED+=(MC)
            ;;
        lc)
            NORMALIZED+=(LC)
            ;;
        micro)
            NORMALIZED+=(Micro)
            ;;
        crypto)
            NORMALIZED+=(Crypto)
            ;;
        *)
            echo "Warning: unknown universe '$u_trimmed' - skipping"
            ;;
    esac
done

# If nothing valid, print help and exit
if [ "${#NORMALIZED[@]}" -eq 0 ]; then
    echo "No valid universes specified. Use '--help' for usage."
    exit 1
fi

# Run selected universes in order
for u in "${NORMALIZED[@]}"; do
    echo "Starting ${u} backtest at $(date)"
    ./target/release/backtester -u "$u" -m testing
    EXIT_CODE=$?
    echo "${u} backtest finished with exit code $EXIT_CODE at $(date)"
done

