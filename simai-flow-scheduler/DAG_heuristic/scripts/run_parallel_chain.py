"""Convenience entry point for the parallel-chain scenario."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from DAG_heuristic.run import main


if __name__ == "__main__":
    sys.argv.insert(1, "parallel_chain")
    main()
