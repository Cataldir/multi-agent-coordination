"""Evaluate one intentional starter/reference contract after admission."""
from __future__ import annotations
import argparse
import os
import unittest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--variant", choices=("starter", "solution"), required=True)
    parser.add_argument("--lab", choices=("11", "22", "27", "33"), required=True)
    args = parser.parse_args()
    os.environ["COURSE_EXERCISES"] = ("starters.exercises" if args.variant == "starter"
                                      else "solutions.exercises")
    suite = unittest.defaultTestLoader.loadTestsFromName(
        f"tests.test_starter_solution.ExerciseContractTests.test_lab{args.lab}")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
