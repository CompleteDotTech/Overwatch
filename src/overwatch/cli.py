"""Command-line entrypoint for Overwatch."""

from overwatch.app import main

__all__ = ["main"]


if __name__ == "__main__":
    raise SystemExit(main())
