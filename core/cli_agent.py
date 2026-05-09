"""Agent-oriented CLI contracts for Thoth.

This module keeps machine-readable CLI metadata and robot-facing payloads out
of the already-large top-level command file.
"""

from __future__ import annotations

import argparse
import difflib
import json
import sys
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any, Iterable


CONTRACT_VERSION = "2026-05-09"

EXIT_CODES = {
    0: "success",
    1: "user-input-error",
    2: "safety-block",
    3: "tool-environment-error",
    4: "upstream-failure",
    5: "conflict",
    130: "interrupted",
}

COMMON_FLAG_TYPOS = {
    "--jsno": "--json",
    "--jason": "--json",
    "--josn": "--json",
    "--jsonl": "--json",
}


@dataclass(frozen=True)
class NormalizedArgv:
    argv: list[str]
    warnings: list[str]


class AgentFriendlyArgumentParser(argparse.ArgumentParser):
    """Argparse subclass that turns common agent mistakes into teaching errors."""

    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        suggestion = self._suggestion(message)
        self.exit(
            1,
            f"{self.prog}: error: {message}\n"
            f"{suggestion or f'Try `{self.prog} --help` for available commands.'}\n",
        )

    def _suggestion(self, message: str) -> str | None:
        unknown_flag = _extract_unknown_flag(message)
        if unknown_flag:
            corrected = _closest_match(unknown_flag, self._known_options())
            if corrected:
                return (
                    f"Did you mean `{corrected}`? Suggested command: "
                    f"`{self.prog} {corrected}`"
                )

        invalid_choice = _extract_invalid_choice(message)
        if invalid_choice:
            commands = self._known_subcommands()
            corrected = _closest_match(invalid_choice, commands)
            if corrected:
                return (
                    f"Did you mean `{corrected}`? Suggested command: "
                    f"`{self.prog} {corrected} --help`"
                )
        return None

    def _known_options(self) -> list[str]:
        options: list[str] = []
        for action in self._actions:
            options.extend(action.option_strings)
        return options

    def _known_subcommands(self) -> list[str]:
        commands: list[str] = []
        for action in self._actions:
            if isinstance(action, argparse._SubParsersAction):
                commands.extend(action.choices.keys())
        return commands


def normalize_agent_argv(argv: Iterable[str]) -> NormalizedArgv:
    """Correct high-confidence flag typos before argparse handles the command."""

    normalized: list[str] = []
    warnings: list[str] = []
    for arg in argv:
        corrected = COMMON_FLAG_TYPOS.get(arg)
        if corrected:
            normalized.append(corrected)
            warnings.append(
                f"Interpreted `{arg}` as `{corrected}`. Use `{corrected}` next time."
            )
        else:
            normalized.append(arg)
    return NormalizedArgv(argv=normalized, warnings=warnings)


def collect_parser_capabilities(parser: argparse.ArgumentParser) -> dict[str, Any]:
    """Inspect argparse and return the command contract as JSON-serializable data."""

    commands = []
    for action in parser._actions:
        if not isinstance(action, argparse._SubParsersAction):
            continue
        for name, subparser in sorted(action.choices.items()):
            commands.append(
                {
                    "name": name,
                    "help": _parser_description(subparser),
                    "usage": subparser.format_usage().strip(),
                    "options": _parser_options(subparser),
                    "subcommands": _nested_subcommands(subparser),
                }
            )

    return {
        "schema_version": "1.0",
        "contract_version": CONTRACT_VERSION,
        "tool": "thoth",
        "entrypoints": ["python thoth.py", ".venv/bin/python thoth.py"],
        "command_count": len(commands),
        "commands": commands,
        "global_options": _parser_options(parser),
        "exit_codes": [
            {"code": code, "meaning": meaning}
            for code, meaning in sorted(EXIT_CODES.items())
        ],
        "agent_surfaces": {
            "archivist_benchmark_json": (
                "python thoth.py archivist --benchmark --limit 0 --json"
            ),
            "capabilities_json": "python thoth.py capabilities --json",
            "robot_docs": "python thoth.py robot-docs guide",
            "robot_triage": "python thoth.py --robot-triage",
            "stats_json": "python thoth.py stats --json",
            "db_stats_json": "python thoth.py db stats --json",
            "delete_dry_run": "python thoth.py delete <tweet_id> --dry-run",
            "wiki_lint_json": "python thoth.py wiki-lint --json",
            "wiki_query_json": "python thoth.py wiki-query <query> --json",
        },
        "stdout_stderr_contract": {
            "stdout": "requested data only, including JSON payloads",
            "stderr": "warnings, diagnostics, and corrected-invocation hints",
        },
    }


def render_capabilities(
    capabilities: dict[str, Any], *, as_json: bool, stream: Any = sys.stdout
) -> None:
    if as_json:
        json.dump(capabilities, stream, indent=2, sort_keys=True)
        stream.write("\n")
        return

    print("Thoth CLI capabilities", file=stream)
    print(f"Contract version: {capabilities['contract_version']}", file=stream)
    print("\nAgent commands:", file=stream)
    for name, command in capabilities["agent_surfaces"].items():
        print(f"  {name}: {command}", file=stream)
    print("\nUse `python thoth.py capabilities --json` for the full contract.", file=stream)


def build_robot_triage_payload(
    capabilities: dict[str, Any],
    *,
    config_valid: bool,
    config_issues: list[str] | None = None,
) -> dict[str, Any]:
    config_issues = config_issues or []
    return {
        "schema_version": "1.0",
        "contract_version": CONTRACT_VERSION,
        "tool": "thoth",
        "health": {
            "config_valid": config_valid,
            "issue_count": len(config_issues),
            "issues": config_issues,
        },
        "quick_ref": [
            "python thoth.py stats --json",
            "python thoth.py db stats --json",
            "python thoth.py archivist --benchmark --limit 0 --json",
            "python thoth.py wiki-lint --stale-after-days 30 --json",
            "python thoth.py delete <tweet_id> --dry-run",
        ],
        "recommended_next_commands": [
            {
                "intent": "Inspect machine-readable CLI contract",
                "command": "python thoth.py capabilities --json",
            },
            {
                "intent": "Read agent-specific operating guide",
                "command": "python thoth.py robot-docs guide",
            },
            {
                "intent": "Check local artifact and queue state",
                "command": "python thoth.py stats --json",
            },
        ],
        "commands": capabilities["commands"],
        "exit_codes": capabilities["exit_codes"],
    }


def render_robot_docs(stream: Any = sys.stdout) -> None:
    print("Thoth robot guide", file=stream)
    print("", file=stream)
    print("Canonical entrypoint:", file=stream)
    print("  python thoth.py <command> [options]", file=stream)
    print("", file=stream)
    print("Fast machine-readable probes:", file=stream)
    print("  python thoth.py --robot-triage", file=stream)
    print("  python thoth.py capabilities --json", file=stream)
    print("  python thoth.py stats --json", file=stream)
    print("  python thoth.py db stats --json", file=stream)
    print("  python thoth.py wiki-query <query> --json", file=stream)
    print("  python thoth.py wiki-lint --json", file=stream)
    print("  python thoth.py archivist --benchmark --limit 0 --json", file=stream)
    print("", file=stream)
    print("Safe mutation pattern:", file=stream)
    print("  Prefer --dry-run when available before running mutating commands.", file=stream)
    print("  For deletion, start with: python thoth.py delete <tweet_id> --dry-run", file=stream)
    print("", file=stream)
    print("Output contract:", file=stream)
    print("  JSON commands write JSON to stdout only.", file=stream)
    print("  Diagnostics, config warnings, and typo-correction hints go to stderr.", file=stream)
    print("", file=stream)
    print("Exit codes:", file=stream)
    for code, meaning in sorted(EXIT_CODES.items()):
        print(f"  {code}: {meaning}", file=stream)


def emit_json(payload: dict[str, Any], stream: Any = sys.stdout) -> None:
    json.dump(payload, stream, indent=2, sort_keys=True)
    stream.write("\n")


def to_jsonable(value: Any) -> Any:
    """Convert dataclasses, paths, and tuples into JSON-safe primitives."""

    if is_dataclass(value):
        return to_jsonable(asdict(value))
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    return value


def _extract_unknown_flag(message: str) -> str | None:
    marker = "unrecognized arguments:"
    if marker not in message:
        return None
    tail = message.split(marker, 1)[1].strip()
    return tail.split()[0] if tail else None


def _extract_invalid_choice(message: str) -> str | None:
    marker = "invalid choice:"
    if marker not in message:
        return None
    tail = message.split(marker, 1)[1].strip()
    if not tail:
        return None
    return tail.split()[0].strip("'\"")


def _closest_match(value: str, options: list[str]) -> str | None:
    matches = difflib.get_close_matches(value, options, n=1, cutoff=0.72)
    return matches[0] if matches else None


def _parser_options(parser: argparse.ArgumentParser) -> list[dict[str, Any]]:
    options = []
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction) or not action.option_strings:
            continue
        options.append(
            {
                "flags": list(action.option_strings),
                "dest": action.dest,
                "help": action.help,
                "required": bool(getattr(action, "required", False)),
            }
        )
    return options


def _nested_subcommands(parser: argparse.ArgumentParser) -> list[dict[str, str]]:
    nested = []
    for action in parser._actions:
        if not isinstance(action, argparse._SubParsersAction):
            continue
        for name, subparser in sorted(action.choices.items()):
            nested.append({"name": name, "help": _parser_description(subparser)})
    return nested


def _parser_description(parser: argparse.ArgumentParser) -> str:
    return parser.description or ""
