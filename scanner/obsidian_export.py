from __future__ import annotations

"""
Write generated briefs into an Obsidian vault as clean Markdown notes.

The vault location is configurable via the OBSIDIAN_VAULT_DIR environment
variable:

- Set it to a folder inside your Obsidian vault (e.g. an iCloud/Dropbox-backed
  path, or a folder the Obsidian Git plugin pulls) to drop notes straight in.
- Leave it unset and notes are written under <repo>/vault/, which a cloud
  runner can commit so the Obsidian Git plugin can pull them down.

Notes are plain Markdown with YAML frontmatter so Obsidian indexes them with
proper dates, tags, and properties.
"""

import os
from datetime import date, datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

# Subfolder (relative to the vault root) where earnings notes are filed.
EARNINGS_SUBDIR = os.getenv("OBSIDIAN_EARNINGS_SUBDIR", "Earnings")


def vault_root() -> Path:
    configured = os.getenv("OBSIDIAN_VAULT_DIR")
    return Path(configured).expanduser() if configured else BASE_DIR / "vault"


def _yaml_escape(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _frontmatter(fields: dict) -> str:
    lines = ["---"]
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            items = ", ".join(_yaml_escape(str(v)) for v in value)
            lines.append(f"{key}: [{items}]")
        elif isinstance(value, (int, float)):
            lines.append(f"{key}: {value}")
        else:
            lines.append(f"{key}: {_yaml_escape(str(value))}")
    lines.append("---")
    return "\n".join(lines)


def write_earnings_note(
    target_date: str,
    session: str,
    body_markdown: str,
    *,
    tickers: list[str] | None = None,
) -> Path:
    """Write one earnings brief as an Obsidian note and return its path."""
    session_label = "Pre-Market" if session == "AM" else "Post-Close"
    note_dir = vault_root() / EARNINGS_SUBDIR
    note_dir.mkdir(parents=True, exist_ok=True)

    frontmatter = _frontmatter({
        "title": f"Earnings — {target_date} {session_label}",
        "date": target_date,
        "session": session,
        "type": "earnings-brief",
        "tickers": tickers or None,
        "tags": ["earnings", "morningsignal"],
        "generated": datetime.now().isoformat(timespec="seconds"),
        "source": "MorningSignal earnings pipeline",
    })

    note_path = note_dir / f"{target_date}_{session}.md"
    note_path.write_text(frontmatter + "\n\n" + body_markdown.strip() + "\n", encoding="utf-8")
    return note_path


if __name__ == "__main__":
    sample = "# Earnings Deep Dive\n\nSample body."
    path = write_earnings_note(date.today().isoformat(), "AM", sample, tickers=["AAPL", "MSFT"])
    print(f"Wrote {path}")
