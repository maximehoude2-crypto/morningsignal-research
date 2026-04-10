"""
Git commit + push to trigger Cloudflare Pages auto-deploy.
"""

import subprocess
import sys
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent


def run(cmd: list[str], cwd: Path = BASE_DIR, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)
    if result.stdout:
        print(f"  {result.stdout.strip()}")
    if result.returncode != 0:
        if result.stderr:
            print(f"  stderr: {result.stderr.strip()}")
        if check:
            raise subprocess.CalledProcessError(result.returncode, cmd)
    return result


def deploy(dry_run: bool = False) -> bool:
    today = date.today().isoformat()

    # Check if docs/ has any changes
    status = run(["git", "status", "--porcelain", "docs/"], check=False)
    if not status.stdout.strip():
        print("No changes in docs/ to commit.")
        return True

    if dry_run:
        print(f"[DRY RUN] Would commit and push docs/ with message: 'Daily update {today}'")
        print("  (Skipping actual git operations in dry run mode)")
        return True

    commands = [
        ["git", "add", "docs/"],
        ["git", "commit", "-m", f"Daily update {today}"],
        ["git", "push", "origin", "main"],
    ]

    for cmd in commands:
        print(f"  Running: {' '.join(cmd)}")
        try:
            run(cmd)
        except subprocess.CalledProcessError as e:
            print(f"ERROR: Command failed: {' '.join(cmd)}")
            return False

    print(f"Deployed to GitHub → Cloudflare Pages auto-deploys in ~30 seconds")
    return True


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    ok = deploy(dry_run=dry)
    sys.exit(0 if ok else 1)
