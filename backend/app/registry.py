from pathlib import Path

from .models import RegistryItem


def _parse_definition(file_path: Path) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for raw_line in file_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        parsed[key.strip()] = value.strip()
    return parsed


def _to_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    return value.lower() == "true"


def load_registry(root: Path, kind: str) -> list[RegistryItem]:
    patterns = {
        "agents": "agents/*/agent.md",
        "skills": "skills/*/skills.md",
        "tools": "tools/*/tool.md",
        "runbooks": "runbooks/*/runbook.md",
    }
    pattern = patterns.get(kind)
    if not pattern:
        return []

    items: list[RegistryItem] = []

    for file_path in sorted(root.glob(pattern)):
        parsed = _parse_definition(file_path)
        items.append(
            RegistryItem(
                path=str(file_path.relative_to(root)),
                name=parsed.get("name", file_path.parent.name),
                item_id=parsed.get("id", parsed.get("name", file_path.parent.name)),
                version=parsed.get("version", "0.0.0"),
                role_or_category=parsed.get("role", parsed.get("category", parsed.get("plane", "unknown"))),
                approval_required=_to_bool(parsed.get("approval_required")),
                icon=parsed.get("icon", ""),
            )
        )

    return items
