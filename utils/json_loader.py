import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

# ── Project root is two levels up from this file (utils/json_loader.py)
_PROJECT_ROOT = Path(__file__).parent.parent


def load_env(env_path: Optional[Path] = None) -> None:
    """Load .env file from project root into os.environ."""
    if env_path is None:
        env_path = _PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key   = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and value:
                os.environ[key] = value


def load_json(path: str, default: Any = None) -> Any:
    """Load a JSON file, returning `default` on any error."""
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = _PROJECT_ROOT / path
    try:
        with open(resolved, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️  Could not load {resolved}: {e}")
        return default


def load_programs(programs_file: Optional[str] = None) -> Dict[str, Any]:
    """Load programs.json, returning a safe default on failure."""
    path = programs_file or os.getenv("PROGRAMS_FILE", "data/programs.json")
    data = load_json(path)
    if data is None:
        return {
            "known_programs": {},
            "aggregator_programs": [],
            "swap_programs": [],
            "token_programs": [
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
            ],
        }
    return data


def load_learned_programs(learned_file: Optional[str] = None) -> Dict[str, Any]:
    path = learned_file or os.getenv("LEARNED_PROGRAMS_FILE", "runtime/learned_programs.json")
    return load_json(path, default={})


def save_learned_programs(cache: Dict[str, Any], learned_file: Optional[str] = None) -> None:
    path = learned_file or os.getenv("LEARNED_PROGRAMS_FILE", "runtime/learned_programs.json")
    resolved = Path(path)
    if not resolved.is_absolute():
        resolved = _PROJECT_ROOT / path
    resolved.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(resolved, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"⚠️  Could not save {resolved}: {e}")