"""Configuration for vendor region analysis.

This module provides default settings and helper functions for loading configuration
parameters used in vendor region analysis.
"""

from pathlib import Path
from typing import Dict, Tuple

import yaml

# ────────────────────────────────────────────────────────────────────────────
# Configuration defaults
# ────────────────────────────────────────────────────────────────────────────
DEFAULT_CFG: Dict = {
    "country": "CN",                     # ISO country code to analyze
    "spend_threshold": 1_000,            # Minimum $ exposure to keep a row
    "tariff_prefix_file": Path("config/tariff_exclusions.yml"),
    # Price‑delta buckets
    "shift_now_threshold": 0.50,         # ≤50% premium counts as shift now
}

def load_yaml_prefixes(path: Path) -> Tuple[str, ...]:
    """Load HTS prefixes from YAML; fall back to hard‑coded defaults."""
    default_prefixes = (
        "8517.13.00", "8471", "8517.62.00", "8473.3",
        "8528.52.00", "8542", "8486", "8524", "8523.51.00",
        "8541.10.00", "8541.21.00", "8541.29.00", "8541.30.00",
        "8541.49.10", "8541.49.70", "8541.49.80", "8541.49.95",
        "8541.51.00", "8541.59.00", "8541.90.00",
    )
    try:
        content = yaml.safe_load(path.read_text())
        return tuple(content.get("prefixes", default_prefixes))
    except FileNotFoundError:
        return default_prefixes 