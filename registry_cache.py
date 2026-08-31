"""
Persistent SPXW contract registry cache.
Avoids re-downloading 20k+ definitions on every dashboard restart.
"""

import os
import json
import time
import logging
from typing import Callable, List, Dict, Any

CACHE_PATH = os.path.join("logs", "contract_registry.json")
MAX_AGE_SEC = 4 * 3600  # 4 hours


def save_registry(contracts: Dict[int, Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    payload = {
        "saved_at": time.time(),
        "contracts": [
            {"instrument_id": iid, **meta}
            for iid, meta in contracts.items()
        ],
    }
    with open(CACHE_PATH, "w") as f:
        json.dump(payload, f)
    logging.info(f"[RegistryCache] Saved {len(contracts):,} contracts to cache")


def load_registry(register_fn: Callable, max_age_sec: int = MAX_AGE_SEC) -> int:
    """Load cached contracts if fresh enough. Returns count loaded."""
    if not os.path.exists(CACHE_PATH):
        return 0
    try:
        with open(CACHE_PATH) as f:
            payload = json.load(f)
        age = time.time() - payload.get("saved_at", 0)
        if age > max_age_sec:
            logging.info(f"[RegistryCache] Cache expired ({age/3600:.1f}h old)")
            return 0
        count = 0
        for c in payload.get("contracts", []):
            iid = c.pop("instrument_id", None)
            if iid is not None:
                register_fn(
                    instrument_id=iid,
                    strike=c.get("strike", 0),
                    option_type=c.get("option_type", "C"),
                    symbol=c.get("symbol", ""),
                    expiration=c.get("expiration", ""),
                )
                count += 1
        logging.info(f"[RegistryCache] Loaded {count:,} contracts from cache ({age/60:.0f}m old)")
        return count
    except Exception as e:
        logging.warning(f"[RegistryCache] Failed to load cache: {e}")
        return 0
