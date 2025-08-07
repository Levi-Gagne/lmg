# utils/jsonConfig.py

import json
from typing import Any

class JSONConfig:
    """
    Utility for safe JSON serialization of Databricks SDK objects.
    """

    @staticmethod
    def safe_for_json(obj: Any) -> Any:
        if hasattr(obj, "as_dict"):
            try:
                return JSONConfig.safe_for_json(obj.as_dict())
            except Exception:
                return str(obj)
        elif isinstance(obj, dict):
            return {k: JSONConfig.safe_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [JSONConfig.safe_for_json(x) for x in obj]
        elif hasattr(obj, "value"):
            return obj.value
        elif isinstance(obj, (str, int, float, bool)) or obj is None:
            return obj
        else:
            return str(obj)