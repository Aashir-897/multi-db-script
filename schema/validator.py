from schema.registry import get_schema
from datetime import datetime

def validate_data(table, data, action="insert"):
    schema = get_schema(table)

    if not schema:
        raise ValueError(f"No schema registered for table '{table}'")

    validated = {}

    for field, cfg in schema.items():

        # required field
        if cfg.get("not_null") and field not in data and action == "insert":
            raise ValueError(f"Missing required field: {field}")

        # default value
        if field not in data:
            if "default" in cfg:
                if cfg["type"].lower() in ["datetime", "timestamp"] and cfg["default"] == "now":
                    validated[field] = datetime.utcnow()  # <-- safe Python datetime
                else:
                    validated[field] = cfg["default"]
            continue
        value = data[field]

        col_type = cfg["type"]

        if "INT" in col_type and not isinstance(value, int):
            raise TypeError(f"{field} must be int")


        if "VARCHAR" in col_type and not isinstance(value, str):
            raise TypeError(f"{field} must be string")

        validated[field] = value

    for field in data:
        if field not in schema:
            raise ValueError(f"Unknown field: {field}")

    return validated
