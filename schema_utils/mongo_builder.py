from datetime import datetime

def build_mongo_validator(schema):
    properties = {}
    required = []

    for field, cfg in schema.items():
        prop = {}
        col_type = cfg["type"].lower()

        # Map neutral types to MongoDB types
        if col_type in ["int", "integer", "serial"]:
            prop["bsonType"] = "int"
        elif col_type in ["string", "varchar", "text"]:
            prop["bsonType"] = "string"
        elif col_type in ["datetime", "timestamp"]:
            prop["bsonType"] = "date"
        else:
            # fallback
            prop["bsonType"] = "string"

        if cfg.get("not_null"):
            required.append(field)

        properties[field] = prop

    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": required,
            "properties": properties
        }
    }

    return validator
def apply_defaults(schema, data):
    """
    Apply defaults from schema to data before insert
    """
    new_data = data.copy()
    
    for field, cfg in schema.items():
        if field not in new_data:
            default_val = cfg.get("default")
            if default_val is not None:
                if cfg["type"].lower() in ["datetime", "timestamp"] and default_val == "now":
                    new_data[field] = datetime.utcnow()
                else:
                    new_data[field] = default_val
    return new_data
