def build_mongo_validator(schema):
    properties = {}
    required = []

    for name, cfg in schema.items():
        bson_type = "int" if "INT" in cfg["type"] else "string"

        properties[name] = {"bsonType": bson_type}

        if cfg.get("not_null") and name != "id":
            required.append(name)

    return {
        "$jsonSchema": {
            "bsonType": "object",
            "properties": properties,
            "required": required
        }
    }

def apply_defaults(schema, data):
    for name, cfg in schema.items():
        if name not in data and "default" in cfg:
            data[name] = cfg["default"]
    return data