
SCHEMA_REGISTRY = {}

def register_schema(name, schema):
    SCHEMA_REGISTRY[name] = schema

def get_schema(name):
    return SCHEMA_REGISTRY.get(name)
