from schema.registry import register_schema

users_schema = {
    "id": {"type": "int", "primary_key": True, "auto_increment": True},
    "name": {"type": "string", "not_null": True},
    "email": {"type": "string", "unique": True, "not_null": True},
    "created_at": {"type": "datetime", "default": "now"} 
}

register_schema("users", users_schema)
