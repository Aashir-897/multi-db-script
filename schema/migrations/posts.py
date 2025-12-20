from schema.registry import register_schema

posts_schema = {
    "id": {
        "type": "INT",
        "primary_key": True,
        "auto_increment": True
    },
    "user_id": {
        "type": "INT",
        "not_null": True,
        "foreign_key": {
            "table": "users",
            "column": "id",
            "on_delete": "CASCADE"
        }
    },
    "title": {
        "type": "VARCHAR(200)",
        "not_null": True
    },
    "content": {
        "type": "TEXT"
    },
    "created_at": {
        "type": "datetime",
        "default": "now"
           
    }
}

register_schema("posts", posts_schema)
