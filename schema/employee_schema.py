from schema.registry import register_schema

employee_schema = {
    "id": {"type": "INT", "primary_key": True, "auto_increment": True},
    "name": {"type": "VARCHAR(50)", "not_null": True},
    "father_name": {"type": "VARCHAR(50)", "not_null": True},
    "age": {"type": "INT"},
    "email": {"type": "VARCHAR(100)", "unique": True},
    "department": {"type": "VARCHAR(50)", "default": "General"},
    "joining_year": {"type": "INT", "default": 2025}
}

register_schema("employee", employee_schema)
