from datetime import datetime

def build_postgres_columns(columns):
    """
    Converts neutral schema to PostgreSQL column definitions
    """
    parts = []

    for col, cfg in columns.items():
        col_type = cfg["type"].lower()

        # Auto increment
        if cfg.get("auto_increment"):
            col_def = f"{col} SERIAL"
        else:
            # Type mapping
            if col_type == "string":
                pg_type = "VARCHAR(255)"
            elif col_type == "text":
                pg_type = "TEXT"
            elif col_type == "int":
                pg_type = "INT"
            elif col_type in ["datetime", "timestamp"]:
                pg_type = "TIMESTAMP"
            else:
                pg_type = col_type.upper()
            col_def = f"{col} {pg_type}"

        # Primary key
        if cfg.get("primary_key"):
            col_def += " PRIMARY KEY"

        # Not null
        if cfg.get("not_null"):
            col_def += " NOT NULL"

        # Unique
        if cfg.get("unique"):
            col_def += " UNIQUE"

        # Default handling
        if "default" in cfg and not cfg.get("auto_increment"):
            default_val = cfg["default"]

            if col_type in ["datetime", "timestamp"] and default_val == "now":
                col_def += " DEFAULT NOW()"
            elif isinstance(default_val, str):
                col_def += f" DEFAULT '{default_val}'"
            else:
                col_def += f" DEFAULT {default_val}"

        # Foreign key 
        fk = cfg.get("foreign_key")
        if fk:
            ref_table = fk["table"]
            ref_col = fk["column"]
            on_delete = fk.get("on_delete", "CASCADE")
            col_def += f" REFERENCES {ref_table}({ref_col}) ON DELETE {on_delete}"

        parts.append(col_def)

    return ", ".join(parts)
