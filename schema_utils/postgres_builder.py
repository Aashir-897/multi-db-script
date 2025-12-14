def build_postgres_columns(columns):
    parts = []

    for col, cfg in columns.items():

        if cfg.get("auto_increment"):
            col_def = f"{col} SERIAL"
        else:
            col_def = f"{col} {cfg['type']}"

        if cfg.get("primary_key"):
            col_def += " PRIMARY KEY"

        if cfg.get("not_null"):
            col_def += " NOT NULL"

        if cfg.get("unique"):
            col_def += " UNIQUE"

        if "default" in cfg and not cfg.get("auto_increment"):
            default_val = cfg["default"]
            if isinstance(default_val, str):
                default_val = f"'{default_val}'"
            col_def += f" DEFAULT {default_val}"

        parts.append(col_def)

    return ", ".join(parts)
