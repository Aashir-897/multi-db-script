def build_mysql_columns(columns):
    parts = []

    for col, cfg in columns.items():
        col_def = f"{col} {cfg['type']}"

        if cfg.get("primary_key"):
            col_def += " PRIMARY KEY"

        if cfg.get("auto_increment"):
            col_def += " AUTO_INCREMENT"

        if cfg.get("not_null"):
            col_def += " NOT NULL"

        if cfg.get("unique"):
            col_def += " UNIQUE"

        if "default" in cfg:
            default_val = cfg["default"]
            if isinstance(default_val, str):
                default_val = f"'{default_val}'"
            col_def += f" DEFAULT {default_val}"

        parts.append(col_def)

    return ", ".join(parts)
