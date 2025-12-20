def build_mysql_columns(schema):
    """
    Converts neutral schema to MySQL column definitions
    """
    cols = []
    for col_name, cfg in schema.items():
        col_type = cfg["type"].lower()
        
        # Type mapping
        if col_type == "string":
            mysql_type = "VARCHAR(255)"
        elif col_type == "text":
            mysql_type = "TEXT"
        elif col_type == "int":
            mysql_type = "INT"
        elif col_type == "datetime":
            mysql_type = "DATETIME"
        else:
            mysql_type = col_type.upper()
        
        col_parts = [col_name, mysql_type]

        # Primary key
        if cfg.get("primary_key"):
            col_parts.append("PRIMARY KEY")
        if cfg.get("auto_increment"):
            col_parts.append("AUTO_INCREMENT")
        if cfg.get("not_null"):
            col_parts.append("NOT NULL")
        if cfg.get("unique"):
            col_parts.append("UNIQUE")

        # Default handling
        default = cfg.get("default")
        if default is not None:
            if col_type in ["datetime"] and default == "now":
                col_parts.append("DEFAULT CURRENT_TIMESTAMP")
            elif isinstance(default, str):
                col_parts.append(f"DEFAULT '{default}'")
            else:
                col_parts.append(f"DEFAULT {default}")

        # Foreign key (optional, simple version)
        fk = cfg.get("foreign_key")
        if fk:
            ref_table = fk["table"]
            ref_col = fk["column"]
            on_delete = fk.get("on_delete", "CASCADE")
            col_parts.append(f"REFERENCES {ref_table}({ref_col}) ON DELETE {on_delete}")

        cols.append(" ".join(col_parts))

    return ", ".join(cols)
