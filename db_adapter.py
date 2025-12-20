from schema.registry import get_schema
from schema.validator import validate_data

class DbAdapter:    
    def __init__(self, engie_class):
        self._engine = engie_class()
    
    def connect(self):
        return self._engine.connect()

    def execute_custom_query(self, query):
        
        action = query["action"]

        if action == "find":
            if "join" in query:
                method = getattr(self._engine, "find_with_join")
            else:
                method = getattr(self._engine, "find")
            return method(query)
        if action in ["insert", "update"]:
            table = query["table"]
            data = query["data"]
            validated_data = validate_data(table, data, action)
            query["data"] = validated_data
            method = getattr(self._engine, action)
            return method(query)        
        elif action == "delete":
            return self._engine.delete(query)
        else:
            raise ValueError("Unsupported action")
    

    def migrate(self, table_name):
        schema = get_schema(table_name)

        if not schema:
            return f"No schema registered for {table_name}"

        if self._engine.table_exists(table_name):
            return self._engine.add_missing_columns(table_name, schema)
        else:
            return self._engine.create_from_schema(table_name, schema)