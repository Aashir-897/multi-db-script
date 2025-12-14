from schema.registry import get_schema

class DbAdapter:    
    def __init__(self, engie_class):
        self._engine = engie_class()
    
    def connect(self):
        return self._engine.connect()

    def execute_custom_query(self, query):
        
        action = query["action"]

        if action == "find":
            return self._engine.find(query)
        elif action == "insert":
            return self._engine.insert(query)
        elif action == "update":
            return self._engine.update(query)
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