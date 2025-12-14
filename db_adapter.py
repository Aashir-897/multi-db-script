
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
    