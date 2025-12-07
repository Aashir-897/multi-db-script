
class DbAdapter:
    def __init__(self, engie_class):
        self._engine = engie_class()
    
    def connect(self):
        return self._engine.connect()

    def execute_custom_query(self, query):
        if query.lower().startswith('select'):
            return self._engine.find(query)
        elif query.lower().startswith('insert'):
            return self._engine.insert(query)
        elif query.lower().startswith('update'):     
            return self._engine.update(query)
        elif query.lower().startswith('delete'):
            return self._engine.delete(query)
        else:
            print("Unsupported query type.")

    