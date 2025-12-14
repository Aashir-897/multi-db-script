from db_interface import DatabaseInterface
from pymongo import MongoClient

class MongoDatabase(DatabaseInterface):
    def __init__(self):
        self._client = None
        self._db = None

    def connect(self):
        self._client = MongoClient("mongodb://localhost:27017")
        self._db = self._client["custom_mongo_db"]
        return "Connected to MongoDB database"

    def find(self, query):
        try:
            collection = self._db[query["table"]]
            return list(collection.find(query.get("where", {})))
        except Exception as e:
            return f"MongoDB Finding Error: {str(e)}"
        
    def insert(self, query):
        try:
            collection = self._db[query["table"]]
            data = query["data"]

            # Auto-generate id if not provided
            if "id" not in data:
                counter = self._db["counters"].find_one_and_update(
                    {"_id": query["table"]},
                    {"$inc": {"seq": 1}},
                    upsert=True,
                    return_document=True
                )
                data["id"] = counter["seq"]

            collection.insert_one(data)
            return "MongoDB: Document inserted"
        except Exception as e:
            return f"MongoDB Inserting Error: {str(e)}"

    def update(self, query):
        try:
            collection = self._db[query["table"]]
            collection.update_many(
                query.get("where", {}),
                {"$set": query["data"]}
            )
            return "MongoDB: Document updated"
        except Exception as e:
            return f"MongoDB Updating Error: {str(e)}"

    def delete(self, query):
        try:
            collection = self._db[query["table"]]
            collection.delete_many(query.get("where", {}))
            return "MongoDB: Document deleted"
        except Exception as e:  
            return f"MongoDB Deleting Error: {str(e)}"
        

    def table_create(self, query):
        try:
            table = query["table"]
            columns = query.get("columns", {})

            if table not in self._db.list_collection_names():
                # MongoDB type mapping & validation
                bson_props = {}
                required_fields = []

                for col_name, col_def in columns.items():
                    col_def_upper = col_def.upper()

                    # Map types
                    if "INT" in col_def_upper:
                        bson_type = "int"
                    elif "VARCHAR" in col_def_upper or "TEXT" in col_def_upper:
                        bson_type = "string"
                    else:
                        bson_type = "string"

                    # Check NOT NULL
                    if "NOT NULL" in col_def_upper or "PRIMARY KEY" in col_def_upper:
                        required_fields.append(col_name)

                    bson_props[col_name] = {"bsonType": bson_type}

                validator = {"$jsonSchema": {"bsonType": "object", "required": required_fields, "properties": bson_props}}
                self._db.create_collection(table, validator=validator)

                # Handle UNIQUE (create index)
                for col_name, col_def in columns.items():
                    if "UNIQUE" in col_def.upper():
                        self._db[table].create_index(col_name, unique=True)

            return f"MongoDB: Collection {table} created"
        except Exception as e:
            return f"MongoDB Table Creation Error: {str(e)}"

    
    def add_column(self, query):
        try:
            table = query["table"]
            columns = query.get("columns", {})

            # Update existing documents with default value 0 or None
            for col_name, col_def in columns.items():
                default_value = None
                if "INT" in col_def or "SERIAL" in col_def:
                    default_value = 0
                self._db[table].update_many({}, {"$set": {col_name: default_value}})

            # Update validator schema
            coll_options = self._db.command("listCollections", filter={"name": table})
            old_validator = coll_options['cursor']['firstBatch'][0].get('options', {}).get('validator', {})
            
            new_validator = old_validator.copy()
            props = new_validator.get("$jsonSchema", {}).get("properties", {})
            req = new_validator.get("$jsonSchema", {}).get("required", [])
            
            for col_name in columns.keys():
                props[col_name] = {"bsonType": "int"}  
                if col_name not in req:
                    req.append(col_name)

            new_validator["$jsonSchema"]["properties"] = props
            new_validator["$jsonSchema"]["required"] = req

            self._db.command("collMod", table, validator=new_validator)
            return f"MongoDB: Columns {', '.join(columns.keys())} added to {table}"
        except Exception as e:
            return f"MongoDB Add Column Error: {str(e)}"  
        
