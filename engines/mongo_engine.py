from db_interface import DatabaseInterface
from pymongo import MongoClient, ReturnDocument
from schema_utils import build_mongo_validator, apply_defaults 


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
        

    def find_with_join(self, query):
        base_coll = self._db[query["table"]]
        pipeline = []

        for j in query.get("join", []):
            jt = j["table"]
            on = j["on"].split("=")
            local_field = on[0].split(".")[-1].strip()
            foreign_field = on[1].split(".")[-1].strip()
            as_field = jt

            # Lookup
            pipeline.append({
                "$lookup": {
                    "from": jt,
                    "localField": local_field,
                    "foreignField": foreign_field,
                    "as": as_field
                }
            })

            # Unwind so that join fields are top-level
            pipeline.append({"$unwind": f"${as_field}"})

            # Project only required fields
            proj = {}
            for field in j.get("select", []):
                if field.startswith(f"{jt}."):
                    proj[field.split(".")[1]] = f"${as_field}.{field.split('.')[1]}"
                else:
                    proj[field.split(".")[1]] = f"${field.split('.')[1]}"
            if proj:
                pipeline.append({"$project": proj})

        return list(base_coll.aggregate(pipeline))
        

    def insert(self, query):
        try:
            collection = self._db[query["table"]]
            data = query["data"]
            schema = query.get("schema") 

            if "id" not in data:
                counter = self._db["counters"].find_one_and_update(
                    {"_id": query["table"]},
                    {"$inc": {"seq": 1}},
                    upsert=True,
                    return_document=ReturnDocument.AFTER
                )
                data["id"] = counter["seq"]

            # Apply defaults from schema
            if schema:
                data = apply_defaults(schema, data)

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
        

    
    def table_exists(self, table):
        return table in self._db.list_collection_names()


    def create_from_schema(self, table, schema):
        validator = build_mongo_validator(schema)

        self._db.create_collection(
            table,
            validator=validator
        )

        # UNIQUE indexes
        for field, cfg in schema.items():
            if cfg.get("unique"):
                self._db[table].create_index(field, unique=True)

        return f"MongoDB: Collection {table} created via migration"
    

    def add_missing_columns(self, table, schema):
        collection = self._db[table]

        sample = collection.find_one() or {}
        existing_fields = set(sample.keys())

        added = []

        for field, cfg in schema.items():
            if field in existing_fields:
                continue

            if "default" in cfg:
                collection.update_many(
                    {field: {"$exists": False}},
                    {"$set": {field: cfg["default"]}}
                )

            added.append(field)

        # 3️⃣ Validator update
        validator = build_mongo_validator(schema)

        self._db.command(
            "collMod",
            table,
            validator=validator
        )

        if added:
            return f"MongoDB: Added fields {added}"
        return "MongoDB: Schema already up to date"