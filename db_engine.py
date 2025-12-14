from db_interface import DatabaseInterface
import mysql.connector
import psycopg2
from pymongo import MongoClient



class MySqlDatabase(DatabaseInterface):
    def __init__(self):
        self._connection = None

    def connect(self):
        self._connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="custom"
        )
        return "Connected to MySQL database"

    def find(self, query):
        try:
            table = query["table"]
            where = query.get("where")

            sql = f"SELECT * FROM {table}"
            values = []

            if where:
                conditions = " AND ".join([f"{k}=%s" for k in where])
                sql += f" WHERE {conditions}"
                values = list(where.values())

            cursor = self._connection.cursor(dictionary=True)
            cursor.execute(sql, values)
            return cursor.fetchall()
        except Exception as e:
            return f"MySQL Finding Error: {e.msg}"  

    def insert(self, query):
        try:
            table = query["table"]
            data = query["data"]

            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            values = list(data.values())

            sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

            cursor = self._connection.cursor()
            cursor.execute(sql, values)
            self._connection.commit()
            cursor.close()

            return "MySQL: Record inserted"
        except Exception as e:
            return f"MySQL Inserting Error: {e.msg}"

    def update(self, query):
        try:
            table = query["table"]
            data = query["data"]
            where = query.get("where")

            set_clause = ", ".join([f"{k}=%s" for k in data])
            values = list(data.values())

            sql = f"UPDATE {table} SET {set_clause}"

            if where:
                conditions = " AND ".join([f"{k}=%s" for k in where])
                sql += f" WHERE {conditions}"
                values += list(where.values())

            cursor = self._connection.cursor()
            cursor.execute(sql, values)
            self._connection.commit()
            cursor.close()

            return "MySQL: Record updated"
        except Exception as e:
            return f"MySQL Updating Error: {e.msg}"

    def delete(self, query):
        try:
            table = query["table"]
            where = query.get("where")

            sql = f"DELETE FROM {table}"
            values = []

            if where:
                conditions = " AND ".join([f"{k}=%s" for k in where])
                sql += f" WHERE {conditions}"
                values = list(where.values())
            else:
                raise ValueError("DELETE without WHERE is not allowed")

            cursor = self._connection.cursor()
            cursor.execute(sql, values)
            self._connection.commit()
            cursor.close()

            return "MySQL: Record(s) deleted"

        except Exception as e:
            return f"MySQL Deleting Error: {e.msg}"


class PostgreSqlDatabase(DatabaseInterface):
    def __init__(self):
        self._connection = None

    def connect(self):
        self._connection = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="root",
            database="custom_post"
        )
        return "Connected to PostgreSQL database"

    def find(self, query):
        try:
            table = query["table"]
            where = query.get("where")

            sql = f"SELECT * FROM {table}"
            values = []

            if where:
                conditions = " AND ".join([f"{k}=%s" for k in where])
                sql += f" WHERE {conditions}"
                values = list(where.values())

            cursor = self._connection.cursor()
            cursor.execute(sql, values)

            cols = [c[0] for c in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            return f"PostgreSQL Finding Error: {e.msg}"
        
    def insert(self, query):
        try:
            table = query["table"]
            data = query["data"]

            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            values = list(data.values())

            sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

            cursor = self._connection.cursor()
            cursor.execute(sql, values)
            self._connection.commit()
            cursor.close()

            return "PostgreSQL: Record inserted"
        
        except Exception as e:
            return f"PostgreSQL Inserting Error: {e.msg}"


    def update(self, query):
        try:
            table = query["table"]
            data = query["data"]
            where = query.get("where")

            set_clause = ", ".join([f"{k}=%s" for k in data])
            values = list(data.values())

            sql = f"UPDATE {table} SET {set_clause}"

            if where:
                conditions = " AND ".join([f"{k}=%s" for k in where])
                sql += f" WHERE {conditions}"
                values += list(where.values())

            cursor = self._connection.cursor()
            cursor.execute(sql, values)
            self._connection.commit()
            cursor.close()

            return "PostgreSQL: Record updated"
        
        except Exception as e:
            return f"PostgreSQL updating  Error: {e.msg}"

    def delete(self, query):
        try:
            table = query["table"]
            where = query.get("where")

            sql = f"DELETE FROM {table}"
            values = []

            if where:
                conditions = " AND ".join([f"{k}=%s" for k in where])
                sql += f" WHERE {conditions}"
                values = list(where.values())
            else:
                raise ValueError("DELETE without WHERE is not allowed")

            cursor = self._connection.cursor()
            cursor.execute(sql, values)
            self._connection.commit()
            cursor.close()

            return "PostgreSQL: Record(s) deleted"
        
        except Exception as e:
            return f"PostgreSQL deleting Error: {e.msg}"

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
            collection.insert_one(query["data"])
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