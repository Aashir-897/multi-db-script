from db_interface import DatabaseInterface
import mysql.connector
from schema_utils import build_mysql_columns  


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
            return f"MySQL Finding Error: {str(e)}"  

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
            return f"MySQL Inserting Error: {str(e)}"

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
            return f"MySQL Updating Error: {str(e)}"

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
            return f"MySQL Deleting Error: {str(e)}"

    def table_exists(self, table):
        cursor = self._connection.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            AND table_name = %s
        """, (table,))
        exists = cursor.fetchone()[0] > 0
        cursor.close()
        return exists
    


    def create_from_schema(self, table, schema):
        
        cols = build_mysql_columns(schema)
        sql = f"CREATE TABLE {table} ({cols})"

        cursor = self._connection.cursor()
        cursor.execute(sql)
        self._connection.commit()
        cursor.close()

        return f"MySQL: Table {table} created via migration"


    def add_missing_columns(self, table, schema):
        cursor = self._connection.cursor()

        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
        """, (table,))

        existing_cols = {row[0] for row in cursor.fetchall()}

        added = []

        for col, cfg in schema.items():
            if col in existing_cols:
                continue

            col_def = build_mysql_columns({col: cfg})
            sql = f"ALTER TABLE {table} ADD COLUMN {col_def}"
            cursor.execute(sql)
            added.append(col)

        self._connection.commit()
        cursor.close()

        if added:
            return f"MySQL: Added columns {added}"
        return "MySQL: Schema already up to date"
