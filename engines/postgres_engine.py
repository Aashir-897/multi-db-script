from db_interface import DatabaseInterface
import psycopg2

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
            return f"PostgreSQL Finding Error: {str(e)}"
        
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
            return f"PostgreSQL Inserting Error: {str(e)}"


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
            return f"PostgreSQL updating  Error: {str(e)}"

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
            return f"PostgreSQL deleting Error: {str(e)}"
        

    def table_create(self, query):
        try:
            table = query["table"]
            columns = query.get("columns", {})

            col_defs = []
            for k, v in columns.items():
                v_pg = v.replace("INT PRIMARY KEY AUTO_INCREMENT", "SERIAL PRIMARY KEY")
                col_defs.append(f"{k} {v_pg}")
            sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(col_defs)})"

            cursor = self._connection.cursor()
            cursor.execute(sql)
            self._connection.commit()
            cursor.close()
            return f"PostgreSQL: Table {table} created"
        except Exception as e:
            return f"PostgreSQL Table Creation Error: {str(e)}"
        
    def add_column(self, query):
        try:
            table = query["table"]
            columns = query.get("columns", {})

            cursor = self._connection.cursor()
            for col_name, col_def in columns.items():
                col_type = col_def["type"]
                default = col_def.get("default")
                sql = f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}"
                if default is not None:
                    sql += f" DEFAULT '{default}'"
                cursor.execute(sql)
            self._connection.commit()
            cursor.close()
            return f"PostgreSQL: Columns {', '.join(columns.keys())} added to {table}"
        except Exception as e:
            return f"PostgreSQL Add Column Error: {str(e)}"
