from db_interface import DatabaseInterface
import psycopg2
from schema_utils import build_postgres_columns  



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
        

    def find_with_join(self, query):
        try:
            base_table = query["table"]
            joins = query.get("join", [])
            
            select_parts = [f"{base_table}.*"]
            join_sql = ""

            for j in joins:
                jt = j["table"]
                on = j["on"]
                sel_cols = j.get("select", [])
                select_parts.extend(sel_cols)
                join_sql += f" JOIN {jt} ON {on}"

            sql = f"SELECT {', '.join(select_parts)} FROM {base_table} {join_sql}"
            
            cursor = self._connection.cursor()  # no dictionary=True
            cursor.execute(sql)
            rows = cursor.fetchall()

            columns = [desc[0] for desc in cursor.description]
            result = [dict(zip(columns, row)) for row in rows]
            cursor.close()
            return result

        except Exception as e:
            return f"PostgreSQL Join Finding Error: {str(e)}"
        
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
        

    
    def table_exists(self, table):
        cursor = self._connection.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = %s
            )
        """, (table,))
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists


    def create_from_schema(self, table, schema):
        
        cols = build_postgres_columns(schema)
        sql = f"CREATE TABLE {table} ({cols})"

        cursor = self._connection.cursor()
        cursor.execute(sql)
        self._connection.commit()
        cursor.close()

        return f"PostgreSQL: Table {table} created via migration"


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

            col_def = build_postgres_columns({col: cfg})
            sql = f"ALTER TABLE {table} ADD COLUMN {col_def}"
            cursor.execute(sql)
            added.append(col)

        self._connection.commit()
        cursor.close()

        if added:
            return f"PostgreSQL: Added columns {added}"
        return "PostgreSQL: Schema already up to date"
    