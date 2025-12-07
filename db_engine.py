from db_interface import DatabaseInterface
import mysql.connector
import psycopg2

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
        cursor = self._connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result

    def insert(self, query):
        cursor = self._connection.cursor()
        cursor.execute(query)
        self._connection.commit()
        cursor.close()
        return "Record inserted successfully"

    def update(self, query):
        cursor = self._connection.cursor()
        cursor.execute(query)   
        self._connection.commit()
        cursor.close()
        return "Record updated successfully"

    def delete(self, query):
        cursor = self._connection.cursor()
        cursor.execute(query)
        self._connection.commit()
        cursor.close()
        return "Record deleted successfully"


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
        cursor = self._connection.cursor()
        cursor.execute(query)
        # fetchall returns list of tuples
        columns = [desc[0] for desc in cursor.description]
        result = [dict(zip(columns, row)) for row in cursor.fetchall()]
        cursor.close()
        return result

    def insert(self, query):
        cursor = self._connection.cursor()
        cursor.execute(query)
        self._connection.commit()
        cursor.close()
        return "Record inserted successfully"

    def update(self, query):
        cursor = self._connection.cursor()
        cursor.execute(query)
        self._connection.commit()
        cursor.close()
        return "Record updated successfully"

    def delete(self, query):
        cursor = self._connection.cursor()
        cursor.execute(query)
        self._connection.commit()
        cursor.close()
        return "Record deleted successfully"
