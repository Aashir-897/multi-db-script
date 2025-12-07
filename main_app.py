from db_adapter import DbAdapter
# from db_engine import MySqlDatabase
from db_engine import MySqlDatabase, PostgreSqlDatabase


# adapter = DbAdapter(MySqlDatabase)
# print(adapter.connect())

# result = adapter.execute_custom_query("SELECT * FROM users")
# print(result)

# msg = adapter.execute_custom_query(
#     "INSERT INTO users (name, age) VALUES ('Aashir', 25)"
# )
# print(msg)

adapter_pg = DbAdapter(PostgreSqlDatabase)
print(adapter_pg.connect())
    
# result_pg = adapter_pg.execute_custom_query("SELECT * FROM users")
# print(result_pg)

msg = adapter_pg.execute_custom_query(
    "INSERT INTO users (name, age) VALUES ('Aashir', 25)"
)
print(msg)
