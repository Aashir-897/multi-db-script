from db_adapter import DbAdapter
from db_engine import MySqlDatabase, PostgreSqlDatabase, MongoDatabase


# Connection Queries
adapter_my_sql = DbAdapter(MySqlDatabase)
print(adapter_my_sql.connect())

adapter_pg = DbAdapter(PostgreSqlDatabase)
print(adapter_pg.connect())

adapter_mg = DbAdapter(MongoDatabase)
print(adapter_mg.connect())


# Find Queries
# users_my_sql = adapter_my_sql.execute_custom_query({
#     "action": "find",
#     "table": "users",
#     'where': {'age':10}
# })
# print('This my Sql Reult', users_my_sql)

# users_pg = adapter_pg.execute_custom_query({
#     "action": "find",
#     "table": "users",
#     'where': {'age':10}
# })
# print('\nThis my postgreSql Reult', users_pg)

# users_mg = adapter_mg.execute_custom_query({
#     "action": "find",
#     "table": "users",
#     'where': {'age':10}
# })
# print('\nThis my mongoDb Reult', users_mg)




# insert_query = {
#     "action": "insert",
#     "table": "users",
#     "data": {
#         "name": "Kiddo",
#         "age": 10,
#     }
# }

# adapter_my_sql.execute_custom_query(insert_query)
# adapter_pg.execute_custom_query(insert_query)
# adapter_mg.execute_custom_query(insert_query)





# update_query = {
#     "action": "update",
#     "table": "users",
#     "data": {
#         "name": "kiddo",
#         "age": 25,
#     },
#     'where': {
#         "name": "kamran"
#     }
# }

# adapter_my_sql.execute_custom_query(update_query)
# adapter_pg.execute_custom_query(update_query)
# adapter_mg.execute_custom_query(update_query)


delete_query = {
    "action": "delete",
    "table": "users",
    "where": {"name": "Aashir_mongo"}
}

print(adapter_my_sql.execute_custom_query(delete_query))
print(adapter_pg.execute_custom_query(delete_query))
print(adapter_mg.execute_custom_query(delete_query))


