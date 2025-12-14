from db_adapter import DbAdapter
from engines import MySqlDatabase, PostgreSqlDatabase, MongoDatabase
import schema.employee_schema

def main():        
    # Connection Queries
    adapter_my_sql = DbAdapter(MySqlDatabase)
    print(adapter_my_sql.connect())

    adapter_pg = DbAdapter(PostgreSqlDatabase)
    print(adapter_pg.connect())

    adapter_mg = DbAdapter(MongoDatabase)
    print(adapter_mg.connect())

    # Quieries
    create_query = {
        "action": "table_create",
        "table": "employee"
    }

    insert_query = {
        "action": "insert",
        "table": "employee",
        "data": {
            "name": "Kiddo",
            "age": 10,
            "email": "user@gmail.com"
        }
    }
    find_query ={
        "action": "find",
        "table": "employee",
        'where': {'e_age':10}
    }

    delete_query = {
        "action": "delete",
        "table": "employee",
        "where": {"e_age" : 10}
    }

    # adapter_my_sql OR adapter_pg OR adapter_mg
    # print(adapter_mg.execute_custom_query(create_query))
    # print(adapter_mg.execute_custom_query(insert_query))
    # print(adapter_mg.execute_custom_query(find_query))
    # print(adapter_mg.execute_custom_query(delete_query))
    print(adapter_my_sql.migrate("employee"))


if __name__ == "__main__":
    main()