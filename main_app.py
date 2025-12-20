from db_adapter import DbAdapter
from engines import MySqlDatabase, PostgreSqlDatabase, MongoDatabase
import schema.migrations
import argparse

def main():        
    # Connection Queries
    adapter_my_sql = DbAdapter(MySqlDatabase)
    print(adapter_my_sql.connect())

    adapter_pg = DbAdapter(PostgreSqlDatabase)
    print(adapter_pg.connect())

    adapter_mg = DbAdapter(MongoDatabase)
    print(adapter_mg.connect())


    # parser = argparse.ArgumentParser()
    # parser.add_argument("command")
    # parser.add_argument("table")

    # args = parser.parse_args()

    # if args.command == "migrate":
    #     print(adapter_mg.migrate(args.table))
    #     return

    # Quieries    
    insert_query = {
        "action": "insert",
        "table": "users",
        "data": {
            "name": "Kiddo",
            # "father_name": "Kiddo father",
            # "age": 10,
            "email": "user@gmail.com"
        }
    }

    query = {
        "action": "find",
        "table": "posts",
        "join": [
            {
                "table": "users",
                "on": "posts.user_id = users.id",
                "select": ["users.name", "posts.title", "posts.content"]
            }
        ]
    }

    results = adapter_mg.execute_custom_query(query)
    print(results)


    insert = ({
    "action": "insert",
    "table": "posts",
    "data": {
        "user_id": 1,  
        "title": "My First Post",
        "content": "Hello world!"
        }
    })

    
    find_query ={
        "action": "find",
        "table": "employee",
        'where': {'age':10}
    }

    delete_query = {
        "action": "delete",
        "table": "employee",
        "where": {"age" : 10}
    }

    # adapter_my_sql OR adapter_pg OR adapter_mg
    # print(adapter_pg.execute_custom_query(insert))
    # print(adapter_mg.execute_custom_query(insert))
    # print(adapter_mg.execute_custom_query(find_query))
    # print(adapter_mg.execute_custom_query(delete_query))


    # for migrations
    # print(adapter_my_sql.migrate("employee"))
    # print(adapter_pg.migrate("employee"))
    # print(adapter_mg.migrate("employee"))




    

if __name__ == "__main__":
    main()