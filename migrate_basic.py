from typedb.client import TypeDB, SessionType, TransactionType

def build_graph():
    with TypeDB.core_client("localhost:1729") as client:
        with client.session("customer_360", SessionType.DATA) as session:
            with session.transaction(TransactionType.WRITE) as transaction:
                if():
                    print("Database already contains data. You're ready to get started with TypeDB!")
                else: 
                    with open('data.tql', 'r') as file:
                        data = file.read().replace('\n', '')
                        transaction.query().insert(data)
                        transaction.commit()
                        print("Data added to database.")

def check_graph():
    with TypeDB.core_client("localhost:1729") as client:
        if(client.databases().contains("customer_360")):
            print("Customer 360 graph exists.")
            build_graph()
        else:
            client.databases().create("customer_360")
            with client.session("customer_360", SessionType.SCHEMA) as session:
                with session.transaction(TransactionType.WRITE) as transaction:
                    with open('schema.tql', 'r') as file:
                        schema = file.read().replace('\n', '')
                    transaction.query().define(schema)
                    transaction.commit()
                    print("Customer 360 graph did not exist. The database has been created, and the schema has been defined. Loading data...")
            build_graph()

def main():
    check_graph()

if __name__ == '__main__':
    main()