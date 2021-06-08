from typedb.client import TypeDB, SessionType, TransactionType
import ijson

inputs = [
    {
        "data_path": "data/accounts",
        "template": "account_template"
    },
    {
        "data_path": "data/people",
        "template": "person_template"
    }]

def account_template(account):
    pass

def person_template(person):
    pass

def parse_data_to_dictionaries(path):
    items = []
    with open(path["data_path"] + ".json") as data:
        for item in ijson.items(data, "item"):
            items.append(item)
    return items

def load_data_into_typedb(input, session):
    items = parse_data_to_dictionaries(input)

    for item in items:
        with session.transaction(TransactionType.WRITE) as transaction:
            typeql_insert_query = input["template"](item)
            print("Executing TypeQL Query: " + typeql_insert_query)
            transaction.query().insert(typeql_insert_query)
            transaction.commit()

    print("\nInserted " + str(len(items)) + " items from [ " + input["data_path"] + "] into TypeDB.\n")

def build_graph(inputs):
    with TypeDB.core_client("localhost:1729") as client:
        with client.session("customer_360", SessionType.DATA) as session:
            for input in inputs:
                print("Loading from [" + input["data_path"] + "] into TypeDB ...")
                load_data_into_typedb(input, session)

def check_graph(inputs):
    with TypeDB.core_client("localhost:1729") as client:
        if(client.databases().contains("customer_360")):
            print("Customer 360 graph exists. Loading data...")
            build_graph(inputs)
        else:
            client.databases().create("customer_360")
            with client.session("customer_360", SessionType.SCHEMA) as session:
                with session.transaction(TransactionType.WRITE) as transaction:
                    with open('schema.tql', 'r') as file:
                        schema = file.read().replace('\n', '')
                    transaction.query().define(schema)
                    if(transaction.future.get()):
                        transaction.commit()
                        print("Customer 360 graph did not exist. The database has been created, and the schema has been defined. Loading data...")
                        build_graph(inputs)
                    else:
                        print("Error in defining schema.")






