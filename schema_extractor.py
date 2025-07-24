from sqlalchemy import create_engine, inspect

def extract_schema(database_url):
    engine = create_engine(database_url)
    inspector = inspect(engine)
    schema = {}
    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        schema[table_name] = [(col['name'], str(col['type'])) for col in columns]
    return schema


if __name__ == "__main__":
    database_url = "postgresql://postgres:12345678@localhost:5432/blinkit_db"
    schema = extract_schema(database_url)
    print(schema)