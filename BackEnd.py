import mysql.connector
import json
import os
import logging

log_directory = "D:\\Database_Project_file_dump"
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

log_file_path = os.path.join(log_directory, 'app.log')

logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def connect_to_mysql():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root'
        )
        return connection, "Connection to MySQL successful."
    except mysql.connector.Error as err:
        return None, f"Error connecting to MySQL: {err}"


def create_database(cursor, db_name):
    try:

        cursor.execute(f"CREATE DATABASE `{db_name}`")
        return f"Database {db_name} created successfully."
    except mysql.connector.Error as err:
        if "1007" in str(err):
            return f"Database {db_name} already exists."
        return f"Failed creating database {db_name}: {err}"


def create_table(cursor, db_name, table_name, columns, valid_data_types):
    valid_data_types_set = set(valid_data_types)

    try:
        cursor.execute(f"USE `{db_name}`")
        logging.info(f"Using database {db_name}")
        status_messages = []

        create_column_statements = []
        foreign_key_statements = []
        for column_name, (data_type, pk, fk, ref_table, ref_column) in columns.items():

            if "varchar" in data_type.lower() and not data_type.lower().endswith(')'):
                data_type += "(255)"

            if data_type.lower() not in valid_data_types_set:
                return f"Invalid data type '{data_type}' provided for column '{column_name}'. Valid types are: {valid_data_types}"

            column_definition = f"`{column_name}` {data_type}"
            if pk.lower() == 'yes':
                column_definition += " PRIMARY KEY"
            create_column_statements.append(column_definition)

            if fk.lower() == 'yes':
                if not ref_table or not ref_column:
                    return f"Missing reference table or column for foreign key on '{column_name}'."

                cursor.execute(
                    f"SELECT * FROM information_schema.tables WHERE table_schema = '{db_name}' AND table_name = '{ref_table}'")
                if cursor.fetchone() is None:
                    return f"Referenced table '{ref_table}' does not exist."
                cursor.execute(
                    f"SELECT * FROM information_schema.columns WHERE table_schema = '{db_name}' AND table_name = '{ref_table}' AND column_name = '{ref_column}'")
                if cursor.fetchone() is None:
                    return f"Referenced column '{ref_column}' in table '{ref_table}' does not exist."
                foreign_key_statements.append(
                    f"FOREIGN KEY (`{column_name}`) REFERENCES `{ref_table}` (`{ref_column}`)")

        if not create_column_statements:
            return f"No valid columns provided for table '{table_name}'"

        column_definitions = ', '.join(create_column_statements)
        table_definition = f"CREATE TABLE `{table_name}` ({column_definitions}"
        if foreign_key_statements:
            table_definition += ", " + ", ".join(foreign_key_statements)
        table_definition += ")"

        print("Executing SQL:", table_definition)

        cursor.execute(table_definition)
        status_messages.append(f"Table {table_name} created successfully in database {db_name}.")

        return "\n".join(status_messages)
    except mysql.connector.Error as err:
        logging.error(f"Failed creating table {table_name} in database {db_name}: {err}")
        return f"Failed creating table {table_name} in database {db_name}: {err}"


def process_json_files(directory, db_name):
    valid_data_types = ["int", "tinyint", "smallint", "mediumint", "bigint", "decimal", "float", "double",
                        "bit", "char", "varchar", "varchar(255)", "binary", "varbinary", "tinyblob", "blob",
                        "mediumblob",
                        "longblob", "tinytext", "text", "mediumtext", "longtext", "enum", "set", "date",
                        "datetime", "time", "timestamp", "year"]

    try:
        connection, message = connect_to_mysql()
        logging.info(message)
        if connection is None:
            logging.error("Failed to connect to MySQL.")
            return message

        cursor = connection.cursor()
        status_message = create_database(cursor, db_name)
        logging.info(status_message)
        if "already exists" in status_message:
            cursor.close()
            connection.close()
            logging.warning(f"Database {db_name} already exists. Exiting.")
            return status_message

        status_messages = [status_message]
        files = sorted([f for f in os.listdir(directory) if f.startswith("Database") and f.endswith(".json")])

        if files:
            latest_file = files[-1]
            with open(os.path.join(directory, latest_file), 'r') as f:
                data = json.load(f)
                for table_name, columns in data.items():
                    table_status = create_table(cursor, db_name, table_name, columns, valid_data_types)
                    status_messages.append(table_status)
                    logging.info(f"Processed table {table_name}: {table_status}")

        cursor.close()
        connection.close()
        final_status = "\n".join(status_messages)
        logging.info(f"Finished processing all tables: {final_status}")
        return final_status
    except Exception as e:
        logging.error(f"An error occurred while processing JSON files: {e}")
        return f"An error occurred: {e}"
