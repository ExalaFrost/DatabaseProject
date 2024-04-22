import tkinter as tk
from tkinter import simpledialog, messagebox
import json
import os
import logging
import mysql

from BackEnd import process_json_files


class DatabaseSchemaApp:
    def __init__(self, root):
        self.root = root
        self.root.withdraw()
        self.db_connection = self.connect_to_mysql()
        self.log_directory = "D:/Database_Project_file_dump"
        self.app_running = True
        self.valid_data_types = ["int", "tinyint", "smallint", "mediumint", "bigint", "decimal", "float", "double",
                                 "bit", "char", "varchar","varchar(255)", "binary", "varbinary", "tinyblob", "blob", "mediumblob",
                                 "longblob", "tinytext", "text", "mediumtext", "longtext", "enum", "set", "date",
                                 "datetime", "time", "timestamp", "year"]

        self.collect_database_info()

    def setup_logging(self, db_name):
        log_filename = os.path.join(self.log_directory, f"{db_name}_error.log")
        logging.basicConfig(filename=log_filename, level=logging.ERROR,
                            format='%(asctime)s:%(levelname)s:%(message)s')

    def connect_to_mysql(self):
        try:
            connection = mysql.connector.connect(
                host='localhost',
                user='root',
                password='root'
            )
            logging.info("Successfully connected to MySQL.")
            return connection
        except mysql.connector.Error as err:
            logging.error(f"Error connecting to MySQL: {err}")
            messagebox.showerror("Database Connection Error", f"Error connecting to MySQL: {err}")
            self.root.destroy()

    def check_database_exists(self, db_name):
        try:
            cursor = self.db_connection.cursor(buffered=True)
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            exists = any(db_name == db[0] for db in databases)
            cursor.close()
            return exists
        except mysql.connector.Error as err:
            logging.error(f"Error checking if database exists: {err}")
            messagebox.showerror("Error", f"Error checking if database exists: {err}")
            cursor.close()
            return True

    def collect_table_info(self, db_type):
        if db_type == "simple":
            table_num = simpledialog.askinteger("Input", "Enter the number of tables in the database (maximum 5).")
            while table_num <= 0 or table_num > 5:
                table_num = simpledialog.askinteger("Input",
                                                    "Invalid input. Please enter a positive integer, maximum 5.")
        else:
            table_num = simpledialog.askinteger("Input", "Enter the number of tables in the database (at least 5).")
            while table_num < 5:
                table_num = simpledialog.askinteger("Input",
                                                    "Invalid input. Please enter an integer greater than or equal to 5.")

        for i in range(table_num):
            table_name = simpledialog.askstring("Input", f"Enter the name for table {i + 1}.")
            column_num = simpledialog.askinteger("Input", f"Enter the number of columns for table {table_name}.")
            while column_num <= 0:
                column_num = simpledialog.askinteger("Input", "Invalid input. Please enter a positive integer.")

            columns = {}
            for j in range(column_num):
                column_info = self.collect_column_info(table_name)
                column_name, data_type, primary_key = column_info[:3]
                if column_info[3].lower() == 'yes':
                    _, _, _, _, foreign_table, foreign_column = column_info
                    columns[column_name] = (data_type, primary_key, 'yes', foreign_table, foreign_column)
                else:
                    columns[column_name] = (data_type, primary_key, 'no', None, None)
            self.table_info[table_name] = columns

    def collect_database_info(self):
        while self.app_running:
            self.database_type = simpledialog.askstring("Input",
                                                        "What kind of database structure you want to create? Simple or Complex.")
            if self.database_type and self.database_type.lower() in ['simple', 'complex']:
                self.db_name = simpledialog.askstring("Input", "Enter the name for the new database:")
                if self.db_name:
                    self.setup_logging(self.db_name)
                    if not self.check_database_exists(self.db_name):
                        self.table_info = {}
                        self.collect_table_info(self.database_type.lower())
                        self.save_to_file()
                        messagebox.showinfo("Success", "Database schema saved successfully.")
                        self.app_running = False
                    else:
                        messagebox.showerror("Error",
                                             f"Database '{self.db_name}' already exists. Please enter a different name.")
                else:
                    messagebox.showerror("Error", "No database name provided.")
                    self.app_running = False
            else:
                messagebox.showerror("Error", "Invalid input. Please enter either 'Simple' or 'Complex'.")
                self.app_running = False

        self.root.destroy()

    def collect_column_info(self, table_name):
        column_name = simpledialog.askstring("Input", f"Enter the name for column of table {table_name}.")
        data_type = simpledialog.askstring("Input",
                                           f"Enter the data type for column {column_name} of table {table_name}.")
        while data_type.lower() not in self.valid_data_types:
            data_type = simpledialog.askstring("Input", "Please enter a valid SQL data type.")

        primary_key = simpledialog.askstring("Input", f"Is column {column_name} a primary key? Yes or No.")
        while primary_key.lower() not in ['yes', 'no']:
            primary_key = simpledialog.askstring("Input", "Please enter either 'Yes' or 'No'.")


        is_foreign_key = simpledialog.askstring("Input", f"Is column {column_name} a foreign key? Yes or No.")
        foreign_table = foreign_column = None
        if is_foreign_key.lower() == 'yes':
            foreign_table = simpledialog.askstring("Input", "Enter the name of the foreign table:")
            foreign_column = simpledialog.askstring("Input",
                                                    "Enter the name of the foreign column in the foreign table:")

        return column_name, data_type, primary_key, is_foreign_key, foreign_table, foreign_column

    def save_to_file(self):
        directory = "D:/Database_Project_file_dump"
        files = [f for f in os.listdir(directory) if f.startswith("Database")]
        latest_num = max([int(f.replace("Database", "").replace(".json", "")) for f in files], default=0)

        file_path = os.path.join(directory, f"Database{latest_num + 1:02}.json")
        with open(file_path, "w") as f:
            json.dump(self.table_info, f)
        messagebox.showinfo("Success", "Database schema saved successfully.")

        # Call backend process
        status_message = process_json_files(directory, self.db_name)
        messagebox.showinfo("Backend Status", status_message)
        self.root.destroy()


# Main window
root = tk.Tk()
app = DatabaseSchemaApp(root)
root.mainloop()
