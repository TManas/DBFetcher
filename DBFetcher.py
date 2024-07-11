# %%
import tkinter as tk
from tkinter import ttk, messagebox
from tkcalendar import DateEntry
import psycopg2
import csv
import logging

logging.basicConfig(level=logging.INFO)

# Configuration for server1 database
db_config_server1 = {
    'dbname': 'dbname',
    'user': 'user',
    'password': 'password',
    'host': 'host',
    'port': 'port'
}

class DBFetcherApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Data Fetcher")
        self.root.geometry("800x600")

        # Initialize variables
        self.start_date_var = tk.StringVar()
        self.end_date_var = tk.StringVar()
        self.id_var = tk.StringVar()
        self.status_var = tk.StringVar()
        self.schema_var = tk.StringVar()
        self.conn = None  # Database connection

        self.setup_ui()

    def setup_ui(self):
        # Main frame to center align all elements
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.grid(row=0, column=0, sticky=(tk.N, tk.S, tk.E, tk.W))
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_columnconfigure(0, weight=1)

        # Grid configuration for main frame
        for i in range(12):
            main_frame.grid_rowconfigure(i, pad=5)
        for i in range(3):
            main_frame.grid_columnconfigure(i, pad=5, weight=1)

        # Server selection
        self.server_label = ttk.Label(main_frame, text="Select Server")
        self.server_label.grid(row=0, column=0, pady=5, sticky=tk.W)
        self.server_var = tk.StringVar(value="server1")
        self.liferay_radio = ttk.Radiobutton(main_frame, text="Server 1", variable=self.server_var, value="server1", command=self.toggle_server_options)
        self.liferay_radio.grid(row=0, column=1, pady=5, sticky=tk.W)
        self.sas_radio = ttk.Radiobutton(main_frame, text="Server 2", variable=self.server_var, value="server2", command=self.toggle_server_options)
        self.sas_radio.grid(row=0, column=2, pady=5, sticky=tk.W)

        # Server 2 credentials fields (initially hidden)
        self.user_id_label = ttk.Label(main_frame, text="User ID")
        self.user_id_entry = ttk.Entry(main_frame, width=30)

        self.password_label = ttk.Label(main_frame, text="Password")
        self.password_entry = ttk.Entry(main_frame, show="*", width=30)

        # Connect button
        self.connect_button = ttk.Button(main_frame, text="Connect", command=self.connect_to_db)
        self.connect_button.grid(row=3, column=0, columnspan=3, pady=10)

        # Date range selection
        self.date_range_label = ttk.Label(main_frame, text="Select Date Range")
        self.date_range_label.grid(row=4, column=0, pady=5, sticky=tk.W)
        self.start_date_label = ttk.Label(main_frame, text="Start Date:")
        self.start_date_label.grid(row=4, column=1, padx=2, pady=5, sticky=tk.E)
        self.start_date = DateEntry(main_frame, width=12, background='darkblue', foreground='white', borderwidth=2, year=2024, month=6, day=3, textvariable=self.start_date_var)
        self.start_date.grid(row=4, column=2, pady=5, padx=5, sticky=tk.W)
        self.end_date_label = ttk.Label(main_frame, text="End Date:")
        self.end_date_label.grid(row=5, column=1, pady=5, sticky=tk.E)
        self.end_date = DateEntry(main_frame, width=12, background='darkblue', foreground='white', borderwidth=2, year=2024, month=6, day=4, textvariable=self.end_date_var)
        self.end_date.grid(row=5, column=2, pady=5, padx=5, sticky=tk.W)

        # Report upload ID
        self.report_id_label = ttk.Label(main_frame, text="Specify reportuploadlogid")
        self.report_id_label.grid(row=6, column=0, pady=5, sticky=tk.W)
        self.report_id_entry = ttk.Entry(main_frame, textvariable=self.id_var, width=30)
        self.report_id_entry.grid(row=6, column=1, columnspan=2, pady=5, padx=5, sticky=tk.W)

        # Schema selection
        self.schema_label = ttk.Label(main_frame, text="Select Schema")
        self.schema_label.grid(row=7, column=0, pady=5, sticky=tk.W)
        self.schema_combo = ttk.Combobox(main_frame, textvariable=self.schema_var, state="readonly", width=30)
        self.schema_combo.grid(row=7, column=1, columnspan=2, pady=5, padx=5, sticky=tk.W)

        # Log status selection (only for server1)
        self.log_status_label = ttk.Label(main_frame, text="Select Log Status")
        self.log_status_combo = ttk.Combobox(main_frame, textvariable=self.status_var, state="readonly", width=30)

        # Save to CSV
        self.save_to_csv_var = tk.BooleanVar()
        self.save_to_csv_check = ttk.Checkbutton(main_frame, text="Save to CSV", variable=self.save_to_csv_var)
        self.save_to_csv_check.grid(row=8, column=1, pady=5, sticky=tk.W)

        # Fetch data button
        self.fetch_button = ttk.Button(main_frame, text="Fetch Data", command=self.fetch_data)
        self.fetch_button.grid(row=8, column=1, columnspan=2, pady=5, padx=5)
        # self.connect_button.grid(row=3, column=0, columnspan=3, pady=10)


        # Fetched data display
        self.fetched_data_label = ttk.Label(main_frame, text="Fetched Data")
        self.fetched_data_label.grid(row=9, column=0, pady=5, sticky=tk.W)
        self.data_tree = ttk.Treeview(main_frame, columns=("table_name", "schema_name", "report_id", "create_date", "report_date", "intermediary_name", "record_count"), show="headings")
        # self.data_tree.heading("#0", text="Index")
        self.data_tree.heading("table_name", text="Table Name")
        self.data_tree.heading("schema_name", text="Schema Name")
        self.data_tree.heading("report_id", text="reportuploadlogid")
        self.data_tree.heading("create_date", text="Create Date")
        self.data_tree.heading("report_date", text="Reporting Date")
        self.data_tree.heading("intermediary_name", text="Intermediary Name")
        self.data_tree.heading("record_count", text="Record Count")
        self.data_tree.grid(row=10, column=0, columnspan=3, pady=5, padx=5, sticky=(tk.W, tk.E))

        # Hide log status fields initially
        self.log_status_label.grid_remove()
        self.log_status_combo.grid_remove()

    def toggle_server_options(self):
        server = self.server_var.get()
        if server == "server1":
            self.user_id_label.grid_remove()
            self.user_id_entry.grid_remove()
            self.password_label.grid_remove()
            self.password_entry.grid_remove()
            self.log_status_label.grid(row=7, column=0, pady=5, sticky=tk.W)
            self.log_status_combo.grid(row=7, column=1, columnspan=2, pady=5, padx=5, sticky=tk.W)
            self.connect_button.grid(row=3, column=0, columnspan=3, pady=10)
        else:
            self.user_id_label.grid(row=1, column=0, pady=5, sticky=tk.W)
            self.user_id_entry.grid(row=1, column=1, columnspan=2, pady=5, padx=5, sticky=tk.W)
            self.password_label.grid(row=2, column=0, pady=5, sticky=tk.W)
            self.password_entry.grid(row=2, column=1, columnspan=2, pady=5, padx=5, sticky=tk.W)
            self.log_status_label.grid_remove()
            self.log_status_combo.grid_remove()
            self.connect_button.grid(row=3, column=0, columnspan=3, pady=10)

    def connect_to_db(self):
        try:
            server = self.server_var.get()

            if server == "server1":
                self.conn = psycopg2.connect(**db_config_server1)
            else:
                self.conn = psycopg2.connect(
                    dbname='db',
                    user=self.user_id_entry.get(),
                    password=self.password_entry.get(),
                    host='host',
                    port='port'
                )

            cursor = self.conn.cursor()

            if server == "server1":
                cursor.execute("SELECT DISTINCT status_ FROM public.reportuploadlog")
                log_statuses = ["All"] + [row[0] for row in cursor.fetchall()]
                self.log_status_combo['values'] = log_statuses
            cursor.execute("SELECT schema_name FROM information_schema.schemata")
            schemas = ["All"] + [row[0] for row in cursor.fetchall() if row[0] not in ['information_schema', 'pg_catalog']]
            self.schema_combo['values'] = schemas

            messagebox.showinfo("Success", "Connected to the database successfully!")

        except Exception as e:
            logging.error(f"An error occurred: {e}")
            messagebox.showerror("Error", f"An error occurred: {e}")

    def fetch_data(self):
        if not self.conn:
            messagebox.showerror("Error", "Not connected to any database.")
            return

        start_date = self.start_date_var.get()
        end_date = self.end_date_var.get()
        reportuploadlogid = self.id_var.get()
        log_status = self.status_var.get()

        try:
            cursor = self.conn.cursor()
            schema = 'public' if self.server_var.get() == 'server1' else self.schema_var.get()

            # Step 1: Get reportuploadlogid values based on log_status if specified
            if log_status and log_status != 'Any':
                cursor.execute(f"SELECT reportuploadlogid FROM {schema}.reportuploadlog WHERE status_ = %s", (log_status,))
                reportuploadlog_ids = [row[0] for row in cursor.fetchall()]
            else:
                reportuploadlog_ids = None

            cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = %s;", (schema,))
            table_names = cursor.fetchall()
            all_data = []

            for table_name_tuple in table_names:
                table_name = table_name_tuple[0].lower()

                cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s;", (schema, table_name))
                columns = cursor.fetchall()
                column_names = [col[0] for col in columns]

                if 'reportuploadlogid' in column_names:
                    create_date_column = next((col for col in column_names if col in ['createdate', 'created_on', 'createdon', 'createddate']), None)
                    intermediary_column = next((col for col in column_names if col in ['intermediary_name', 'pension_fund_name']), None)
                    report_date_column = next((col for col in column_names if col in ['date', 'reportdate', 'reporting_date', 'reportingdate', 'portfolio_reporting_date']), None)

                    if create_date_column:
                        select_columns = ['form.reportuploadlogid', f'form.{create_date_column}', f'form.{intermediary_column}' if intermediary_column else 'NULL']
                        if report_date_column:
                            select_columns.append(f'form.{report_date_column}')
                        column_name = ', '.join(select_columns)

                        # Base query and params
                        query = f"SELECT {column_name} FROM {schema}.{table_name} AS form WHERE form.{create_date_column} BETWEEN %s AND %s"
                        params = [start_date, end_date]

                        # Add reportuploadlogid condition based on the provided input or log_status filter
                        if reportuploadlogid:
                            query += " AND form.reportuploadlogid = %s"
                            params.append(int(reportuploadlogid))
                        elif reportuploadlog_ids:
                            query += " AND form.reportuploadlogid = ANY(%s)"
                            params.append(reportuploadlog_ids)

                        cursor.execute(query, params)
                        rows = cursor.fetchall()

                        if rows:
                            merged_rows = {}

                            for row in rows:
                                report_upload_log_id = row[0]
                                create_date_value = row[1]
                                intermediary_name = row[2] if intermediary_column else None
                                report_date = row[3] if report_date_column else None

                                key = (report_upload_log_id, create_date_value)
                                if key in merged_rows:
                                    merged_rows[key][2] += 1
                                else:
                                    merged_rows[key] = [intermediary_name, report_date, 1]

                            for idx, ((report_upload_log_id, create_date_value), (intermediary_name, report_date, records_count)) in enumerate(merged_rows.items()):
                                row_data = [idx + 1, table_name, schema, report_upload_log_id, create_date_value, report_date, intermediary_name, records_count]
                                all_data.append(row_data)

                            logging.info(f"Data from table '{table_name}' has been collected.")
                        else:
                            logging.info(f"No records found in table '{table_name}'")
                    else:
                        logging.info(f"Skipping table '{table_name}' as 'create_date' column is missing.")
                else:
                    logging.info(f"Skipping table '{table_name}' as 'reportuploadlogid' is missing.")

            for item in self.data_tree.get_children():
                self.data_tree.delete(item)

            for data_row in all_data:
                self.data_tree.insert('', tk.END, values=data_row[1:], text=data_row[1])

            if self.save_to_csv_var.get():
                if all_data:
                    csv_file_path = "Selected_Tables.csv"
                    with open(csv_file_path, 'w', newline='') as csvfile:
                        csv_writer = csv.writer(csvfile)
                        header = ['Index', 'Table Name', 'Schema Name', 'reportuploadlogid', 'Create Date', 'Reporting Date', 'Intermediary Name', 'Record Count']
                        csv_writer.writerow(header)
                        csv_writer.writerows(all_data)
                    logging.info(f"All data has been fetched and saved to: {csv_file_path}")
                else:
                    logging.info("No data to write to CSV.")
            else:
                logging.info("Data displayed on GUI only.")

        except Exception as e:
            logging.error(f"An error occurred: {e}")
            messagebox.showerror("Error", f"An error occurred: {e}")
        finally:
            cursor.close()
            self.conn.close()
            self.conn = None

if __name__ == "__main__":
    root = tk.Tk()
    app = DBFetcherApp(root)
    root.mainloop()

# %% [markdown]
# 


