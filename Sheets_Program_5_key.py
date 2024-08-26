import threading
import time
import tkinter as tk
from tkinter import messagebox
import configparser
import pandas as pd
import requests


# Libraries used:
# requests: pip install requests
# pandas: pip install pandas
# tkinter: pip install tkinter

class SheetsExtractProgram:

    # this dunder always starts as soon as the class is called/instantiated
    def __init__(self):
        # initial values for class variables
        self.error_message = ""
        self.spreadsheet_id = ""
        self.worksheet = ""
        self.api_key_1 = ""
        self.api_key_2 = ""
        self.api_key_3 = ""
        self.api_key_4 = ""
        self.api_key_5 = ""
        self.seconds = 1
        self.filename = ""
        self.threadRunning = False
        self.errorMessage = None
        self.thread = None
        self.config_file = "config.ini"
        self.config = configparser.ConfigParser()
        self.api_key_index = 0

        # load config file or set config file if it doesn't exist
        try:
            self.config.read(self.config_file)
        except Exception:
            SheetsExtractProgram.show_error_message("Unable to load config.ini Try deleting it, or checking the "
                                                    "headers.")
            return

        try:
            self.spreadsheet_id = self.config['MAIN']['spreadsheet_id']
            self.worksheet = self.config['MAIN']['worksheet']
            self.api_key_1 = self.config['MAIN']['api_key_1']
            self.api_key_2 = self.config['MAIN']['api_key_2']
            self.api_key_3 = self.config['MAIN']['api_key_3']
            self.api_key_4 = self.config['MAIN']['api_key_4']
            self.api_key_5 = self.config['MAIN']['api_key_5']
            self.seconds = float(self.config['MAIN']['seconds'])
            self.filename = self.config['MAIN']['filename']
        except Exception:
            if not self.config.has_section('MAIN'):
                self.config.add_section('MAIN')
            self.config.set('MAIN', 'spreadsheet_id', self.spreadsheet_id)
            self.config.set('MAIN', 'worksheet', self.worksheet)
            self.config.set('MAIN', 'api_key_1', self.api_key_1)
            self.config.set('MAIN', 'api_key_2', self.api_key_2)
            self.config.set('MAIN', 'api_key_3', self.api_key_3)
            self.config.set('MAIN', 'api_key_4', self.api_key_4)
            self.config.set('MAIN', 'api_key_5', self.api_key_5)
            self.config.set('MAIN', 'seconds', str(self.seconds))
            self.config.set('MAIN', 'filename', self.filename)

            with open(self.config_file, 'w') as configfile:
                self.config.write(configfile)

        # Create the GUI with the text boxes, buttons and status box
        self.window = tk.Tk()
        self.window.title("Google Sheet Data Puller")
        self.window.geometry("590x500")  # Changed to make the window taller
        self.window.resizable(True, True)

        # Create a frame to hold the text boxes and labels
        self.frame = tk.Frame(self.window)
        self.frame.grid(row=0, column=0)

        # Create the text boxes and labels for the spreadsheet ID, worksheet name, API key,
        # seconds to loop and filename # Added a text box and a label for the filename
        self.spreadsheet_id_entry = tk.Entry(self.frame, width=30, font=("Arial", 16))
        self.spreadsheet_id_entry.insert(0, self.spreadsheet_id)
        self.spreadsheet_id_label = tk.Label(self.frame, text="Spreadsheet ID:", font=("Arial", 16))
        self.worksheet_entry = tk.Entry(self.frame, width=30, font=("Arial", 16))
        self.worksheet_entry.insert(0, self.worksheet)
        self.worksheet_label = tk.Label(self.frame, text="Worksheet Name:", font=("Arial", 16))
        # API KEY 1
        self.api_key_1_entry = tk.Entry(self.frame, width=30, font=("Arial", 16))
        self.api_key_1_entry.insert(0, self.api_key_1)
        self.api_key_1_label = tk.Label(self.frame, text="API Key 1:", font=("Arial", 16))
        # API KEY 2
        self.api_key_2_entry = tk.Entry(self.frame, width=30, font=("Arial", 16))
        self.api_key_2_entry.insert(0, self.api_key_2)
        self.api_key_2_label = tk.Label(self.frame, text="API Key 2:", font=("Arial", 16))
        # API KEY 3
        self.api_key_3_entry = tk.Entry(self.frame, width=30, font=("Arial", 16))
        self.api_key_3_entry.insert(0, self.api_key_3)
        self.api_key_3_label = tk.Label(self.frame, text="API Key 3:", font=("Arial", 16))
        # API KEY 4
        self.api_key_4_entry = tk.Entry(self.frame, width=30, font=("Arial", 16))
        self.api_key_4_entry.insert(0, self.api_key_4)
        self.api_key_4_label = tk.Label(self.frame, text="API Key 4:", font=("Arial", 16))
        # API Key 5
        self.api_key_5_entry = tk.Entry(self.frame, width=30, font=("Arial", 16))
        self.api_key_5_entry.insert(0, self.api_key_5)
        self.api_key_5_label = tk.Label(self.frame, text="API Key 5:", font=("Arial", 16))
        # Seconds
        self.seconds_entry = tk.Entry(self.frame, width=30, font=("Arial", 16))
        self.seconds_entry.insert(0, str(self.seconds))
        self.seconds_label = tk.Label(self.frame, text="Seconds to Loop:", font=("Arial", 16))
        self.filename_entry = tk.Entry(self.frame, width=30,font=("Arial", 16))  # Changed the width to make the text box shorter
        self.filename_entry.insert(0, self.filename)  # Added to insert the filename from the config file
        self.filename_label = tk.Label(self.frame, text="Output Filename:",font=("Arial", 16))  # Added a label for the filename
        self.csv_label = tk.Label(self.frame, text=".csv",font=("Arial", 16))  # Added a label to show the file extension

        # Create the buttons for start/stop and save
        self.startStop_btn = tk.Button(self.window, text="Start", font=("Arial", 16), width=10)
        self.save_button = tk.Button(self.window, text="Save", font=("Arial", 16), width=10)

        # Create the status box to show the readiness or running state of the program
        self.status_box = tk.Label(self.window, text="", font=("Arial", 30), width=10)

        # Arrange the widgets in a grid layout
        self.spreadsheet_id_label.grid(row=0, column=0)
        self.spreadsheet_id_entry.grid(row=0, column=1)
        self.worksheet_label.grid(row=1, column=0)
        self.worksheet_entry.grid(row=1, column=1)
        self.api_key_1_label.grid(row=2, column=0)
        self.api_key_1_entry.grid(row=2, column=1)
        self.api_key_2_label.grid(row=3, column=0)
        self.api_key_2_entry.grid(row=3, column=1)
        self.api_key_3_label.grid(row=4, column=0)
        self.api_key_3_entry.grid(row=4, column=1)
        self.api_key_4_label.grid(row=5, column=0)
        self.api_key_4_entry.grid(row=5, column=1)
        self.api_key_5_label.grid(row=6, column=0)
        self.api_key_5_entry.grid(row=6, column=1)
        self.seconds_label.grid(row=7, column=0)
        self.seconds_entry.grid(row=7, column=1)
        self.filename_label.grid(row=8, column=0)  # Added to place the filename label in the grid layout
        self.filename_entry.grid(row=8, column=1)  # Added to place the filename text box in the grid layout
        self.csv_label.grid(row=8, column=2)  # Added to place the file extension label in the grid layout
        self.startStop_btn.grid(row=9, columnspan=2, pady=5)
        self.save_button.grid(row=10, columnspan=2, pady=5)
        self.status_box.grid(row=11, columnspan=2, pady=5)
        # Create a text widget to display the log # Added to embed the log into the GUI

        self.startStop_btn.config(command=self.start_loop)

        # Bind the save button to the save settings function
        self.save_button.config(command=self.save_settings)

        try:
            # Print the initial message to the console
            print("Program started")
        except Exception as e:
            # Print and append any error in opening or writing to the log file
            print(f"Error opening or writing to log file: {e}")
        self.update_status()
        # starts tk window loop program
        self.window.protocol("WM_DELETE_WINDOW", self.on_close)
        self.main_loop()
        self.window.mainloop()

    def main_loop(self):
        self.window.after(1000, self.main_loop)
        self.update_status()

    def on_close(self):
        self.threadRunning = False
        self.window.after(1000, self.window.destroy)

    def log(self, message):
        print(message)

    # Define a function to update the status box based on the data fields
    def update_status(self):
        print("updating values")
        self.spreadsheet_id = self.spreadsheet_id_entry.get()
        self.worksheet = self.worksheet_entry.get()
        self.api_key_1 = self.api_key_1_entry.get()
        self.api_key_2 = self.api_key_2_entry.get()
        self.api_key_3 = self.api_key_3_entry.get()
        self.api_key_4 = self.api_key_4_entry.get()
        self.api_key_5 = self.api_key_5_entry.get()
        try:
            self.seconds = float(self.seconds_entry.get())
        except ValueError:
            self.seconds = 0
        self.filename = self.filename_entry.get()  # Added to get the filename from the text box
        if not self.threadRunning:
            if self.spreadsheet_id and self.worksheet and self.api_key_1 and self.seconds > 0 and self.filename:  # Added
                # to check if the filename is not empty
                self.status_box.config(text="READY", bg="green")
                # self.log("Status updated: READY")  # Added to log the status update
            else:
                self.status_box.config(text="NOT READY", bg="gray")
                # self.log("Status updated: NOT READY")  # Added to log the status update

    # Define a function to save the data fields to the config file
    def save_settings(self):
        self.update_status()
        self.config.set('MAIN', 'spreadsheet_id', self.spreadsheet_id)
        self.config.set('MAIN', 'worksheet', self.worksheet)
        self.config.set('MAIN', 'api_key_1', self.api_key_1)
        self.config.set('MAIN', 'api_key_2', self.api_key_2)
        self.config.set('MAIN', 'api_key_3', self.api_key_3)
        self.config.set('MAIN', 'api_key_4', self.api_key_4)
        self.config.set('MAIN', 'api_key_5', self.api_key_5)
        self.config.set('MAIN', 'seconds', str(self.seconds))
        self.config.set('MAIN', 'filename', self.filename)
        with open(self.config_file, 'w') as configfile:
            self.config.write(configfile)

    # Define a function to get the sheet data and transpose it using the pandas library
    def get_sheet_data(self):
    # Filter out empty API keys
        valid_api_keys = [key for key in [self.api_key_1, self.api_key_2, self.api_key_3, self.api_key_4, self.api_key_5] if key]

        if not valid_api_keys:
            self.error_message = "NO VALID API KEYS PROVIDED"
            self.log("No valid API keys provided")
            return

        api_key_index = getattr(self, 'api_key_index', 0)  # get the current index, or 0 if it doesn't exist

        try:
            # Get the next API key from the list
            api_key = valid_api_keys[api_key_index % len(valid_api_keys)]
            self.api_key_index = api_key_index + 1  # increment the index for the next call

            response = requests.get(
                f"https://sheets.googleapis.com/v4/spreadsheets/{self.spreadsheet_id}/values/{self.worksheet}"
                f"?key={api_key}")
            response.raise_for_status()
            data = response.json()
            rows = data["values"]
            rows.pop(0)
            tmp = {k[0]: k[1:] for k in rows}
            tmp = pd.DataFrame.from_dict(tmp, orient='index')
            tmp = tmp.transpose()
            tmp.to_csv(self.filename + ".csv", index=False,
                   encoding="utf-8")  # Changed to use the filename variable as the output file name
            self.error_message = ""
            # Log the success message
            self.log(
            f"Successfully wrote to {self.filename}.csv")  # Changed to show the filename in the log message
        except requests.exceptions.HTTPError as e:
            # Log the HTTP error message
            self.log(f"HTTP Error: {e}")
        except PermissionError as e:
            # Log and show the permission error message
            self.error_message = "CANNOT WRITE TO DISK, FILE IN USE"
            self.log(f"Permission Error: {e}\nYou may have the CSV file open, please close it!")
        except Exception as e:
            # Log any other error message
            self.log(f"Other Error: {e}")


    def export_thread(self):

        while self.threadRunning:
            self.get_sheet_data()
            time.sleep(self.seconds)

    # Define a function to start the secondary loop thread
    def start_loop(self):

        self.update_status()
        if self.status_box["text"] == "READY" and self.startStop_btn['text'] == "Start":
            self.startStop_btn.config(text="Stop")
            self.status_box.config(text="RUNNING", bg="red")
            # Gray out the text input boxes
            self.spreadsheet_id_entry.config(state="disabled")
            self.worksheet_entry.config(state="disabled")
            self.api_key_1_entry.config(state="disabled")
            self.api_key_2_entry.config(state="disabled")
            self.api_key_3_entry.config(state="disabled")
            self.api_key_4_entry.config(state="disabled")
            self.api_key_5_entry.config(state="disabled")
            self.seconds_entry.config(state="disabled")
            self.filename_entry.config(state="disabled")  # Added to gray out the filename text box
            self.threadRunning = True
            # Create and start the secondary loop thread
            self.thread = threading.Thread(target=self.export_thread)
            self.thread.start()

            # Log that the start button was pushed and the thread started
            self.log("Start button pushed\nSecondary loop thread started")
            return
        else:
            # Log that the start button was pushed but the status was not ready
            self.log(f"Start button pushed\nStatus not ready")

        if self.startStop_btn['text'] == "Stop":
            self.startStop_btn.config(text="Start")
            self.stop_loop()
            return

    # Define a function to stop the secondary loop thread
    def stop_loop(self):

        self.threadRunning = False
        self.status_box.config(text="READY", bg="green")
        # Enable the text input boxes
        self.spreadsheet_id_entry.config(state="normal")
        self.worksheet_entry.config(state="normal")
        self.api_key_1_entry.config(state="normal")
        self.api_key_2_entry.config(state="normal")
        self.api_key_3_entry.config(state="normal")
        self.api_key_4_entry.config(state="normal")
        self.api_key_5_entry.config(state="normal")
        self.seconds_entry.config(state="normal")
        self.filename_entry.config(state="normal")  # Added to enable the filename text box
        # Log that the stop button was pushed and the thread stopped
        self.log("Stop button pushed \nSecondary loop thread stopped")

    @staticmethod
    def show_error_message(message):
        root = tk.Tk()
        root.withdraw()  # Hide the root window
        messagebox.showerror("Error", message)
        root.destroy()  # Destroy the root window when done


# start the overall class program.
if __name__ == '__main__':
    program = SheetsExtractProgram()