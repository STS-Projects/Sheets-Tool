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
        self.api_key = ""
        self.seconds = 1
        self.filename = ""
        self.threadRunning = False
        self.errorMessage = None
        self.thread = None
        self.config_file = "config.ini"
        self.config = configparser.ConfigParser()

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
            self.api_key = self.config['MAIN']['api_key']
            self.seconds = float(self.config['MAIN']['seconds'])
            self.filename = self.config['MAIN']['filename']
        except Exception:
            if not self.config.has_section('MAIN'):
                self.config.add_section('MAIN')
            self.config.set('MAIN', 'spreadsheet_id', self.spreadsheet_id)
            self.config.set('MAIN', 'worksheet', self.worksheet)
            self.config.set('MAIN', 'api_key', self.api_key)
            self.config.set('MAIN', 'seconds', str(self.seconds))
            self.config.set('MAIN', 'filename', self.filename)

            with open(self.config_file, 'w') as configfile:
                self.config.write(configfile)

        # Create the GUI with the text boxes, buttons and status box
        self.window = tk.Tk()
        self.window.title("Google Sheet Data Puller")
        self.window.geometry("700x600")  # Changed to make the window taller
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
        self.api_key_entry = tk.Entry(self.frame, width=30, font=("Arial", 16))
        self.api_key_entry.insert(0, self.api_key)
        self.api_key_label = tk.Label(self.frame, text="API Key:", font=("Arial", 16))
        self.seconds_entry = tk.Entry(self.frame, width=30, font=("Arial", 16))
        self.seconds_entry.insert(0, str(self.seconds))
        self.seconds_label = tk.Label(self.frame, text="Seconds to Loop:", font=("Arial", 16))
        self.filename_entry = tk.Entry(self.frame, width=30,
                                       font=("Arial", 16))  # Changed the width to make the text box shorter
        self.filename_entry.insert(0, self.filename)  # Added to insert the filename from the config file
        self.filename_label = tk.Label(self.frame, text="Output Filename:",
                                       font=("Arial", 16))  # Added a label for the filename
        self.csv_label = tk.Label(self.frame, text=".csv",
                                  font=("Arial", 16))  # Added a label to show the file extension

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
        self.api_key_label.grid(row=2, column=0)
        self.api_key_entry.grid(row=2, column=1)
        self.seconds_label.grid(row=3, column=0)
        self.seconds_entry.grid(row=3, column=1)
        self.filename_label.grid(row=4, column=0)  # Added to place the filename label in the grid layout
        self.filename_entry.grid(row=4, column=1)  # Added to place the filename text box in the grid layout
        self.csv_label.grid(row=4, column=2)  # Added to place the file extension label in the grid layout
        self.startStop_btn.grid(row=5, columnspan=2, pady=5)
        self.save_button.grid(row=6, columnspan=2, pady=5)
        self.status_box.grid(row=7, columnspan=2, pady=5)
        # Create a text widget to display the log # Added to embed the log into the GUI
        self.log_text = tk.Text(self.window, height=15, font=("Arial", 12))
        self.log_text.grid(row=8, columnspan=1, pady=5)  # Changed the height to fill the space

        self.startStop_btn.config(command=self.start_loop)

        # Bind the save button to the save settings function
        self.save_button.config(command=self.save_settings)

        try:
            # Print the initial message to the console
            print("Program started")
            # Append the initial message to the text widget
            self.log_text.insert(tk.END, "Program started\n")
            self.log_text.see(tk.END)
        except Exception as e:
            # Print and append any error in opening or writing to the log file
            print(f"Error opening or writing to log file: {e}")
            self.log_text.insert(tk.END, f"Error opening or writing to log file: {e}\n")
            self.log_text.see(tk.END)
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

    # Define a function to write and print a log message and append it to the text widget
    # Modified to embed the log into the GUI
    def log(self, message):
        try:
            print(message)
            self.log_text.insert(tk.END, message + "\n")
            self.log_text.see(tk.END)
        except Exception as e:
            # Print and append any error in opening or writing to the log file
            print(f"Error opening or writing to log file: {e}")
            self.log_text.insert(tk.END, f"Error opening or writing to log file: {e}\n")
            self.log_text.see(tk.END)

    # Define a function to update the status box based on the data fields
    def update_status(self):
        print("updating values")
        self.spreadsheet_id = self.spreadsheet_id_entry.get()
        self.worksheet = self.worksheet_entry.get()
        self.api_key = self.api_key_entry.get()
        try:
            self.seconds = float(self.seconds_entry.get())
        except ValueError:
            self.seconds = 0
        self.filename = self.filename_entry.get()  # Added to get the filename from the text box
        if not self.threadRunning:
            if self.spreadsheet_id and self.worksheet and self.api_key and self.seconds > 0 and self.filename:  # Added
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
        self.config.set('MAIN', 'api_key', self.api_key)
        self.config.set('MAIN', 'seconds', str(self.seconds))
        self.config.set('MAIN', 'filename', self.filename)
        with open(self.config_file, 'w') as configfile:
            self.config.write(configfile)

    # Define a function to get the sheet data and transpose it using the pandas library
    def get_sheet_data(self):

        try:
            response = requests.get(
                f"https://sheets.googleapis.com/v4/spreadsheets/{self.spreadsheet_id}/values/{self.worksheet}"
                f"?key={self.api_key}")
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
            self.api_key_entry.config(state="disabled")
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
        self.api_key_entry.config(state="normal")
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
