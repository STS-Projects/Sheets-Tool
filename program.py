import requests
import pandas as pd
import tkinter as tk
import threading
import time

# Libraries used:
# requests: pip install requests
# pandas: pip install pandas
# tkinter: pip install tkinter

# Create a global variable to store the state of the secondary loop thread
running = False


# Define a function to write and print a log message and append it to the text widget # Modified to embed the log into the GUI
def log(message):
    try:
        print(message)
        log_text.insert(tk.END, message + "\n")
        log_text.see(tk.END)
    except Exception as e:
        # Print and append any error in opening or writing to the log file
        print(f"Error opening or writing to log file: {e}")
        log_text.insert(tk.END, f"Error opening or writing to log file: {e}\n")
        log_text.see(tk.END)

# Create a config file if it does not exist and load it on startup
config_file = "config.txt"
try:
    with open(config_file, "r") as f:
        lines = f.readlines()
        try:
            spreadsheet_id = lines[0].strip()
        except IndexError:
            spreadsheet_id = "" # Added to handle missing parameter in config file
        try:
            worksheet = lines[1].strip()
        except IndexError:
            worksheet = "" # Added to handle missing parameter in config file
        try:
            api_key = lines[2].strip()
        except IndexError:
            api_key = "" # Added to handle missing parameter in config file
        try:
            seconds = int(lines[3].strip())
        except (IndexError, ValueError):
            seconds = 2 # Added to handle missing or invalid parameter in config file
        try:
            filename = lines[4].strip()
        except IndexError:
            filename = "" # Added to handle missing parameter in config file
except FileNotFoundError:
    with open(config_file, "w") as f:
        f.write("spreadsheet_id\n")
        f.write("worksheet\n")
        f.write("api_key\n")
        f.write("seconds\n")
        f.write("filename\n") # Added to create a new config file with the filename field
    spreadsheet_id = ""
    worksheet = ""
    api_key = ""
    seconds = 0
    filename = "" # Added to initialize the filename variable

# Create the GUI with the text boxes, buttons and status box
window = tk.Tk()
window.title("Google Sheet Data Puller")
window.geometry("600x800") # Changed to make the window taller
window.resizable(False, False)

# Create a frame to hold the text boxes and labels
frame = tk.Frame(window)
frame.pack(padx=10, pady=10)

# Create the text boxes and labels for the spreadsheet ID, worksheet name, API key, seconds to loop and filename # Added a text box and a label for the filename
spreadsheet_id_entry = tk.Entry(frame, width=30, font=("Arial", 16))
spreadsheet_id_entry.insert(0, spreadsheet_id)
spreadsheet_id_label = tk.Label(frame, text="Spreadsheet ID:", font=("Arial", 16))
worksheet_entry = tk.Entry(frame, width=30, font=("Arial", 16))
worksheet_entry.insert(0, worksheet)
worksheet_label = tk.Label(frame, text="Worksheet Name:", font=("Arial", 16))
api_key_entry = tk.Entry(frame, width=30, font=("Arial", 16))
api_key_entry.insert(0, api_key)
api_key_label = tk.Label(frame, text="API Key:", font=("Arial", 16))
seconds_entry = tk.Entry(frame, width=30, font=("Arial", 16))
seconds_entry.insert(0, seconds)
seconds_label = tk.Label(frame, text="Seconds to Loop:", font=("Arial", 16))
filename_entry = tk.Entry(frame, width=30, font=("Arial", 16)) # Changed the width to make the text box shorter
filename_entry.insert(0, filename) # Added to insert the filename from the config file
filename_label = tk.Label(frame, text="Output Filename:", font=("Arial", 16)) # Added a label for the filename
csv_label = tk.Label(frame, text=".csv", font=("Arial", 16)) # Added a label to show the file extension

# Create the buttons for start, stop and save
start_button = tk.Button(window, text="Start", font=("Arial", 16), width=10)
stop_button = tk.Button(window, text="Stop", font=("Arial", 16), width=10)
save_button = tk.Button(window, text="Save", font=("Arial", 16), width=10)

# Create the status box to show the readiness or running state of the program
status_box = tk.Label(window, text="", font=("Arial", 30), width=10)

# Create a text widget to display the log # Added to embed the log into the GUI
log_text = tk.Text(window, font=("Arial", 12))
log_text.place(x=0, y=550, relwidth=1, height=250) # Changed the height to fill the space

# Define a function to update the status box based on the data fields
def update_status():
    global spreadsheet_id
    global worksheet
    global api_key
    global seconds
    global filename # Added to update the filename variable
    spreadsheet_id = spreadsheet_id_entry.get()
    worksheet = worksheet_entry.get()
    api_key = api_key_entry.get()
    try:
        seconds = int(seconds_entry.get())
    except ValueError:
        seconds = 0
    filename = filename_entry.get() # Added to get the filename from the text box
    if spreadsheet_id and worksheet and api_key and seconds > 0 and filename: # Added to check if the filename is not empty
        status_box.config(text="READY", bg="green")
        log("Status updated: READY") # Added to log the status update
    else:
        status_box.config(text="NOT READY", bg="gray")
        log("Status updated: NOT READY") # Added to log the status update

# Define a function to save the data fields to the config file
def save_settings():
    global spreadsheet_id
    global worksheet
    global api_key
    global seconds
    global filename # Added to save the filename variable
    update_status()
    try:
        with open(config_file, "w") as f:
            f.write(spreadsheet_id + "\n")
            f.write(worksheet + "\n")
            f.write(api_key + "\n")
            f.write(str(seconds) + "\n")
            f.write(filename + "\n") # Added to save the filename to the config file
        log("Settings saved") # Added to log the settings save
    except Exception as e:
        log(f"Error saving settings: {e}") # Added to log any error in saving settings

# Bind the save button to the save settings function
save_button.config(command=save_settings)

# Update the status box initially
update_status()
# Arrange the widgets in a grid layout
spreadsheet_id_label.grid(row=0, column=0)
spreadsheet_id_entry.grid(row=0, column=1)
worksheet_label.grid(row=1, column=0)
worksheet_entry.grid(row=1, column=1)
api_key_label.grid(row=2, column=0)
api_key_entry.grid(row=2, column=1)
seconds_label.grid(row=3, column=0)
seconds_entry.grid(row=3, column=1)
filename_label.grid(row=4, column=0) # Added to place the filename label in the grid layout
filename_entry.grid(row=4, column=1) # Added to place the filename text box in the grid layout
csv_label.grid(row=4, column=2) # Added to place the file extension label in the grid layout
start_button.place(x=150, y=250)
stop_button.place(x=350, y=250)
save_button.place(x=250, y=350)
status_box.place(x=195, y=450)

# Create a global variable to store the state of the secondary loop thread
running = False

try:
    # Print the initial message to the console
    print("Program started")
    # Append the initial message to the text widget
    log_text.insert(tk.END, "Program started\n")
    log_text.see(tk.END)
except Exception as e:
    # Print and append any error in opening or writing to the log file
    print(f"Error opening or writing to log file: {e}")
    log_text.insert(tk.END, f"Error opening or writing to log file: {e}\n")
    log_text.see(tk.END)

# Define a function to get the sheet data and transpose it using the pandas library
def get_sheet_data():
    global error_message
    try:
        response = requests.get(
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{worksheet}?key={api_key}")
        response.raise_for_status()

        data = response.json()
        rows = data["values"]
        rows.pop(0)
        tmp = {k[0]: k[1:] for k in rows}
        tmp = pd.DataFrame.from_dict(tmp, orient='index')
        tmp = tmp.transpose()
        tmp.to_csv(filename + ".csv", index=False, encoding="utf-8") # Changed to use the filename variable as the output file name
        error_message = ""
        # Log the success message
        log(f"Successfully wrote to {filename}.csv") # Changed to show the filename in the log message
    except requests.exceptions.HTTPError as e:
        # Log the HTTP error message
        log(f"HTTP Error: {e}")
    except PermissionError as e:
        # Log and show the permission error message
        error_message = "CANNOT WRITE TO DISK, FILE IN USE"
        log(f"Permission Error: {e}")
        log("You may have the CSV file open, please close it!")
    except Exception as e:
        # Log any other error message
        log(f"Other Error: {e}")
    time.sleep(seconds)

# Define a function to run the secondary loop in a separate thread
def secondary_loop():
    global running
    while running:
        get_sheet_data()

# Define a function to start the secondary loop thread
def start_loop():
    global running
    start_button.config(state=tk.DISABLED)
    update_status()
    if status_box["text"] == "READY":
        running = True
        status_box.config(text="RUNNING", bg="red")
        # Gray out the text input boxes
        spreadsheet_id_entry.config(state="disabled")
        worksheet_entry.config(state="disabled")
        api_key_entry.config(state="disabled")
        seconds_entry.config(state="disabled")
        filename_entry.config(state="disabled") # Added to gray out the filename text box
        # Create and start the secondary loop thread
        thread = threading.Thread(target=secondary_loop)
        thread.start()
        # Log that the start button was pushed and the thread started
        log("Start button pushed")
        log("Secondary loop thread started")
    else:
        # Log that the start button was pushed but the status was not ready
        log("Start button pushed")
        log("Status not ready")
        start_button.config(state=tk.NORMAL)

# Define a function to stop the secondary loop thread
def stop_loop():
    global running
    running = False
    start_button.config(state=tk.NORMAL)
    status_box.config(text="READY", bg="green")
    # Enable the text input boxes
    spreadsheet_id_entry.config(state="normal")
    worksheet_entry.config(state="normal")
    api_key_entry.config(state="normal")
    seconds_entry.config(state="normal")
    filename_entry.config(state="normal") # Added to enable the filename text box
    # Log that the stop button was pushed and the thread stopped
    log("Stop button pushed")
    log("Secondary loop thread stopped")

# Bind the start button to the start loop function
start_button.config(command=start_loop)

# Bind the stop button to the stop loop function
stop_button.config(command=stop_loop)

# Create a main loop to update the GUI
def main_loop():
    window.after(1000, main_loop)

# Start the main loop
main_loop()

# Start the GUI
window.mainloop()
