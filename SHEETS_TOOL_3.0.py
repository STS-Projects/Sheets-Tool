# Required libraries:
# pip install google-api-python-client google-auth-httplib2 pandas pygame requests

import ctypes
ctypes.windll.kernel32.AllocConsole()

import tkinter as tk
from tkinter import ttk, font, messagebox
import configparser
import logging
import os
import time
import threading
import queue
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pygame # Import pygame for audio with volume control
import urllib.request # Added for vMix API calls
import urllib.error # Added for vMix API error handling

# --- Constants ---
CONFIG_FILE = 'config.ini'
LOG_FILE = 'log.txt'
DEFAULT_LOOP_SECONDS = 1.0
DEFAULT_SOUND_FILE = 'notification.wav'
DEFAULT_SOUND_VOLUME = 100 # Volume percentage (0-100)
THREAD_TIMEOUT_SECONDS = 5.0 # Max time for API call thread
API_CENSOR_STARS = '*' * 20 # Use 20 stars for censoring
CONFIG_SAVE_DISPLAY_MS = 2000 # 2 seconds for "CONFIG SAVED" message
DEFAULT_VMIX_API_HEADER = 'vMixCommand' # Consistent naming

# --- Global Variables ---
config = configparser.ConfigParser()
is_running = False
loop_thread = None
worker_thread = None
stop_event = threading.Event()
result_queue = queue.Queue()
current_api_key_index = 0
last_data_pulled = None # Stores the previously fetched data (as DataFrame) for comparison
current_active_worker_instance_id = None # Track the unique ID string of the intended active worker
pygame_mixer_initialized = False # Flag to track mixer initialization
revert_status_job_id = None # To store the ID of the scheduled status revert task
force_write_on_next_pull = False # Flag to force writing CSV on the first pull after starting
last_vmix_api_id = None # Stores the ID of the last executed vMix command
skip_next_vmix_execution_on_change = False # Flag to skip the *first* vMix execution after start

# --- Logging Setup ---
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# File Handler
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

# Console Handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# --- Configuration ---
# load_config, save_config, revert_status_label, censor_api_key remain unchanged
def censor_api_key(api_key):
    """Censors all but the last 4 characters of an API key with many stars."""
    if isinstance(api_key, str) and len(api_key) > 4:
        return f"{API_CENSOR_STARS}{api_key[-4:]}"
    elif isinstance(api_key, str) and len(api_key) > 0:
         return API_CENSOR_STARS # Censor short keys completely
    return "EMPTY"

def load_config():
    """Loads configuration from config.ini or creates it with defaults."""
    global config
    defaults = {
        'Settings': {
            'spreadsheet_id': '',
            'worksheet_name': '',
            'api_key_1': '',
            'api_key_2': '',
            'api_key_3': '',
            'api_key_4': '',
            'api_key_5': '',
            'loop_seconds': str(DEFAULT_LOOP_SECONDS),
            'output_csv_filename': 'output',
            'transpose_data': 'False',
            'play_sound_on_change': 'False',
            'sound_filename': DEFAULT_SOUND_FILE,
            'sound_volume': str(DEFAULT_SOUND_VOLUME),
            'vmix_api_enabled': 'False',
            'vmix_api_header': DEFAULT_VMIX_API_HEADER,
        }
    }
    if not os.path.exists(CONFIG_FILE):
        logger.info(f"Configuration file '{CONFIG_FILE}' not found. Creating with default values.")
        config.read_dict(defaults)
        try:
            with open(CONFIG_FILE, 'w') as configfile:
                config.write(configfile)
            logger.info(f"Successfully created '{CONFIG_FILE}'. Please edit it with your details.")
        except IOError as e:
            logger.error(f"Failed to create configuration file '{CONFIG_FILE}': {e}")
            messagebox.showerror("Config Error", f"Could not create config file: {e}")
            config = configparser.ConfigParser()
            config.read_dict(defaults)

    else:
        logger.info(f"Loading configuration from '{CONFIG_FILE}'.")
        try:
            config = configparser.ConfigParser(interpolation=None) # Disable interpolation for safety
            config.read_dict(defaults) # Apply defaults
            config.read(CONFIG_FILE) # Override with file contents

            logger.info("Loaded Configuration:")
            logger.info(f"  Spreadsheet ID: {config.get('Settings', 'spreadsheet_id')}")
            logger.info(f"  Worksheet Name: {config.get('Settings', 'worksheet_name')}")
            for i in range(1, 6):
                key_name = f'api_key_{i}'
                key_value = config.get('Settings', key_name)
                logger.info(f"  API Key {i}: {censor_api_key(key_value)}")
            logger.info(f"  Loop Seconds: {config.getfloat('Settings', 'loop_seconds')}")
            logger.info(f"  Output CSV Filename: {config.get('Settings', 'output_csv_filename')}")
            logger.info(f"  Transpose Data: {config.getboolean('Settings', 'transpose_data')}")
            logger.info(f"  Play Sound on Change: {config.getboolean('Settings', 'play_sound_on_change')}")
            logger.info(f"  Sound Filename: {config.get('Settings', 'sound_filename')}")
            try:
                vol = config.getint('Settings', 'sound_volume')
                if not (0 <= vol <= 100):
                    logger.warning(f"Config sound_volume '{vol}' out of range (0-100). Correcting to {DEFAULT_SOUND_VOLUME}.")
                    vol = DEFAULT_SOUND_VOLUME
                    config.set('Settings', 'sound_volume', str(vol)) # Correct in config object for consistency
                logger.info(f"  Sound Volume: {vol}%")
            except ValueError:
                logger.error(f"Invalid sound_volume in config. Using default {DEFAULT_SOUND_VOLUME}.")
                config.set('Settings', 'sound_volume', str(DEFAULT_SOUND_VOLUME))
                logger.info(f"  Sound Volume: {DEFAULT_SOUND_VOLUME}%")

            logger.info(f"  vMix API Enabled: {config.getboolean('Settings', 'vmix_api_enabled')}")
            logger.info(f"  vMix API Header: {config.get('Settings', 'vmix_api_header')}")

        except (configparser.Error, ValueError, KeyError) as e:
            logger.error(f"Error reading configuration file '{CONFIG_FILE}': {e}. Some values might revert to defaults.")
            messagebox.showerror("Config Error", f"Error reading config file: {e}\nSome values might use defaults.")

def save_config():
    """Saves current GUI settings to config.ini and shows temporary status."""
    global config, revert_status_job_id
    logger.info(f"Attempting to save configuration to '{CONFIG_FILE}'.")
    previous_status_text = status_label.cget('text')
    previous_status_color = status_label.cget('fg')

    try:
        if not config.has_section('Settings'):
            config.add_section('Settings')

        config.set('Settings', 'spreadsheet_id', entry_spreadsheet_id.get())
        config.set('Settings', 'worksheet_name', entry_worksheet_name.get())
        for i, entry in enumerate(api_key_entries, 1):
            config.set('Settings', f'api_key_{i}', entry.get())
        config.set('Settings', 'loop_seconds', entry_loop_seconds.get())
        config.set('Settings', 'output_csv_filename', entry_csv_filename.get())
        config.set('Settings', 'transpose_data', str(transpose_var.get()))
        config.set('Settings', 'play_sound_on_change', str(sound_var.get()))
        try:
            current_sound_file = config.get('Settings', 'sound_filename')
            config.set('Settings', 'sound_filename', current_sound_file)
        except (configparser.NoSectionError, configparser.NoOptionError):
             config.set('Settings', 'sound_filename', DEFAULT_SOUND_FILE) # Fallback if missing

        config.set('Settings', 'sound_volume', str(int(volume_var.get())))
        config.set('Settings', 'vmix_api_enabled', str(vmix_api_enabled_var.get()))
        config.set('Settings', 'vmix_api_header', entry_vmix_header.get())


        with open(CONFIG_FILE, 'w') as configfile:
            config.write(configfile)
        logger.info(f"Successfully saved configuration to '{CONFIG_FILE}'.")

        set_status("CONFIG SAVED", "orange")
        if revert_status_job_id:
            try:
                root.after_cancel(revert_status_job_id)
            except ValueError: pass # Ignore if ID is invalid
        revert_status_job_id = root.after(CONFIG_SAVE_DISPLAY_MS,
                                          revert_status_label,
                                          previous_status_text,
                                          previous_status_color)

    except (IOError, configparser.Error, ValueError, tk.TclError) as e:
        logger.error(f"Failed to save configuration file '{CONFIG_FILE}': {e}")
        messagebox.showerror("Config Error", f"Could not save config file: {e}")
        if root and status_label: # Check if label still exists
             try:
                 set_status(previous_status_text, previous_status_color) # Revert status immediately on failure
             except tk.TclError: pass # Ignore if GUI is gone

def revert_status_label(original_text, original_color):
    """Reverts the status label to its previous state."""
    global revert_status_job_id
    logger.debug(f"Reverting status label to: {original_text} ({original_color})")
    if is_running:
        set_status("RUNNING", "red")
    else:
        set_status_based_on_inputs() # Re-evaluate READY/NOT READY
    revert_status_job_id = None

# --- Google Sheets Interaction ---
# fetch_data_worker, get_next_api_key remain unchanged
def fetch_data_worker(api_key, spreadsheet_id, worksheet_name, result_queue, worker_instance_id):
    """Fetches data from Google Sheets using a specific API key. Runs in a thread."""
    global current_active_worker_instance_id
    logger.info(f"{worker_instance_id}: Attempting to fetch data using API Key: {censor_api_key(api_key)}")
    try:
        service = build('sheets', 'v4', developerKey=api_key, cache_discovery=False)
        sheet = service.spreadsheets()
        range_name = f"'{worksheet_name}'"
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        values = result.get('values', [])

        if worker_instance_id == current_active_worker_instance_id:
            result_queue.put({'data': values, 'api_key': api_key, 'worksheet': worksheet_name, 'success': True, 'worker_id': worker_instance_id})
            logger.info(f"{worker_instance_id}: Successfully fetched data using API key {censor_api_key(api_key)} and queueing result.")
        else:
            logger.warning(f"{worker_instance_id}: Data fetched, but the globally active worker is now '{current_active_worker_instance_id}'. Discarding result.")

    except HttpError as err:
        logger.error(f"{worker_instance_id}: Google API HTTP Error for key {censor_api_key(api_key)}: {err.resp.status} {err.resp.reason} - {err.content}")
        if worker_instance_id == current_active_worker_instance_id:
            result_queue.put({'error': err, 'api_key': api_key, 'success': False, 'worker_id': worker_instance_id})
        else:
             logger.warning(f"{worker_instance_id}: HTTP Error occurred, but the globally active worker is now '{current_active_worker_instance_id}'. Discarding error.")

    except Exception as e:
        logger.error(f"{worker_instance_id}: Unexpected error fetching data with key {censor_api_key(api_key)}: {e}", exc_info=True)
        if worker_instance_id == current_active_worker_instance_id:
             result_queue.put({'error': e, 'api_key': api_key, 'success': False, 'worker_id': worker_instance_id})
        else:
            logger.warning(f"{worker_instance_id}: Exception occurred, but the globally active worker is now '{current_active_worker_instance_id}'. Discarding error.")

def get_next_api_key():
    """Cycles through the available API keys."""
    global current_api_key_index
    api_keys = [entry.get() for entry in api_key_entries if entry.get()]
    if not api_keys:
        logger.error("No valid API keys provided.")
        return None
    key_to_use = api_keys[current_api_key_index % len(api_keys)]
    current_api_key_index = (current_api_key_index + 1) % len(api_keys)
    return key_to_use


# --- vMix API Call ---
# execute_vmix_api, update_vmix_status_label remain unchanged
def execute_vmix_api(api_url):
    """
    Executes a vMix Web API call. Runs Synchronously.

    Args:
        api_url (str): The full URL for the vMix API call.

    Returns:
        tuple: (status_code, response_text)
               status_code is the HTTP status code (e.g., 200, 500) or None on connection errors.
               response_text is the content returned by vMix or an error message.
    """
    if not api_url or not isinstance(api_url, str) or not api_url.startswith(('http://', 'https://')):
        logger.error(f"[vMix API] Invalid vMix API URL provided: {api_url}")
        return None, "Invalid API URL format"

    logger.info(f"[vMix API] Executing: {api_url}")
    try:
        with urllib.request.urlopen(api_url, timeout=5) as response: # 5 second timeout
            status_code = response.getcode()
            response_text = response.read().decode('utf-8', errors='ignore') # Read response body
            logger.info(f"[vMix API] Response status: {status_code}")
            if response_text:
                if status_code != 200 or len(response_text) < 200: # Avoid logging huge success responses
                    logger.info(f"[vMix API] Response content:\n---\n{response_text}\n---")
                else:
                     logger.info("[vMix API] Response content received (likely XML, length > 200).")
            return status_code, response_text
    except urllib.error.HTTPError as e:
        logger.error(f"[vMix API] HTTP Error: {e.code} {e.reason}")
        error_body = ""
        try: # Try to read error body if available
            error_body = e.read().decode('utf-8', errors='ignore')
            logger.error(f"[vMix API] Error response body:\n---\n{error_body}\n---")
        except Exception: pass
        response_text = f"HTTP Error {e.code} {e.reason}" + (f"\n{error_body}" if error_body else "")
        return e.code, response_text
    except urllib.error.URLError as e:
        logger.error(f"[vMix API] URL Error (e.g., connection refused, DNS): {e.reason}")
        return None, f"URL Error: {e.reason}"
    except TimeoutError:
        logger.error("[vMix API] Request timed out.")
        return None, "Request Timed Out"
    except Exception as e:
        logger.error(f"[vMix API] Unexpected error during vMix API call: {e}", exc_info=True)
        return None, f"Request Failed: {e}"

def update_vmix_status_label(status_code, message=""):
    """Updates the vMix status label in the GUI. MUST be called from the main GUI thread or scheduled."""
    if not root or not vmix_status_label: return # Check if GUI elements exist
    try:
        if status_code == 200:
            text = f"vMix API OK ({status_code})"
            color = "green"
        elif status_code is None: # Connection errors etc.
            text = f"vMix API Request Failed"
            if isinstance(message, str):
                first_line = message.splitlines()[0] if message else ""
                if first_line:
                    text += f": {first_line[:60]}" # Limit length
                    if len(first_line) > 60: text += "..."
            color = "red"
        else: # Other HTTP errors (e.g., 500)
            text = f"vMix API Error ({status_code})"
            if isinstance(message, str):
                first_line = message.splitlines()[0] if message else ""
                if first_line:
                    text += f": {first_line[:60]}" # Limit length
                    if len(first_line) > 60: text += "..."
            color = "red"

        current_text = vmix_status_label.cget('text')
        current_color = vmix_status_label.cget('fg')

        if current_text != text or current_color != color:
             vmix_status_label.config(text=text, fg=color)

    except tk.TclError as e:
        logger.warning(f"Failed to update vMix status label (TclError): {e}")
    except Exception as e:
         logger.error(f"Unexpected error updating vMix status label: {e}", exc_info=True)


# --- vMix Processing Function (runs in thread) ---
def process_vmix_api_call(csv_filename, header_name):
    """
    Reads the specified CSV file, checks for the vMix command based on the
    header name in the first row, compares the API ID from the second row,
    and executes the API call if needed. Designed to run in a separate thread.
    Skips execution but updates ID on the first change detected after start.
    """
    global last_vmix_api_id, skip_next_vmix_execution_on_change # <<< ADDED GLOBALS HERE
    logger.info(f"[vMix Thread] Processing CSV '{csv_filename}' for header '{header_name}'.")

    # --- Validate Inputs ---
    if not header_name or not isinstance(header_name, str):
        logger.error(f"[vMix Thread] Invalid vMix header name provided: '{header_name}'. Aborting.")
        root.after(0, update_vmix_status_label, None, "Invalid Header Name")
        return
    if not csv_filename or not isinstance(csv_filename, str):
        logger.error(f"[vMix Thread] Invalid CSV filename provided: '{csv_filename}'. Aborting.")
        root.after(0, update_vmix_status_label, None, "Invalid CSV Filename")
        return

    # --- Read CSV File ---
    try:
        # Read without header, treat all as strings initially to preserve IDs
        df_from_csv = pd.read_csv(csv_filename, header=None, dtype=str, keep_default_na=False)

        if df_from_csv.empty:
            logger.warning(f"[vMix Thread] CSV file '{csv_filename}' is empty. Cannot process.")
            return

        if df_from_csv.shape[0] < 2:
             logger.warning(f"[vMix Thread] CSV file '{csv_filename}' has less than 2 rows. Cannot find header and value.")
             root.after(0, update_vmix_status_label, None, "CSV too short (<2 rows)")
             return

        # --- Find Header Column and Get Value ---
        first_row = df_from_csv.iloc[0]
        target_col_index = -1

        # Find the first column index where the value in the first row matches the header_name
        for idx, value in enumerate(first_row):
            # Case-sensitive match after stripping whitespace
            if isinstance(value, str) and value.strip() == header_name.strip():
                target_col_index = idx
                break # Use the first match

        if target_col_index == -1:
            logger.warning(f"[vMix Thread] Header '{header_name}' not found in the first row of '{csv_filename}'.")
            root.after(0, update_vmix_status_label, None, f"Header '{header_name}' not found")
            return

        # Get the value from the second row (index 1) at the found column index
        cell_value = df_from_csv.iloc[1, target_col_index]
        found_location = f"row 2, column {target_col_index+1} (header '{header_name}' found in row 1)"
        logger.debug(f"[vMix Thread] Found value '{cell_value}' at {found_location}")

        # --- Process Value ---
        if cell_value and isinstance(cell_value, str) and cell_value.strip():
            if ',' in cell_value:
                # Split on every comma and trim whitespace from each part
                parts = [p.strip() for p in cell_value.split(',')]
                current_api_id = parts[0]
                commands = parts[1:]
                
                # Validate API ID and ensure at least one command is provided
                if not current_api_id:
                    logger.warning("\033[91m%s\033[0m", f"[vMix Thread] Extracted API ID is empty from cell value '{cell_value}'. Skipping.")
                    return
                if len(commands) == 0:
                    logger.warning("\033[91m%s\033[0m", "[vMix Thread] No API command provided after the ID. Skipping.")
                    root.after(0, update_vmix_status_label, None, "No API command provided")
                    return
                if len(commands) > 10:
                    logger.warning("\033[91m%s\033[0m", "[vMix Thread] More than 10 API commands provided. Only executing the first 10.")
                    commands = commands[:10]
                
                # --- Compare ID and Execute ---
                if current_api_id != last_vmix_api_id:
                    execute_api = True

                    # --- Skip execution on the first change after start ---
                    if skip_next_vmix_execution_on_change:
                        logger.info(f"[vMix Thread] First change detected after start (ID: '{current_api_id}'). Skipping execution, but updating ID tracker.")
                        execute_api = False
                        skip_next_vmix_execution_on_change = False  # Consume the flag
                    logger.info(f"[vMix Thread] Updating last known vMix API ID from '{last_vmix_api_id}' to '{current_api_id}'.")
                    last_vmix_api_id = current_api_id

                    # Execute the API commands if allowed
                    if execute_api:
                        logger.info("\033[38;5;208m%s\033[0m", f"[vMix Thread] New API ID detected and execution allowed. Executing commands for ID '{current_api_id}'.")
                        responses = []
                        last_status_code = None
                        for cmd in commands:
                            if cmd:  # Ensure command is not empty
                                status_code, response_msg = execute_vmix_api(cmd)
                                responses.append(response_msg)
                                last_status_code = status_code  # Use the last status code (could be adjusted as needed)
                        combined_response = "|".join(responses)
                        root.after(0, update_vmix_status_label, last_status_code, combined_response)
                else:
                    logger.info(f"[vMix Thread] API ID ('{current_api_id}') hasn't changed since last known ID. Skipping.")
            else:
                logger.warning("\033[91m%s\033[0m", f"[vMix Thread] Value in cell ('{cell_value}') is not in the expected '<id>,<command>' format.")
                root.after(0, update_vmix_status_label, None, "Invalid cell format")


    except FileNotFoundError:
        logger.error(f"[vMix Thread] CSV file not found: '{csv_filename}'")
        root.after(0, update_vmix_status_label, None, "CSV file not found")
    except pd.errors.EmptyDataError:
        logger.warning(f"[vMix Thread] CSV file '{csv_filename}' is empty (Pandas EmptyDataError). Cannot process.")
        root.after(0, update_vmix_status_label, None, "CSV is empty")
    except PermissionError:
         logger.error(f"[vMix Thread] Permission denied reading CSV file: '{csv_filename}'")
         root.after(0, update_vmix_status_label, None, "CSV permission denied")
    except IndexError as e:
         logger.error(f"[vMix Thread] IndexError accessing CSV data in '{csv_filename}' (likely accessing row/col that doesn't exist): {e}", exc_info=True)
         root.after(0, update_vmix_status_label, None, "CSV data access error")
    except Exception as e:
         logger.error(f"[vMix Thread] Unexpected error processing CSV '{csv_filename}' for vMix: {e}", exc_info=True)
         root.after(0, update_vmix_status_label, None, f"CSV Processing Error: {e}")


# --- Main Loop Logic ---
def run_loop():
    """The main loop that triggers data fetching periodically."""
    global is_running, last_data_pulled, worker_thread, current_active_worker_instance_id, force_write_on_next_pull, last_vmix_api_id
    logger.info("Starting data fetch loop.")

    GREEN = '\033[92m'
    RESET = '\033[0m'

    while is_running:
        loop_start_time = time.monotonic()
        current_status_text = status_label.cget('text')
        if current_status_text != "RUNNING" and "ERROR" not in current_status_text and "CONFIG SAVED" not in current_status_text:
             set_status("RUNNING", "red")

        # Get Parameters
        spreadsheet_id = entry_spreadsheet_id.get()
        worksheet_name = entry_worksheet_name.get()
        csv_filename = entry_csv_filename.get() # Crucial for the vMix thread now
        should_transpose = transpose_var.get()
        should_play_sound = sound_var.get()
        try:
            sound_file = config.get('Settings', 'sound_filename')
        except (configparser.NoOptionError, configparser.NoSectionError):
             sound_file = DEFAULT_SOUND_FILE
        current_vmix_api_enabled = vmix_api_enabled_var.get()
        current_vmix_api_header = entry_vmix_header.get() # Crucial for the vMix thread

        try:
            current_volume_percent = int(volume_var.get())
        except (ValueError, tk.TclError):
             current_volume_percent = DEFAULT_SOUND_VOLUME
             logger.warning("Could not read volume slider value, using default.")
        try:
            loop_interval = float(entry_loop_seconds.get())
            if loop_interval <= 0: loop_interval = DEFAULT_LOOP_SECONDS
        except ValueError:
            loop_interval = DEFAULT_LOOP_SECONDS
            logger.warning("Invalid loop interval format. Using default.")

        # --- Google API Fetch Start ---
        api_key = get_next_api_key()
        if not api_key:
            set_status("ERROR: No API Keys", "red")
            time.sleep(1) # Prevent tight loop with no keys
            continue

        while not result_queue.empty():
            try: old_result = result_queue.get_nowait()
            except queue.Empty: break
            logger.debug(f"Loop: Discarding stale result from queue: {old_result.get('worker_id', 'Unknown')}")

        worker_instance_id = f"Worker-{int(loop_start_time * 1000)}"
        current_active_worker_instance_id = worker_instance_id # Mark this worker as the one we expect results from
        logger.info(f"Loop: Intending to start {worker_instance_id} (setting as active).")
        worker_thread = threading.Thread(
            target=fetch_data_worker,
            args=(api_key, spreadsheet_id, worksheet_name, result_queue, worker_instance_id),
            daemon=True, name=worker_instance_id)
        worker_thread.start()
        logger.info(f"Loop: Thread for {worker_instance_id} started with key {censor_api_key(api_key)}.")
        # --- Google API Fetch End ---

        # --- Wait for Result ---
        result = None
        try:
            logger.debug(f"Loop: Waiting for result from active worker '{current_active_worker_instance_id}'...")
            result = result_queue.get(timeout=THREAD_TIMEOUT_SECONDS)
            if result and result.get('worker_id') != current_active_worker_instance_id:
               logger.warning(f"Loop: Received result from unexpected worker '{result.get('worker_id')}', expected '{current_active_worker_instance_id}'. Discarding.")
               result = None # Discard the stale result
        except queue.Empty:
            logger.warning(f"Loop: Timed out waiting for worker '{current_active_worker_instance_id}' after {THREAD_TIMEOUT_SECONDS} seconds (using key {censor_api_key(api_key)}).")
            if current_active_worker_instance_id == worker_instance_id:
                 current_active_worker_instance_id = None
                 logger.info(f"Loop: Cleared active worker ID due to timeout for {worker_instance_id}.")
            set_status("ERROR: API Timeout", "red")
        # --- End Wait for Result ---

        # --- Process Result ---
        if result:
            processed_worker_id = result.get('worker_id', 'Unknown')
            logger.debug(f"Loop: Processing result received from {processed_worker_id}")

            if result.get('success'):
                data = result.get('data')
                used_api_key = result.get('api_key')
                fetched_worksheet = result.get('worksheet')
                current_data = None # DataFrame placeholder

                # --- DataFrame Creation/Padding Logic (largely unchanged) ---
                if not data:
                    logger.warning(f"No data returned from {fetched_worksheet} using key {censor_api_key(used_api_key)}.")
                    current_status = status_label.cget('text')
                    if current_status != "RUNNING (No Data)" and "CONFIG SAVED" not in current_status:
                        set_status("RUNNING (No Data)", "orange")
                    current_data = pd.DataFrame()
                elif isinstance(data, list) and len(data) > 0:
                    header = data[0]
                    num_columns = len(header) if header else 0
                    data_rows = data[1:]

                    if not header:
                        logger.warning("Sheet data received but has no header row. Treating all as data.")
                        current_data = pd.DataFrame(data_rows)
                    elif not data_rows and header:
                       logger.warning("Sheet contains only a header row.")
                       current_data = pd.DataFrame(columns=header)
                       current_status_text = status_label.cget('text')
                       if ("ERROR" in current_status_text or "orange" in status_label.cget('fg')) and "CONFIG SAVED" not in current_status_text :
                          set_status("RUNNING", "red")
                    elif data_rows:
                       processed_data_rows = []
                       row_num = 1
                       for row in data_rows:
                           row_len = len(row)
                           if row_len == num_columns:
                               processed_data_rows.append(row)
                           elif row_len < num_columns:
                               padding = [''] * (num_columns - row_len)
                               processed_data_rows.append(row + padding)
                           else: # row_len > num_columns
                               logger.warning(f"Data row #{row_num} found with {row_len} items, header has {num_columns}. Truncating row.")
                               processed_data_rows.append(row[:num_columns])
                           row_num += 1
                       try:
                           current_data = pd.DataFrame(processed_data_rows, columns=header)
                           logger.debug(f"DataFrame created successfully with shape {current_data.shape}")
                           current_status_text = status_label.cget('text')
                           if ("ERROR" in current_status_text or "orange" in status_label.cget('fg')) and "CONFIG SAVED" not in current_status_text:
                               set_status("RUNNING", "red")
                       except Exception as df_creation_err:
                           logger.error(f"Error creating DataFrame after padding/processing: {df_creation_err}", exc_info=True)
                           set_status("ERROR: DataFrame Creation", "red")
                           set_error_message(f"DataFrame Error: {df_creation_err}")
                           current_data = None # Indicate failure
                    else: # Only header row existed case already handled
                       logger.warning("Data list was not empty but failed header/data rows check.")
                       current_data = pd.DataFrame(columns=header) # Empty DF with headers
                elif isinstance(data, list) and len(data) == 0: # Empty list returned
                     logger.warning(f"Empty list returned from {fetched_worksheet} using key {censor_api_key(used_api_key)}.")
                     current_status = status_label.cget('text')
                     if current_status != "RUNNING (No Data)" and "CONFIG SAVED" not in current_status:
                         set_status("RUNNING (No Data)", "orange")
                     current_data = pd.DataFrame()
                else: # Unexpected data format
                     logger.error(f"Unexpected data format received: {type(data)}. Skipping processing.")
                     set_status("ERROR: Bad Data Format", "red")
                     set_error_message(f"Bad Data Format: {type(data)}")
                     current_data = None # Indicate failure
                # --- End DataFrame Creation ---


                # --- Process DataFrame if successfully created/handled ---
                if current_data is not None: # Proceed only if DataFrame creation didn't fail
                    try:
                        # --- Determine if data changed or needs forced write ---
                        data_changed = False
                        change_reason = ""
                        if force_write_on_next_pull:
                             data_changed = True
                             change_reason = "First iteration after start."
                             logger.info("First iteration after start: Forcing data write.")
                             force_write_on_next_pull = False # Reset flag after use
                             logger.info("Resetting vMix API ID tracking on forced write.")
                        elif last_data_pulled is None:
                            data_changed = True
                            change_reason = "Initial data load."
                            logger.info("Initial data load.")
                            logger.info("Resetting vMix API ID tracking on initial load.")
                        elif not current_data.equals(last_data_pulled):
                            data_changed = True
                            change_reason = "Data content changed."
                            logger.info("Data change detected compared to last pull.")

                        # --- Perform actions only if data changed/forced ---
                        if data_changed:
                             # Play sound only if change was due to content and enabled
                             if change_reason == "Data content changed." and should_play_sound:
                                 logger.info("DATA UPDATE DETECTED - PLAYING SOUND")
                                 play_notification_sound(sound_file, current_volume_percent)

                             # --- Transpose right before writing, only if needed ---
                             df_to_write = current_data # Start with the original fetched data
                             if should_transpose:
                                 if not df_to_write.empty:
                                     logger.debug("Transposing data before writing.")
                                     try:
                                         df_to_write = df_to_write.T
                                     except Exception as transpose_err:
                                         logger.error(f"Error during data transposition: {transpose_err}")
                                         set_status("ERROR: Transpose failed", "red")
                                         df_to_write = None # Prevent further processing if transpose fails
                                 else:
                                     logger.debug("Skipping transpose for empty DataFrame.")

                             # --- CSV Write and vMix API Trigger ---
                             csv_written_successfully = False # Flag for vMix logic
                             if df_to_write is not None: # Proceed only if transpose didn't fail
                                 try:
                                     # Unconditionally append '.csv' to the provided filename
                                     csv_filename = entry_csv_filename.get() + ".csv"
                                     # --- Write to CSV ---
                                     df_to_write.to_csv(csv_filename, index=False, header=False)
                                     csv_written_successfully = True

                                     if change_reason == "Data content changed.": log_prefix = "DATA UPDATE DETECTED"
                                     elif change_reason == "First iteration after start.": log_prefix = "FORCED WRITE (POST-START)"
                                     else: log_prefix = "INITIAL WRITE"
                                     logger.info(f"{GREEN}{log_prefix} - WRITING TO '{csv_filename}' (Worker: {processed_worker_id}){RESET}")

                                     current_status_text = status_label.cget('text')
                                     if "CONFIG SAVED" not in current_status_text:
                                          clear_error_message()
                                          if current_status_text != "RUNNING":
                                              set_status("RUNNING", "red")

                                     last_data_pulled = current_data.copy() # Update last *original* data

                                     # --- Trigger vMix API Call (if enabled and CSV written) ---
                                     # Use the actual CSV filename now
                                     if csv_written_successfully and current_vmix_api_enabled and current_vmix_api_header:
                                         logger.info(f"[vMix Trigger] CSV written, vMix enabled. Starting vMix processing thread for header '{current_vmix_api_header}' in file '{csv_filename}'.")
                                         vmix_thread = threading.Thread(
                                             target=process_vmix_api_call,
                                             args=(csv_filename, current_vmix_api_header), # Pass filename and header name
                                             daemon=True,
                                             name="vMixAPIThread"
                                         )
                                         vmix_thread.start()
                                     elif csv_written_successfully and current_vmix_api_enabled and not current_vmix_api_header:
                                          logger.warning("[vMix Trigger] vMix API Check: Enabled, but no vMix API header specified in the text field.")
                                          root.after(0, update_vmix_status_label, None, "Header not specified")

                                 except (IOError, PermissionError) as write_err:
                                     logger.error(f"Cannot write to disk '{csv_filename}': {write_err}")
                                     set_error_message(f"CANNOT WRITE TO DISK: {write_err}")
                                     set_status("ERROR: File Write", "red")
                                     csv_written_successfully = False # Ensure flag is false on error
                                 except Exception as general_write_err:
                                     logger.error(f"Unexpected error writing CSV '{csv_filename}': {general_write_err}", exc_info=True)
                                     set_error_message(f"CSV WRITE FAILED: {general_write_err}")
                                     set_status("ERROR: File Write", "red")
                                     csv_written_successfully = False # Ensure flag is false on error
                             # --- End CSV Write and vMix API Trigger Section ---

                        else: # Data has not changed
                            logger.info("No data change detected. Skipping write and vMix check.")
                            current_status_text = status_label.cget('text')
                            if "CONFIG SAVED" not in current_status_text:
                                clear_error_message()
                                if current_status_text != "RUNNING":
                                    set_status("RUNNING", "red")

                    except Exception as process_err:
                         logger.error(f"Unexpected error during data comparison or write preparation: {process_err}", exc_info=True)
                         set_status("ERROR: Processing Failed", "red")
                         set_error_message(f"Processing Error: {process_err}")
                # --- End Process DataFrame ---

            else: # Fetch failed (result['success'] was False)
                 error_info = result.get('error', 'Unknown fetch error')
                 failed_api_key = result.get('api_key')
                 logger.error(f"Data fetch failed using API key {censor_api_key(failed_api_key)}. Error: {error_info}")
                 set_status("ERROR: API Fetch", "red")
                 if isinstance(error_info, HttpError):
                     set_error_message(f"API Error: {error_info.resp.status} {error_info.resp.reason}")
                 else:
                     set_error_message(f"Fetch Error: {error_info}")
        # --- End Process Result ---

        # --- Calculate Sleep Time ---
        loop_end_time = time.monotonic()
        elapsed_time = loop_end_time - loop_start_time
        sleep_time = loop_interval - elapsed_time
        if is_running: # Check again in case stop was pressed during processing
            if sleep_time > 0:
                interrupted = stop_event.wait(sleep_time)
                if interrupted:
                    logger.info("Loop sleep interrupted by stop event.")
                    break # Exit loop immediately
            else:
                logger.warning(f"Loop took {elapsed_time:.2f}s, which is longer than the interval of {loop_interval:.2f}s.")
                interrupted = stop_event.wait(0.01)
                if interrupted:
                    logger.info("Loop yield interrupted by stop event.")
                    break # Exit loop immediately
        # --- End Sleep Time ---

    # --- Loop cleanup ---
    logger.info("Data fetch loop stopped.")
    current_active_worker_instance_id = None # Clear active worker ID
    set_status_based_on_inputs() # Set status based on inputs (READY/NOT READY)
    update_ui_element_states() # Update UI elements to reflect stopped state


# --- Sound ---
# initialize_pygame_mixer, play_notification_sound remain unchanged
def initialize_pygame_mixer():
    """Initializes pygame.mixer, handling potential errors."""
    global pygame_mixer_initialized
    if pygame_mixer_initialized: return True
    try:
        pygame.mixer.init(buffer=1024)
        pygame_mixer_initialized = True
        logger.info("Pygame mixer initialized successfully.")
        return True
    except pygame.error as e:
        logger.error(f"Failed to initialize pygame mixer: {e}")
        pygame_mixer_initialized = False
        return False
    except Exception as e:
        logger.error(f"Unexpected error initializing pygame mixer: {e}")
        pygame_mixer_initialized = False
        return False

def play_notification_sound(sound_file, volume_percent):
    """Plays the notification sound using pygame, respecting volume. Runs in a thread."""
    if not pygame_mixer_initialized:
        if not initialize_pygame_mixer():
            logger.error("Cannot play sound, mixer not initialized.")
            return
    if not os.path.exists(sound_file):
        logger.error(f"Sound file not found: {sound_file}")
        # Schedule error message on main thread
        root.after(0, set_error_message, f"SOUND FILE NOT FOUND: {sound_file}")
        return

    def _play():
        try:
            logger.info(f"Attempting to play sound: {sound_file} at {volume_percent}% volume.")
            sound = pygame.mixer.Sound(sound_file)
            volume_float = max(0.0, min(1.0, volume_percent / 100.0))
            sound.set_volume(volume_float)
            sound.play()
        except pygame.error as e:
             logger.error(f"Pygame error playing sound {sound_file}: {e}")
             root.after(0, set_error_message, f"Pygame sound error: {e}")
        except Exception as e:
             logger.error(f"Unexpected error playing sound {sound_file}: {e}")
             root.after(0, set_error_message, f"Sound play error: {e}")

    sound_thread = threading.Thread(target=_play, daemon=True, name="SoundPlayer")
    sound_thread.start()


# --- GUI Functions ---
# set_status, set_error_message, clear_error_message, update_ui_element_states, set_status_based_on_inputs
# toggle_loop, on_api_focus_in, on_api_focus_out, update_volume_label, on_vmix_checkbox_toggle, show_vmix_help
# remain unchanged
def set_status(text, color):
    """Updates the status label, cancelling any pending revert job."""
    global revert_status_job_id
    if revert_status_job_id:
        try:
            root.after_cancel(revert_status_job_id)
            logger.debug(f"Cancelled pending status revert job ID: {revert_status_job_id}")
            revert_status_job_id = None
        except (tk.TclError, ValueError) as e: # ValueError can happen if ID is invalid
            logger.warning(f"Could not cancel status revert job (may already have run or window closing): {e}")
            revert_status_job_id = None

    if root and status_label:
        try:
            current_text = status_label.cget('text')
            current_color = status_label.cget('fg')
            if current_text != text or current_color != color:
                 status_label.config(text=text, fg=color)
                 logger.debug(f"Status label set to: {text} ({color})")
        except tk.TclError as e:
            logger.error(f"Failed to update status label (TclError): {e}")
        except Exception as e:
            logger.error(f"Unexpected error updating status label: {e}", exc_info=True)

def set_error_message(text):
    """Displays an error message at the bottom."""
    if root and error_label:
        try:
            if error_label.cget('text') != text: error_label.config(text=text)
        except tk.TclError as e: logger.error(f"Failed to update error label: {e}")
        except Exception as e: logger.error(f"Unexpected error updating error label: {e}", exc_info=True)

def clear_error_message():
    """Clears the error message."""
    if root and error_label:
        try:
            if error_label.cget('text') != "": error_label.config(text="")
        except tk.TclError as e: logger.error(f"Failed to clear error label: {e}")
        except Exception as e: logger.error(f"Unexpected error clearing error label: {e}", exc_info=True)

def update_ui_element_states(*args):
    """Enables/disables GUI elements based on the running state and input validity."""
    global is_running
    required_filled = False
    vmix_is_currently_enabled = False # For vMix entry state

    try:
        if not is_running:
            required_filled = bool(
                entry_spreadsheet_id.get() and
                entry_worksheet_name.get() and
                entry_csv_filename.get() and
                any(entry.get() for entry in api_key_entries)
            )
        if root and vmix_api_enabled_var:
            vmix_is_currently_enabled = vmix_api_enabled_var.get()

    except tk.TclError: logger.warning("update_ui_element_states checking state during potential GUI shutdown.")
    except NameError: logger.warning("update_ui_element_states checking state before all GUI elements defined.")
    except Exception as e: logger.error(f"Error getting state in update_ui_element_states: {e}")

    if is_running:
        general_state = tk.DISABLED
        button_text = "Stop"
        start_stop_button_state = tk.NORMAL # Always allow stopping
        save_button_state = tk.DISABLED
    else:
        general_state = tk.NORMAL
        button_text = "Start"
        start_stop_button_state = tk.NORMAL if required_filled else tk.DISABLED
        save_button_state = tk.NORMAL

    vmix_entry_state = tk.DISABLED # Default to disabled
    if general_state == tk.NORMAL and vmix_is_currently_enabled:
        vmix_entry_state = tk.NORMAL

    try:
        if root: # Only proceed if root window exists
            if entry_spreadsheet_id and entry_spreadsheet_id.cget('state') != general_state: entry_spreadsheet_id.config(state=general_state)
            if entry_worksheet_name and entry_worksheet_name.cget('state') != general_state: entry_worksheet_name.config(state=general_state)
            if entry_csv_filename and entry_csv_filename.cget('state') != general_state: entry_csv_filename.config(state=general_state)
            if entry_loop_seconds and entry_loop_seconds.cget('state') != general_state: entry_loop_seconds.config(state=general_state)
            if api_key_entries:
                for entry in api_key_entries:
                    if entry:
                        current_entry_state = entry.cget('state')
                        if current_entry_state != general_state: entry.config(state=general_state)
                        if general_state == tk.DISABLED and entry.cget('show') == '': entry.config(show='*')
                        elif general_state == tk.NORMAL and entry.cget('show') == '*': pass # Keep masked if user focused out

            if transpose_checkbox and transpose_checkbox.cget('state') != general_state: transpose_checkbox.config(state=general_state)
            if sound_checkbox and sound_checkbox.cget('state') != general_state: sound_checkbox.config(state=general_state)
            if volume_slider and volume_slider.cget('state') != general_state: volume_slider.config(state=general_state)

            if vmix_api_checkbox and vmix_api_checkbox.cget('state') != general_state: vmix_api_checkbox.config(state=general_state)
            if entry_vmix_header and entry_vmix_header.cget('state') != vmix_entry_state: entry_vmix_header.config(state=vmix_entry_state)
            if vmix_help_button and vmix_help_button.cget('state') != general_state: vmix_help_button.config(state=general_state) # Help always available when not running

            if start_stop_button and (start_stop_button.cget('text') != button_text or start_stop_button.cget('state') != start_stop_button_state):
                start_stop_button.config(text=button_text, state=start_stop_button_state)
            if save_button and save_button.cget('state') != save_button_state:
                save_button.config(state=save_button_state)

    except tk.TclError: logger.warning("update_ui_element_states encountered TclError, likely during shutdown.")
    except NameError: logger.warning("update_ui_element_states called before all GUI elements defined.")
    except Exception as e: logger.error(f"Error applying state in update_ui_element_states: {e}", exc_info=True)

def set_status_based_on_inputs(*args):
    """Sets the status label to READY or NOT READY based on inputs, only if not running."""
    if is_running or revert_status_job_id: return

    required_filled = False
    try:
        if (root and entry_spreadsheet_id and entry_worksheet_name and
            entry_csv_filename and api_key_entries):
            required_filled = bool(
                entry_spreadsheet_id.get() and
                entry_worksheet_name.get() and
                entry_csv_filename.get() and
                any(entry.get() for entry in api_key_entries if entry) # Check entry exists
            )
            if required_filled:
                set_status("READY", "green")
            else:
                set_status("NOT READY", "gray")
        else:
             set_status("NOT READY", "gray") # Default if GUI elements missing

        update_ui_element_states()
    except tk.TclError: logger.warning("set_status_based_on_inputs called while GUI is potentially shutting down.")
    except NameError: logger.warning("set_status_based_on_inputs called before all GUI elements defined.")
    except Exception as e: logger.error(f"Error in set_status_based_on_inputs: {e}", exc_info=True)

def toggle_loop():
    """Starts or stops the data fetching loop."""
    global is_running, loop_thread, force_write_on_next_pull, last_vmix_api_id
    global skip_next_vmix_execution_on_change # <<< ADDED GLOBAL

    if is_running:
        logger.info("Stop button pressed.")
        is_running = False
        skip_next_vmix_execution_on_change = False # <<< RESET FLAG ON STOP
        stop_event.set() # Signal the loop and any waiting threads to stop
        if start_stop_button:
            try:
                start_stop_button.config(text="Stopping...", state=tk.DISABLED)
            except tk.TclError: pass # Ignore if widget destroyed
    else:
        required_filled = False
        try:
             if (root and entry_spreadsheet_id and entry_worksheet_name and
                entry_csv_filename and api_key_entries):
                required_filled = bool(
                    entry_spreadsheet_id.get() and
                    entry_worksheet_name.get() and
                    entry_csv_filename.get() and
                    any(entry.get() for entry in api_key_entries if entry)
                )
        except tk.TclError: pass # Ignore if GUI closing

        if not required_filled:
            logger.warning("Start pressed but requirements not met.")
            messagebox.showwarning("Not Ready", "Please fill in Spreadsheet ID, Worksheet Name, Output CSV Filename, and at least one API Key.")
            return

        if sound_var.get() and not pygame_mixer_initialized:
            if not initialize_pygame_mixer():
                 messagebox.showwarning("Audio Warning", "Failed to initialize audio playback.\nSound notifications will not work, but proceeding anyway.")

        logger.info("Start button pressed.")
        is_running = True
        stop_event.clear() # Clear the stop signal for the new run
        clear_error_message() # Clear any previous errors
        set_status("RUNNING", "red")
        if vmix_status_label:
            try:
                vmix_status_label.config(text="No vMix API Response yet", fg="gray")
            except tk.TclError: pass # Ignore if GUI closing

        force_write_on_next_pull = True # Set flag for initial write
        skip_next_vmix_execution_on_change = True # <<< SET FLAG ON START
        # Reset last vMix ID on start to ensure the first read value is treated as 'new'
        # but execution will be skipped by the flag above.
        last_vmix_api_id = None
        logger.info("Flags set to force write and skip first vMix execution on change after start. vMix API ID tracker reset.")

        update_ui_element_states() # Disable inputs immediately
        loop_thread = threading.Thread(target=run_loop, daemon=True, name="MainLoopThread")
        loop_thread.start()

def on_api_focus_in(event):
    """Show API key content on focus if not running."""
    if not is_running:
        try:
            widget = event.widget
            if widget and widget.cget('show') == '*':
                widget.config(show='')
        except tk.TclError: pass # Ignore if widget is destroyed

def on_api_focus_out(event):
    """Mask API key content on focus out if not running and not empty."""
    try:
        widget = event.widget
        if widget:
            if widget.get() and not is_running:
                widget.config(show='*')
    except tk.TclError: pass # Ignore if widget is destroyed

def update_volume_label(*args):
    """Updates the volume percentage label."""
    if root and volume_label:
        try:
            volume_percent = int(volume_var.get())
            volume_label.config(text=f"{volume_percent}%")
        except (ValueError, tk.TclError):
             if volume_label: volume_label.config(text="--%") # Handle errors or initial state

def on_vmix_checkbox_toggle():
    """Handles the vMix API checkbox state change by updating UI element states."""
    update_ui_element_states()

def show_vmix_help():
    """Displays help information for the vMix API feature."""
    help_text = (
        "To execute vMix API commands from the spreadsheet:\n\n"
        "1. Create a column in your Google Sheet.\n\n"
        "2. Set the header (first row) of this column to exactly match the text entered in the 'vMix API command header' field below the checkbox (e.g., `vMix_API_Command`).\n\n"
        "3. In the cells of that column (starting from the second row), use a formula to generate the required format:\n"
        "   `<random_number>,<vmix_api_call>`\n\n"
        "   Example Google Apps Script for a random number (put in Tools > Script editor):\n"
        "   ```javascript\n"
        "   /** @OnlyCurrentDoc */\n"
        "   function generateRandomId() {\n"
        "     // Generate ~18 digit random number as string\n"
        "     return Math.random().toString().substring(2, 10) + Math.random().toString().substring(2, 12);\n"
        "   }\n"
        "   ```\n\n"
        "   Then in the sheet cell (e.g., in cell C2 if your header is C1):\n"
        "   `=generateRandomId() & \",\" & \"http://localhost:8088/api/?function=AdjustCountdown&Input=Preview\"`\n\n"
        "   (Replace the URL part with your actual vMix API command.)\n\n"
        "4. Check the 'vMix API command header' box and ensure the header name matches your sheet.\n\n"
        "5. The script writes the sheet data (potentially transposed) to the local CSV file.\n\n"
        "6. It then reads that CSV file, looks for your specified header in the *first row*, reads the `<id>,<url>` value from the *second row* in that same column, and executes the command if the ID has changed."
    )
    messagebox.showinfo("vMix API Help", help_text)

# --- GUI Setup ---
# GUI layout remains the same
root = tk.Tk()
root.title("Google Sheet Exporter")
root.geometry("400x820")
root.minsize(400, 750) # Set a minimum size

try:
    large_font = font.Font(family="Helvetica", size=12)
    label_font = font.Font(family="Helvetica", size=10)
    status_font = font.Font(family="Helvetica", size=14, weight="bold")
    error_font = font.Font(family="Helvetica", size=11, weight="bold")
    vmix_status_font = font.Font(family="Helvetica", size=10) # Font for vMix status
except tk.TclError: # Fallback if font creation fails (e.g., headless environment)
    large_font = None
    label_font = None
    status_font = None
    error_font = None
    vmix_status_font = None
    logger.error("Failed to create fonts.")

input_frame = ttk.Frame(root, padding="10 10 10 10")
input_frame.pack(fill=tk.X)
input_frame.columnconfigure(0, weight=1) # Make column expandable

ttk.Label(input_frame, text="Spreadsheet ID:", font=label_font).grid(row=0, column=0, columnspan=2, sticky=tk.W, pady=2)
entry_spreadsheet_id = ttk.Entry(input_frame, width=40, font=large_font)
entry_spreadsheet_id.grid(row=1, column=0, columnspan=2, sticky=tk.EW, pady=(0, 10))
entry_spreadsheet_id.bind("<KeyRelease>", set_status_based_on_inputs)

ttk.Label(input_frame, text="Worksheet Name:", font=label_font).grid(row=2, column=0, columnspan=2, sticky=tk.W, pady=2)
entry_worksheet_name = ttk.Entry(input_frame, width=40, font=large_font)
entry_worksheet_name.grid(row=3, column=0, columnspan=2, sticky=tk.EW, pady=(0, 10))
entry_worksheet_name.bind("<KeyRelease>", set_status_based_on_inputs)

ttk.Label(input_frame, text="API Keys (fill top-down):", font=label_font).grid(row=4, column=0, columnspan=2, sticky=tk.W, pady=2)
api_key_entries = []
for i in range(5):
    entry = ttk.Entry(input_frame, width=40, font=large_font, show='*')
    entry.grid(row=5+i, column=0, columnspan=2, sticky=tk.EW, pady=1)
    entry.bind("<FocusIn>", on_api_focus_in)
    entry.bind("<FocusOut>", on_api_focus_out)
    entry.bind("<KeyRelease>", set_status_based_on_inputs)
    api_key_entries.append(entry)

ttk.Label(input_frame, text="Loop Interval (seconds):", font=label_font).grid(row=10, column=0, columnspan=2, sticky=tk.W, pady=(10, 2))
entry_loop_seconds = ttk.Entry(input_frame, width=10, font=large_font)
entry_loop_seconds.grid(row=11, column=0, columnspan=2, sticky=tk.W, pady=(0, 10))

ttk.Label(input_frame, text="Output CSV Filename:", font=label_font).grid(row=12, column=0, columnspan=2, sticky=tk.W, pady=2)
entry_csv_filename = ttk.Entry(input_frame, width=40, font=large_font)
entry_csv_filename.grid(row=13, column=0, columnspan=2, sticky=tk.EW, pady=(0, 10))
entry_csv_filename.bind("<KeyRelease>", set_status_based_on_inputs)

options_frame = ttk.Frame(root, padding="10 0 10 10")
options_frame.pack(fill=tk.X)
options_frame.columnconfigure(1, weight=1) # Allow slider to expand

transpose_var = tk.BooleanVar()
transpose_checkbox = ttk.Checkbutton(options_frame, text="Transpose Data", variable=transpose_var, onvalue=True, offvalue=False)
transpose_checkbox.grid(row=0, column=0, columnspan=3, sticky=tk.W, pady=(0,5))

sound_var = tk.BooleanVar()
sound_checkbox = ttk.Checkbutton(options_frame, text="Play Sound on Data Change", variable=sound_var, onvalue=True, offvalue=False)
sound_checkbox.grid(row=1, column=0, columnspan=3, sticky=tk.W)

ttk.Label(options_frame, text="Sound Volume:", font=label_font).grid(row=2, column=0, sticky=tk.W, pady=(5,0))
volume_var = tk.DoubleVar()
volume_slider = ttk.Scale(options_frame, from_=0, to=100, orient=tk.HORIZONTAL, variable=volume_var, command=update_volume_label)
volume_slider.grid(row=2, column=1, sticky=tk.EW, padx=(5, 5), pady=(5,0))
volume_label = ttk.Label(options_frame, text="100%", width=5, font=label_font)
volume_label.grid(row=2, column=2, sticky=tk.W, pady=(5,0))

vmix_frame = ttk.Frame(root, padding="10 5 10 10")
vmix_frame.pack(fill=tk.X)
vmix_frame.columnconfigure(0, weight=1)

vmix_api_enabled_var = tk.BooleanVar()
vmix_api_checkbox = ttk.Checkbutton(vmix_frame, text="vMix API command header", variable=vmix_api_enabled_var,
                                     onvalue=True, offvalue=False, command=on_vmix_checkbox_toggle)
vmix_api_checkbox.grid(row=0, column=0, columnspan=2, sticky=tk.W, pady=(0, 2))

entry_vmix_header = ttk.Entry(vmix_frame, width=40, font=large_font)
entry_vmix_header.grid(row=1, column=0, columnspan=2, sticky=tk.EW, pady=(0, 5))

vmix_status_label = tk.Label(vmix_frame, text="No vMix API Response yet", font=vmix_status_font, fg="gray", anchor=tk.W, justify=tk.LEFT)
vmix_status_label.grid(row=2, column=0, columnspan=2, sticky=tk.EW, pady=(0, 10))

vmix_help_button = ttk.Button(vmix_frame, text="vMix API Help", command=show_vmix_help, width=15)
vmix_help_button.grid(row=3, column=0, columnspan=2, sticky=tk.W, pady=(5,0))

button_frame = ttk.Frame(root, padding="10 10 10 10")
button_frame.pack(fill=tk.X)

start_stop_button = ttk.Button(button_frame, text="Start", command=toggle_loop, width=15)
start_stop_button.pack(pady=5)

save_button = ttk.Button(button_frame, text="Save Config", command=save_config, width=15)
save_button.pack(pady=5)

status_label = tk.Label(root, text="NOT READY", font=status_font, fg="gray", anchor=tk.CENTER)
status_label.pack(fill=tk.X, pady=(10, 5)) # Main status

error_label = tk.Label(root, text="", font=error_font, fg="red", anchor=tk.CENTER)
error_label.pack(fill=tk.X, side=tk.BOTTOM, pady=(5, 10)) # Error message at the very bottom


# --- Initialization ---
# initialize_app remains unchanged
def initialize_app():
    """Loads config and populates the GUI."""
    global config # Ensure we're using the global config object

    load_config() # Loads or creates config, applies defaults
    initialize_pygame_mixer() # Attempt mixer init early

    try:
        entry_spreadsheet_id.insert(0, config.get('Settings', 'spreadsheet_id'))
        entry_worksheet_name.insert(0, config.get('Settings', 'worksheet_name'))
        for i, entry in enumerate(api_key_entries, 1):
            key = config.get('Settings', f'api_key_{i}')
            entry.insert(0, key)
            if key: entry.config(show='*')
            else: entry.config(show='')

        try:
            loop_sec_str = config.get('Settings', 'loop_seconds')
            loop_sec = float(loop_sec_str)
            if loop_sec <= 0: loop_sec = DEFAULT_LOOP_SECONDS
        except (ValueError, configparser.NoOptionError, configparser.NoSectionError):
            logger.warning(f"Invalid or missing loop_seconds in config, using default {DEFAULT_LOOP_SECONDS}.")
            loop_sec = DEFAULT_LOOP_SECONDS
        entry_loop_seconds.insert(0, str(loop_sec))

        entry_csv_filename.insert(0, config.get('Settings', 'output_csv_filename'))
        transpose_var.set(config.getboolean('Settings', 'transpose_data'))
        sound_var.set(config.getboolean('Settings', 'play_sound_on_change'))

        try:
            vol_str = config.get('Settings', 'sound_volume')
            vol = int(vol_str)
            vol = max(0, min(100, vol)) # Clamp between 0 and 100
        except (ValueError, configparser.NoOptionError, configparser.NoSectionError):
            logger.warning(f"Invalid or missing sound_volume in config, using default {DEFAULT_SOUND_VOLUME}.")
            vol = DEFAULT_SOUND_VOLUME
        volume_var.set(float(vol))
        update_volume_label() # Update label based on initial value

        vmix_api_enabled_var.set(config.getboolean('Settings', 'vmix_api_enabled'))
        entry_vmix_header.insert(0, config.get('Settings', 'vmix_api_header'))

        set_status_based_on_inputs() # Sets READY/NOT READY and calls update_ui_element_states

    except tk.TclError as e:
         logger.error(f"Error initializing GUI elements (TclError): {e}")
    except Exception as e:
        logger.critical(f"Fatal error during application initialization: {e}", exc_info=True)
        try: # Try to show error before exiting
            messagebox.showerror("Initialization Error", f"Failed to initialize application:\n{e}\n\nCheck log for details.")
        except tk.TclError: pass # Ignore if GUI failed even for messagebox

# --- Application Exit Handling ---
# on_closing remains unchanged
def on_closing():
    """Handles window close event."""
    global is_running, revert_status_job_id
    logger.info("Window close requested.")
    if revert_status_job_id:
        try:
            root.after_cancel(revert_status_job_id)
            logger.debug(f"Cancelled pending status revert job {revert_status_job_id} during closing.")
            revert_status_job_id = None
        except (tk.TclError, ValueError): pass # Ignore errors on cancel during shutdown

    if is_running:
        logger.info("Signaling loop and threads to stop...")
        is_running = False # Prevent new loop iterations
        stop_event.set() # Signal waiting threads/loop sleep
        if loop_thread and loop_thread.is_alive():
            logger.info("Waiting briefly for main loop thread to join...")
            loop_thread.join(timeout=0.5) # Give loop thread a moment to exit cleanly

    if pygame_mixer_initialized:
        try:
            pygame.mixer.stop()
            pygame.mixer.quit()
            logger.info("Pygame mixer stopped and quit.")
        except Exception as pg_quit_err: logger.error(f"Error during pygame mixer quit: {pg_quit_err}")

    logger.info("Destroying root window.")
    try:
        root.destroy()
    except tk.TclError as e:
        logger.error(f"Error destroying root window (may already be destroyed): {e}")
    logger.info("Application shutdown sequence finished.")


# --- Run Application ---
if __name__ == "__main__":
    app_exit_code = 0
    try:
        initialize_app()
        root.protocol("WM_DELETE_WINDOW", on_closing) # Set custom close behavior
        root.mainloop()
    except tk.TclError as e:
        if "invalid command name" not in str(e):
             logger.critical(f"Unhandled TclError in main execution scope: {e}", exc_info=True)
             app_exit_code = 1
        else:
            logger.warning(f"Ignoring TclError likely related to widget destruction during exit: {e}")
    except Exception as main_err:
         logger.critical(f"Unhandled exception in main execution scope: {main_err}", exc_info=True)
         app_exit_code = 1
         try:
             if root and root.winfo_exists():
                 messagebox.showerror("Fatal Error", f"A critical error occurred:\n{main_err}\n\nCheck log.txt for details.")
         except tk.TclError:
             print(f"FATAL ERROR (GUI unavailable): {main_err}") # Fallback to console
    finally:
        logger.info("Application exited.")
        # import sys
        # sys.exit(app_exit_code)
