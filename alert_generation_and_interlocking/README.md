# Global Helper Variables: 




# Helper Functions: 

## Function: check_PLC_MQTT_DB_connection()

### Description:
This function checks the connections between various components such as PLCs, MQTT brokers, and databases for multiple press machines. It determines the status of connections and logs any errors encountered during the process.

### Parameters:
This function doesn't take any explicit parameters.  

### Global Variables Used:
- `Broker_connections`: A dictionary to store the status of connections between the MQTT broker and PLCs.
- `Db_connections`: A dictionary to store the status of connections between databases and PLCs.
- `Overall_connections`: A dictionary to track overall connection status for each press machine.

### Dependencies:
- `subprocess`: Used to execute system commands.
- `time`: For time-related operations like sleeping.
- `datetime`: For handling date and time.
- `platform`: To identify the operating system.
- `myclient_global`: A global MongoDB client object.
- Other global MQTT client objects (e.g., `mqttc_24`, `mqttc_21_and_05`, `mqttc_11`) and associated callback functions (`on_publish`, `on_connect_24`, `on_connect_21_and_05`, `on_connect_11`, `on_message`).
- `presses_and_IP` : A dictionary that parses through a excel sheet and stores a press number as a key with the IP as its key value pair

### Behavior:
- Iterates through a set of presses and their associated IP addresses.
- Conducts a ping check to validate the PLC to broker connection status.
- Checks the database connection by querying the most recent data entry within a specific time frame.
- Logs any connection errors encountered during the process.
- Initiates MQTT client connections based on press number and connection status.
- Logs critical network or database failures if encountered.

### Exceptions:
- The function handles exceptions by logging critical errors and marking all connections as unsuccessful for the respective press machines.

### Notes:
- The function makes use of global variables for storing connection statuses and requires external configuration of MQTT clients.
- The status of connections is recorded in respective dictionaries (`Broker_connections`, `Db_connections`, `Overall_connections`).
- Error handling is performed for connection establishment with MQTT clients, with appropriate logging if connections fail.


## Function: get_job(press)

### Description:
This function retrieves the most recent job ID associated with a specific press machine from a database. It specifically handles the case for press machine '24' and returns the recent job ID if available, otherwise, it returns a message indicating that no job has been inputted. For other presses, it returns `None`.

### Parameters:
- `press` (str): Represents the identifier for the press machine.

### Global Variables Used:
- `myclient_global`: A global MongoDB client object.

### Behavior:
- Checks if the `press` parameter matches the identifier for press machine '24'.
    - Once job input functionality is added for all the other presses it will scale the same behavior for all presses, not just press 24 
- If the `press` is '24':
  - Retrieves the MongoDB client object associated with the specific press.
  - Queries the most recent document from the 'BATCH_1' collection for that press.
  - Extracts the job ID from the retrieved document.
  - Returns the recent job ID if available, else returns "No Job Inputted".
- For other press identifiers:
  - Returns `None`.

### Returns:
- If `press` is '24' and a recent job ID exists: Returns the most recent job ID.
- If `press` is '24' and no recent job ID exists: Returns "No Job Inputted".
- For other presses: Returns `None`.

### Notes:
- This function assumes a specific structure and naming convention for the MongoDB collections ('BATCH_1') and job ID field ('job_id').
- Global MongoDB client object (`myclient_global`) is utilized to establish connections with the database.
- It retrieves the most recent job ID associated with press '24' from the specified collection.
- If there's no recent job ID for press '24', it returns a predefined message.
- For any other press, it returns `None`.



## Function: check_if_alert_duplicate(press)

### Description:
This function checks for duplicate alerts for a specific press in a database collection. It compares the timestamp of the most recent alert document for the given press (`Press_{press}`) with the start time of the current alert (`start_of_the_alert`). If the timestamps match, it indicates a duplicate alert; otherwise, it concludes that there is no duplicate alert.

### Parameters:
- `press` (str): Represents the identifier for the press machine.

### Global Variables Used:
- `alert_collection`: Presumed to be a global MongoDB collection object.

### Behavior:
- Attempts to retrieve the most recent document for the specified press from the `alert_collection`.
- Handles the edge case of no previous documents by returning `0`.
- Compares the timestamp (`_id`) of the most recent document with the `start_of_the_alert` timestamp.
- If the timestamps match, it indicates a duplicate alert and returns `1`.
- Otherwise, it concludes that there is no duplicate alert and returns `0`.

### Returns:
- `1` if a duplicate alert is found.
- `0` if no duplicate alert is detected or in the case of an edge scenario where no previous document exists.

### Notes:
- Assumes that the database collection (`alert_collection`) stores documents with timestamps (`_id`) representing the alert creation time.
- The function handles cases where no previous document exists for the specified press by returning `0`.
- It compares the timestamp of the most recent document with the timestamp of the current alert to determine duplicity.
- Utilizes global MongoDB collection object (`alert_collection`) to query the database for alert documents related to the specified press.


## Function: PLC_warning_signal()

### Description:
This function evaluates alert codes associated with various PLC sensors to determine the severity level of the alerts. It assesses different types of alerts such as "Temperature change," "Number of peaks," and "Moving average difference" to derive an overall error value.

### Behavior:
- Checks for the presence of an `alert_code`. If an `alert_code` is present:
  - Evaluates "Temperature change" alerts for each sensor, categorizing them as critical alerts (`critical_alert_code`) if the temperature change exceeds a certain threshold (10 units). Otherwise, it assigns a warning alert (`warning_code`).
  - Examines "Number of peaks" alerts and considers them as warning alerts if they exist.
  - Analyzes "Moving average difference" alerts for each sensor, categorizing them as critical alerts if the percentage change exceeds 50%. Otherwise, it assigns a warning alert.
- Calculates an overall `error_val` based on the severity of the detected alerts.

### Returns:
- The `error_val` representing the severity level of the detected alerts:
  - If critical alerts are detected, returns `critical_alert_code`.
  - If only warning alerts are detected, returns `warning_code`.
  - If no alerts are detected, returns `0`.

### Notes:
- The function assumes the existence of an `alert_code` dictionary containing different types of alerts related to PLC sensors.
- The determination of critical alerts depends on specific threshold values (e.g., temperature change threshold of 10 units, moving average difference threshold of 50%).
- It calculates an overall error value based on the severity of the detected alerts.
- The function doesn't directly modify the global `alert_code` variable.



## Function: send_alerts_to_DB_and_PLC()

### Description:
This function manages the handling and distribution of alerts to both a database and Programmable Logic Controllers (PLCs). It checks for the presence of alerts within the `alert_code` dictionary and proceeds to store these alerts in the database. Additionally, it publishes alert messages to the respective PLCs based on the alerts detected.

### Behavior:
- Attempts to save alerts in a database if there are alerts present in the `alert_code` dictionary.
- Uses the global `alert_collection` object for database interactions.
- Updates the alerts with a timestamp (`_id`) before storing them in the database to prevent duplicate key errors.
- Handles scenarios of no alerts by publishing corresponding messages to PLCs indicating "No Alerts/Warnings" for the chosen press machines (`'05'`, `'11'`, `'21'`, `'24'`).
- Publishes alert messages to the appropriate PLCs based on the detected alerts by invoking the `PLC_warning_signal()` function (commented out code sections indicate an area for adjustment for new thresholds or requirements).
- Displays a message indicating the successful addition of alerts for the hour if alerts are present and processed.

### Returns:
- This function does not explicitly return any value.

### Notes:
- Assumes the existence of the `alert_code` dictionary containing alerts.
- Utilizes global variables such as `alert_collection`, `mqttc_21_and_05`, `mqttc_11`, `mqttc_24`, etc., for database access and PLC communication.
- Handles cases where no alerts are present by sending appropriate messages to the respective PLCs indicating no alerts.
- Incorporates commented out sections for updating or modifying alerts, deleting previous alerts, or publishing alerts to PLCs, potentially based on specific conditions or thresholds.

# Running of the file

## Description: 
This section of the code (after send_alerts_to_DB_and_PLC() function) runs an infinite loop that starts the threads to connect to the broker and and generates the alerts to send it.

## Behavior:
- iniates various time variables that are used within other functions
- runs `check_PLC_MQTT_DB_connection()`
- within a for loop that iterates through the `Overall_connections` dictionary (which contains press numbers with working connections) 
 - this loop then runs `check_if_alert_duplicate(press)`, `get_result(press_chosen)`, `send_alerts_to_DB_and_PLC()` 