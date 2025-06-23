import os
from databricks import sql
from databricks.sdk.core import Config
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import serving
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import requests
import json

# --- Databricks Configuration and Authentication ---
# Initialize Databricks WorkspaceClient and Config for SQL Warehouse connection
w = WorkspaceClient()
cfg = Config()

# Retrieve user access token from Streamlit context headers
# This is crucial for authenticating with Databricks SQL Warehouse
user_token = st.context.headers.get('X-Forwarded-Access-Token')
if not user_token:
    st.error("User access token not found. This app must be run within the Databricks environment.")
    st.stop() # Stop the app if no user token is available

# Initialize session state for API response
# This ensures the API response persists across Streamlit reruns
if 'api_response' not in st.session_state:
    st.session_state.api_response = None

# --- Database Query Functions ---
@st.cache_data(ttl=600) # Cache data for 10 minutes (600 seconds) to improve performance
def sql_query_with_user_token(query: str, token: str) -> pd.DataFrame:
    """
    Executes a SQL query against the Databricks SQL Warehouse using the provided user token.
    Data is cached to prevent re-fetching on every rerun if inputs are the same.
    """
    try:
        # Establish connection to Databricks SQL Warehouse
        with sql.connect(
            server_hostname=cfg.host,
            http_path=f"/sql/1.0/warehouses/{cfg.warehouse_id}",
            access_token=token
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query) # Execute the SQL query
                # Convert to Pandas DataFrame. Assuming column names are preserved from SQL query.
                return cursor.fetchall_arrow().to_pandas()
    except Exception as e:
        # Display an error message if connection or query execution fails
        st.error(f"Error connecting to Databricks SQL Warehouse or executing query: {e}")
        st.stop() # Stop the app to prevent further errors
        return pd.DataFrame() # Return an empty DataFrame on error

@st.cache_data(ttl=3600) # Cache distinct dates for 1 hour
def load_dates_for_selection() -> pd.DataFrame:
    """
    Loads distinct dates from the 'gold_iop_features_version_demo' table for the date selection box.
    """
    query = f"SELECT DISTINCT CAST(date AS DATE) AS date_col FROM mining_iron_ore_processing_demo_catalog.iop_schema.gold_iop_features_version_demo ORDER BY date_col LIMIT 5000"
    # Call the cached SQL query function with the user token
    df_dates = sql_query_with_user_token(query, token=user_token)
    return df_dates

def load_data_for_app(selected_date: datetime.date) -> pd.DataFrame:
    """
    Loads data from the 'gold_iop_features_version_demo' table for the given date.
    The 'date' column in the database is assumed to contain timestamps.
    """
    # Construct the SQL query. Note: We are still filtering by date, but the SELECT *
    # is assumed to bring back the full timestamp in the 'date' column.
    query = f"SELECT * EXCEPT(`_rescued_data`, `Percent_iron_concentrate`, `percent_silica_concentrate`) FROM mining_iron_ore_processing_demo_catalog.iop_schema.gold_iop_features_version_demo WHERE CAST(date AS DATE) = '{selected_date.strftime('%Y-%m-%d')}' LIMIT 5000"
    # Call the cached SQL query function with the user token
    df = sql_query_with_user_token(query, token=user_token)
    return df

def send_to_databricks_serving_endpoint(record_data: dict):
    """
    Sends a single record (dictionary) to Databricks model serving endpoints
    and returns their combined responses.
    """
    # Convert the single record dictionary into a list containing one dictionary
    # which is the expected format for dataframe_records in the query method.
    df_single_row = pd.DataFrame([record_data])
    input_payload_records = df_single_row.to_dict('records')
    
    try:
        # Query the first model serving endpoint
        fe_response = w.serving_endpoints.query(
            name="iop_schema-fe_model-serving-endpoint",
            dataframe_records=input_payload_records
        )
        
        # Query the second model serving endpoint
        si_response = w.serving_endpoints.query(
            name="iop_schema-si_model-serving-endpoint",
            dataframe_records=input_payload_records
        )
        
        # Combine predictions from both endpoints
        response = {
            "Fe": fe_response.predictions[0] if fe_response.predictions else None,
            "Si": si_response.predictions[0] if si_response.predictions else None
        }
        return response
    except Exception as e:
        st.error(f"Error querying model serving endpoint: {e}")
        return None

# --- Streamlit UI Configuration ---
# Set general page layout and title
st.set_page_config(layout="wide", page_title="Fe Concentrator Simulator") # Changed layout to "wide"
st.title("Fe Concentrator Plant Simulator")
st.markdown("Select a Date and an Hour to pre fill our simulated plant set points from Historical Data.")

# --- Date and Hour Selection (Moved to Sidebar) ---
with st.sidebar:
    st.subheader("Select Data Filters")

    # Load distinct dates from the database with a spinner
    dates_df = pd.DataFrame()
    with st.spinner("Loading available dates..."):
        dates_df = load_dates_for_selection()

    fixed_dates = []
    if not dates_df.empty and 'date_col' in dates_df.columns:
        # Ensure the column is datetime type, then extract dates
        fixed_dates = sorted([d.date() for d in pd.to_datetime(dates_df['date_col']).unique()])
    else:
        st.error("Could not load distinct dates from the database. Please check the query and table schema.")

    # Prepare the options for the date selectbox, including a placeholder
    date_options = ["Select a Date..."] + [d.strftime('%Y-%m-%d') for d in fixed_dates]

    # Create the selectbox for date selection
    selected_date_str = st.selectbox(
        "Select a Date:",
        date_options,
        key="date_selector", # Unique key for this widget
        index=0 # Default to the placeholder "Select a Date..."
    )

    # Initialize selected_date and daily_data
    selected_date = None
    daily_data = pd.DataFrame() # Initialize an empty DataFrame for daily data

    # Only convert the string to a date object if a valid date (not the placeholder) is selected
    if selected_date_str != "Select a Date...":
        selected_date = datetime.strptime(selected_date_str, '%Y-%m-%d').date()

        # --- Load Data Based on Selected Date ---
        # Display a spinner (loading indicator) while the data is being fetched
        with st.spinner(f"Loading data for {selected_date_str}..."):
            daily_data = load_data_for_app(selected_date)

        # Check if data was loaded successfully for the selected date
        if daily_data.empty:
            st.warning(f"No data found for {selected_date_str}. Please select another date.")
            # Reset selected_hour and prevent further UI elements from rendering
            selected_hour = None # Ensure selected_hour is None if no data is found
        else:
            # --- Hour Selection Dropdown (Conditional) ---
            # Extract unique hours from the 'date' timestamp column for the hour selector
            if 'date' in daily_data.columns:
                # Ensure the 'date' column is in datetime format for hour extraction
                try:
                    daily_data['date'] = pd.to_datetime(daily_data['date'])
                    # Extract unique hours and format them as 'HH:00'
                    available_hours = sorted(daily_data['date'].dt.strftime('%H:00').unique().tolist())
                except Exception as e:
                    st.error(f"Error converting 'date' column to datetime or extracting hours: {e}")
                    available_hours = []
            else:
                st.error("The loaded data does not contain a 'date' column required for hour extraction. Please check your database schema.")
                available_hours = [] # No hours available if column is missing

            # Prepare the options for the hour selectbox, including a placeholder
            hour_options = ["Select an Hour..."] + available_hours

            # Create the selectbox for hour selection
            selected_hour_str = st.selectbox(
                "Select an Hour:",
                hour_options,
                key="hour_selector", # Unique key for this widget
                index=0 # Default to the placeholder "Select an Hour..."
            )
            # Initialize selected_hour to None. It will be set if a valid hour is chosen.
            selected_hour = None
            # Only use the selected hour if it's not the placeholder
            if selected_hour_str != "Select an Hour...":
                selected_hour = selected_hour_str
    else:
        st.info("Please select a date from above.")

# --- Main Content Area for Data Editing Form and API Results ---
# Condition to show the form and API results: Both date and hour must be selected, and daily_data must not be empty.
if (selected_date_str != "Select a Date..." and
    selected_hour_str != "Select an Hour..." and
    selected_hour is not None and
    not daily_data.empty):

    # Create two main columns for the form and the API results side-by-side
    form_col, results_col = st.columns([2, 1]) # Adjust ratio as needed (e.g., 2:1 for form to results)

    with form_col:
        # Filter the daily data further to get the single row for the selected hour.
        # Filter based on the hour extracted from the 'date' column
        final_row_df = daily_data[
            (daily_data['date'].dt.strftime('%H:00') == selected_hour)
        ].head(1)

        # Check if a row was actually found
        if not final_row_df.empty:
            target_row = final_row_df.iloc[0] # Get the first (and only expected) row

            st.markdown("---") # Visual separator
            st.subheader(f"Edit Details for Event on {selected_date_str} at {selected_hour}")

            # Create a Streamlit form for editing the data.
            # Inputs inside a form only update their values when the form's submit button is clicked.
            with st.form("event_edit_form"):
                # Create three columns for the form elements within the form_col
                col1, col2, col3 = st.columns(3)
                
                # Dictionary to store the dynamically created form input values
                form_input_values = {}
                
                # Keep track of the current column index
                col_idx = 0

                # Iterate through the columns of the target row to create dynamic inputs
                # Exclude 'date' as it's used for selection/filtering and might be a complex object.
                columns_to_exclude = ['date']
                for col_name, col_value in target_row.items():
                    if col_name not in columns_to_exclude:
                        # Determine which column to place the input in
                        current_col = [col1, col2, col3][col_idx % 3]
                        
                        display_name = f"{col_name.replace('_', ' ').title()}:"
                        unique_key = f"form_input_{col_name}"

                        with current_col: # Place the widget in the current column
                            if col_name == 'description':
                                # Use text_area for description, as it's typically longer text
                                form_input_values[col_name] = st.text_area(
                                    display_name,
                                    value=str(col_value), # Ensure value is string
                                    key=unique_key
                                )
                            elif isinstance(col_value, (int, float)):
                                # Use number_input for integers and floats
                                form_input_values[col_name] = st.number_input(
                                    display_name,
                                    value=float(col_value), # Use float for number_input
                                    step=1 if isinstance(col_value, int) else None, # Step 1 for integers
                                    key=unique_key
                                )
                            else:
                                # Default to text_input for all other types (strings, etc.)
                                form_input_values[col_name] = st.text_input(
                                    display_name,
                                    value=str(col_value), # Ensure value is string
                                    key=unique_key
                                )
                        col_idx += 1 # Move to the next column for the next input

                # The submit button should typically be outside the columns or in its own row/column for clarity
                st.markdown("---") # Add a separator before the submit button
                submit_button = st.form_submit_button("Simulate Plant Output")

                # If the submit button is clicked
                if submit_button:
                    # Prepare the payload for the Databricks serving endpoint
                    # This should match the expected input schema of your models
                    api_payload = {}
                    # Add all the dynamically collected form values
                    api_payload.update(form_input_values)

                    # Call the Databricks serving endpoint with a spinner
                    with st.spinner("Sending data to Databricks Serving Endpoints..."):
                        api_response = send_to_databricks_serving_endpoint(api_payload)
                    
                    # Store the response in session state
                    st.session_state.api_response = api_response
                    
                    if not api_response:
                        st.error("Failed to get a response from the Databricks Serving Endpoints.")

        else:
            # Message if no data found for the selected combination
            st.warning("No specific event details found for the selected date and hour combination. Please ensure your data has a unique entry for each time slot if you expect a single row.")
    
    with results_col:
        # --- Display API Results in the second main column ---
        if st.session_state.api_response:
            st.markdown("---")
            st.subheader("Simulated Plant Results")
            api_response = st.session_state.api_response
            fe_val = api_response.get("Fe")
            si_val = api_response.get("Si")
            
            st.metric(label="Fe Prediction", value=f"{fe_val:.2f}" if isinstance(fe_val, (int, float)) else "N/A")
            st.metric(label="Si Prediction", value=f"{si_val:.2f}" if isinstance(si_val, (int, float)) else "N/A")
            
            st.info("These are the latest simulation results from the Databricks Serving Endpoints.")
        else:
            st.markdown("---")
            st.subheader("Simulated Plant Results")
            st.info("Submit the form to see simulation results here.")

# --- Initial Messages for User Guidance ---
# Only show these if date/hour haven't been fully selected to guide the user
else:
    if selected_date_str == "Select a Date...":
        st.info("Please select a **Date** from the **sidebar** to proceed and enable the form.")
    elif selected_hour_str == "Select an Hour...":
        st.info("Please select an **Hour** from the **sidebar** to view and edit event details.")
    elif daily_data.empty and selected_date_str != "Select a Date...":
        st.info(f"No data available for the selected date ({selected_date_str}). Please choose a different date from the sidebar.")
    else:
        # Fallback for any other unhandled states
        st.info("Waiting for data selection to enable the form. Please make your selections in the sidebar.")

