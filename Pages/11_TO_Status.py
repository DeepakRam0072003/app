import pyodbc
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta

# Streamlit page configuration
st.set_page_config(
    page_title="TO Status Report",
    page_icon="🚛",
    layout="wide"
)

# Title
st.title("TO Status Report")

# Date range selection (default to last 3 months)
end_date = datetime.now()
start_date = end_date - timedelta(days=90)

# Create columns for layout
col1, col2 = st.columns(2)

with col1:
    from_date = st.date_input("From Date", value=start_date)
    refresh_button = st.button("Refresh Data")
    # Moved record count message here
    if 'df_export' in st.session_state:
        st.success(f"Report generated with {len(st.session_state.df_export)} active records (excluding completed TOs)")

with col2:
    to_date = st.date_input("To Date", value=end_date)
    # Download button
    if 'df_export' in st.session_state:
        @st.cache_data
        def convert_df_to_csv(df):
            return df.to_csv(index=False).encode('utf-8')
        
        csv = convert_df_to_csv(st.session_state.df_export)
        st.download_button(
            label="📥 Download Excel Report",
            data=csv,
            file_name=f"to_status_report_{datetime.now().strftime('%Y%m%d')}.csv",
            mime='text/csv',
            key='download_button'
        )

# Connection strings
@st.cache_resource
def get_db_connections():
    bcs_conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=caappsdb,1435;'
        'DATABASE=BCSSoft_ConAppSys;'
        'UID=Deepak;'
        'PWD=Deepak@321;'
        'Trusted_Connection=no;'
    )

    edlive_conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=nav18db;'
        'DATABASE=EDLIVE;'
        'UID=barcode1;'
        'PWD=barcode@1433;'
        'Trusted_Connection=no;'
    )
    
    return bcs_conn_str, edlive_conn_str

bcs_conn_str, edlive_conn_str = get_db_connections()

# Query from BCS with date range filter
def get_bcs_data(from_date, to_date):
    bcs_sql = f"""
    SELECT 
        toh.TONo,
        toh.TOStsCode AS TOStatus,
        tl.TruckLoadStsCode,
        tl.LoadClosedDt,
        tl.UnloadClosedDt AS TruckUnloadClosed,
        tl.TONo AS TL_TONo,
        tl.HostHeaderNo
    FROM [dbo].[tbTOHeader] toh
    INNER JOIN [dbo].[tbTruckLoadHeader] tl
        ON toh.TONo = tl.TONo
    WHERE 
        tl.SourceFrom = 'TO'
        AND tl.CreatedDt BETWEEN '{from_date}' AND '{to_date}';
    """
    with pyodbc.connect(bcs_conn_str) as conn_bcs:
        return pd.read_sql(bcs_sql, conn_bcs)

# Query from EDLIVE
def get_edlive_data():
    edlive_sql = """
    SELECT 
        tsh.[External Document No_] AS Shipment_External_Doc_No,
        tsh.[External Document No_ 2] AS Shipment_External_Doc_2,
        tsh.[Created by WS] AS Shipment_Created_By,
        trh.[External Document No_] AS Receipt_External_Doc_No,
        trh.[External Document No_ 2] AS Receipt_External_Doc_2,
        trh.[Created by WS] AS Receipt_Created_By
    FROM [dbo].[Eastern Decorator Sdn_ Bhd_$Transfer Shipment Header] tsh
    LEFT JOIN [dbo].[Eastern Decorator Sdn_ Bhd_$Transfer Receipt Header] trh
        ON tsh.[External Document No_] = trh.[External Document No_];
    """
    with pyodbc.connect(edlive_conn_str) as conn_edlive:
        return pd.read_sql(edlive_sql, conn_edlive)

# Main processing function
def process_data(from_date, to_date):
    with st.spinner('Fetching data from databases...'):
        df_bcs = get_bcs_data(from_date, to_date)
        df_edlive = get_edlive_data()

    # Merge data
    df_merged = pd.merge(
        df_bcs,
        df_edlive,
        how='left',
        left_on='HostHeaderNo',
        right_on='Shipment_External_Doc_No'
    )

    # Add TruckUnloadClosedStatus
    df_merged['TruckUnloadClosedStatus'] = df_merged['TruckUnloadClosed'].apply(
        lambda x: 'Closed' if pd.notnull(x) else 'New'
    )

    # Add ActionTaken
    def get_action(row):
        actions = []

        if str(row['TruckLoadStsCode']).strip().lower() == 'invalid':
            return 'TOInvalid'

        if str(row['TOStatus']).strip().lower() == 'new':
            actions.append('TO need to be Closed')
        if str(row['TruckUnloadClosedStatus']).strip().lower() == 'new':
            actions.append('TruckUnload need to be Closed')
        if str(row['TruckLoadStsCode']).strip().lower() == 'new':
            actions.append('TruckLoad need to be Closed')
        return '; '.join(actions) if actions else ''

    df_merged['ActionTaken'] = df_merged.apply(get_action, axis=1)

    # Add IsTOCompleted?
    def get_completion_status(row):
        truckload_status = str(row['TruckLoadStsCode']).strip().lower()
        to_status = str(row['TOStatus']).strip().lower()
        truckunload_status = str(row['TruckUnloadClosedStatus']).strip().lower()
        ship = pd.notnull(row['Shipment_External_Doc_No'])
        rec = pd.notnull(row['Receipt_External_Doc_No'])

        if truckload_status == 'invalid':
            return 'Invalid'
        elif ship and not rec:
            return 'Receiving yet Done in NAV'
        elif ship and rec:
            return 'TOCompleted'
        elif not ship and not rec and truckload_status == 'closed' and truckunload_status == 'closed':
            return 'TODeleted(NAV)'
        elif to_status == 'closed' and truckload_status == 'closed' and truckunload_status == 'new':
            return 'Pending TruckUnload to Closed'
        elif to_status == 'new' and truckload_status == 'new' and truckunload_status == 'new':
            return 'TO yet created in NAV'
        elif to_status == 'closed' and truckload_status == 'new' and truckunload_status == 'new':
            return 'Ship & Receiving yet Done in NAV'
        else:
            return 'In Progress or Unknown'

    df_merged['IsTOCompleted?'] = df_merged.apply(get_completion_status, axis=1)

    # Filter out completed TOs before reordering columns
    df_filtered = df_merged[df_merged['IsTOCompleted?'] != 'TOCompleted']

    # Reorder columns
    cols = list(df_filtered.columns)
    truck_unload_idx = cols.index('TruckUnloadClosed')
    cols.remove('TruckUnloadClosedStatus')
    cols.insert(truck_unload_idx + 1, 'TruckUnloadClosedStatus')
    cols.remove('ActionTaken')
    cols.remove('IsTOCompleted?')
    cols.extend(['ActionTaken', 'IsTOCompleted?'])
    df_filtered = df_filtered[cols]

    # Drop hidden columns
    columns_to_drop = [
        'TL_TONo', 'HostHeaderNo',
        'Shipment_Created_By', 'Receipt_Created_By',
        'Receipt_External_Doc_No', 'Receipt_External_Doc_2'
    ]
    return df_filtered.drop(columns=columns_to_drop)

# Process data when dates are selected or refresh is clicked
if (from_date and to_date) and (refresh_button or 'df_export' not in st.session_state):
    if from_date > to_date:
        st.error("Error: From Date must be before To Date")
    else:
        df_export = process_data(from_date, to_date)
        st.session_state.df_export = df_export
        
        # Add row numbers starting from 1 (without column name)
        if '' in df_export.columns:
            df_export = df_export.drop(columns=[''])
        df_export.insert(0, '', range(1, 1 + len(df_export)))

        # Display data with empty header for row numbers
        st.dataframe(
            df_export,
            use_container_width=True,
            hide_index=True,
            column_config={
                "": st.column_config.NumberColumn("", width="small"),
                "LoadClosedDt": st.column_config.DatetimeColumn("Load Closed Date"),
                "TruckUnloadClosed": st.column_config.DatetimeColumn("Unload Closed Date"),
                "TruckUnloadClosedStatus": st.column_config.TextColumn("Unload Status"),
                "ActionTaken": st.column_config.TextColumn("Action Required"),
                "IsTOCompleted?": st.column_config.TextColumn("Completion Status")
            }
        )

# Display existing data if refresh wasn't clicked but data exists
elif 'df_export' in st.session_state:
    df_export = st.session_state.df_export.copy()
    if '' in df_export.columns:
        df_export = df_export.drop(columns=[''])
    df_export.insert(0, '', range(1, 1 + len(df_export)))
    st.dataframe(
        df_export,
        use_container_width=True,
        hide_index=True,
        column_config={
            "": st.column_config.NumberColumn("", width="small"),
            "LoadClosedDt": st.column_config.DatetimeColumn("Load Closed Date"),
            "TruckUnloadClosed": st.column_config.DatetimeColumn("Unload Closed Date"),
            "TruckUnloadClosedStatus": st.column_config.TextColumn("Unload Status"),
            "ActionTaken": st.column_config.TextColumn("Action Required"),
            "IsTOCompleted?": st.column_config.TextColumn("Completion Status")
        }
    )