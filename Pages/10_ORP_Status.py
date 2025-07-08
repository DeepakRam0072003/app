import pandas as pd
from sqlalchemy import create_engine, text
import urllib
import streamlit as st
from datetime import datetime, timedelta

# Streamlit page configuration
st.set_page_config(
    page_title="ORP Status Report",
    page_icon="📊",
    layout="wide"
)

# Title
st.title("ORP Status Report")

# Date range selection
col1, col2 = st.columns(2)
with col1:
    from_date = st.date_input("From Date", value=datetime.now() - timedelta(days=90))
    refresh_button = st.button("🔄 Refresh Data")

with col2:
    to_date = st.date_input("To Date", value=datetime.now())
    # Placeholder for download button - will be populated after data loads
    download_placeholder = st.empty()

# Connection strings
@st.cache_resource
def get_db_connections():
    orp_connection_string = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=caappsdb,1435;'
        'DATABASE=BCSSoft_ConAppSys;'
        'UID=Deepak;'
        'PWD=Deepak@321;'
        'Trusted_Connection=no;'
    )

    nav_connection_string = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=nav18db;'
        'DATABASE=EDLIVE;'
        'UID=barcode1;'
        'PWD=barcode@1433;'
        'Trusted_Connection=no;'
    )

    engine_orp = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(orp_connection_string)}")
    engine_edlive = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(nav_connection_string)}")
    
    return engine_orp, engine_edlive

engine_orp, engine_edlive = get_db_connections()

# Modified query to use date range
@st.cache_data(ttl=3600, show_spinner="Fetching ORP data...")
def get_orp_data(from_date, to_date):
    query_orp = f"""
    WITH ShipmentData AS (
        SELECT 
            h.ORPNo,
            h.CounterCode,
            h.CreatedDt,
            d.WMSOrderKey,
            d.WMSCfmSts,
            ROW_NUMBER() OVER (PARTITION BY h.ORPNo, d.WMSOrderKey ORDER BY h.LastModDt DESC) AS RowNum
        FROM 
            [dbo].[tbORPTempHdr] h
        INNER JOIN 
            [dbo].[tbORPTempDtl] d ON h.ORPTempHdrId = d.ORPTempHdrId
        WHERE 
            d.WMSCfmSts = 'Shipped Complete'
            AND h.CreatedDt BETWEEN '{from_date}' AND '{to_date}'
    ),
    TruckData AS (
        SELECT 
            TONo,
            TruckLoadStsCode,
            UnloadClosedDt,
            CASE 
                WHEN UnloadClosedDt IS NOT NULL THEN 'Closed' 
                ELSE 'Open'
            END AS TruckUnloadStatus
        FROM 
            [dbo].[tbTruckLoadHeader]
        WHERE 
            TONo LIKE '0000%'
    ),
    ReceiptData AS (
        SELECT 
            rh.WMSOrderNo,
            rh.ReceiptStsCode,
            rd.ReceiveClosedDt
        FROM 
            [dbo].[tbReceiptHeader] rh
        INNER JOIN 
            [dbo].[tbReceiptDetail] rd
            ON rh.ReceiptHeaderId = rd.ReceiptHeaderId
    )
    SELECT 
        s.ORPNo,
        s.CounterCode,
        s.CreatedDt,
        s.WMSOrderKey,
        s.WMSCfmSts,
        t.TruckLoadStsCode,
        t.UnloadClosedDt,
        t.TruckUnloadStatus,
        r.ReceiptStsCode,
        r.ReceiveClosedDt
    FROM 
        ShipmentData s
    INNER JOIN 
        TruckData t ON s.WMSOrderKey = t.TONo
    LEFT JOIN 
        ReceiptData r ON s.WMSOrderKey = r.WMSOrderNo
    WHERE 
        s.RowNum = 1
    ORDER BY 
        s.CreatedDt DESC, 
        t.UnloadClosedDt DESC
    """
    with engine_orp.connect() as conn_orp:
        return pd.read_sql(text(query_orp), conn_orp)

# Query to get Transfer data from EDLIVE
@st.cache_data(ttl=3600, show_spinner="Fetching transfer data...")
def get_transfer_data():
    query_transfer = """
    SELECT 
        [Transfer Order No_] AS TransferOrderNo,
        [External Document No_] AS ExternalDocNo
    FROM 
        [dbo].[Eastern Decorator Sdn_ Bhd_$Transfer Receipt Header]
    """
    with engine_edlive.connect() as conn_edlive:
        return pd.read_sql(text(query_transfer), conn_edlive)

# Main processing function
def process_data(from_date, to_date):
    # Clear cache if refresh button is clicked
    if refresh_button:
        st.cache_data.clear()
    
    df_orp = get_orp_data(from_date, to_date)
    df_transfer = get_transfer_data()

    df_merged = pd.merge(
        df_orp,
        df_transfer,
        left_on='WMSOrderKey',
        right_on='ExternalDocNo',
        how='left'
    )

    df_merged['NAVPostedReceipt'] = df_merged['TransferOrderNo'].notna().map({True: 'TRUE', False: 'FALSE'})

    def determine_action(row):
        if row['TruckUnloadStatus'] == 'Closed' and row['ReceiptStsCode'] == 'Closed':
            return 'Completed'
        elif row['ReceiptStsCode'] == 'Received':
            return 'There are Variance in Receiving - Require to Confirm Receipt Manually in Portal'
        elif row['TruckUnloadStatus'] == 'Open' and row['ReceiptStsCode'] == 'New':
            return 'TruckUnload must be Closed and Receiving must be Closed'
        elif row['TruckUnloadStatus'] == 'Closed' and row['ReceiptStsCode'] == 'New':
            return 'Receiving must be Closed'
        elif row['NAVPostedReceipt'] == 'FALSE':
            return 'TruckUnload must be Closed'
        else:
            return ''

    df_merged['ActionTaken'] = df_merged.apply(determine_action, axis=1)
    
    # Filter out "Completed" actions before returning
    df_filtered = df_merged[df_merged['ActionTaken'] != 'Completed']
    
    # Reorder columns to put ActionTaken first
    cols = ['ActionTaken'] + [col for col in df_filtered.columns if col != 'ActionTaken']
    return df_filtered[cols]

# Process and display data automatically when dates are selected
if from_date and to_date:
    if from_date > to_date:
        st.error("Error: From Date must be before To Date")
    else:
        df_export = process_data(from_date, to_date)
        
        # Add row numbers starting from 1 (without column name)
        df_display = df_export.copy()
        if '' in df_display.columns:
            df_display = df_display.drop(columns=[''])
        df_display.insert(0, '', range(1, 1 + len(df_display)))

        # Display data with empty header for the numbers column
        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "": st.column_config.NumberColumn("", width="small"),
                "CreatedDt": st.column_config.DatetimeColumn("Created Date"),
                "UnloadClosedDt": st.column_config.DatetimeColumn("Unload Closed Date"),
                "ReceiveClosedDt": st.column_config.DatetimeColumn("Receive Closed Date"),
                "NAVPostedReceipt": st.column_config.TextColumn("NAV Posted Receipt"),
                "ActionTaken": st.column_config.TextColumn("Action Required"),
                "Action Required": st.column_config.TextColumn("Action Required", width="large")  # This makes the column auto-expand
            }
        )

        # Record count message under refresh button
        with col1:
            st.success(f"Report generated with {len(df_export)} records requiring action")

        # Download button under To Date
        with col2:
            @st.cache_data
            def convert_df_to_csv(df):
                return df.to_csv(index=False).encode('utf-8')
            
            csv = convert_df_to_csv(df_export)
            download_placeholder.download_button(
                label="📥 Download Excel Report",
                data=csv,
                file_name=f"orp_status_report_{datetime.now().strftime('%Y%m%d')}.csv",
                mime='text/csv',
                key='download_button'
            )