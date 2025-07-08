import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from io import BytesIO

# Configuration for both databases
CONFIG = {
    'nav': {
        'server': 'nav18db',
        'database': 'EDLIVE',
        'username': 'barcode1',
        'password': 'barcode@1433',
        'driver': 'ODBC Driver 17 for SQL Server'
    },
    'to': {
        'server': 'caappsdb,1435',
        'database': 'BCSSoft_ConAppSys',
        'username': 'Deepak',
        'password': 'Deepak@321',
        'driver': 'ODBC Driver 17 for SQL Server'
    }
}

def get_db_connection(db_type):
    config = CONFIG[db_type]
    connection_string = (
        f"mssql+pyodbc://{config['username']}:%s@{config['server']}/{config['database']}"
        f"?driver={quote_plus(config['driver'])}&TrustServerCertificate=yes"
    ) % quote_plus(config['password'])
    return create_engine(connection_string)

def get_transfer_data(engine, start_date, end_date):
    query = text(f"""
        SELECT [No_], 
               CASE WHEN Status = 1 THEN 'Released'
                    WHEN Status = 0 THEN 'Open'
                    ELSE CAST(Status AS VARCHAR(10)) END AS [Status],
               [External Document No_] AS [TONo],
               [Transfer-from Code], [Transfer-to Code], [Posting Date]
        FROM [dbo].[Eastern Decorator Sdn_ Bhd_$Transfer Header]
        WHERE Status IN (0,1) AND [Posting Date] BETWEEN '{start_date}' AND '{end_date}'
    """)
    return pd.read_sql(query, engine)

def get_transfer_shipment_headers(engine, start_date, end_date):
    query = text(f"""
        SELECT [Transfer Order No_], [External Document No_]
        FROM [dbo].[Eastern Decorator Sdn_ Bhd_$Transfer Shipment Header]
        WHERE [Posting Date] BETWEEN '{start_date}' AND '{end_date}'
    """)
    df = pd.read_sql(query, engine)
    df['External Document No_'] = df['External Document No_'].str.strip().str.upper()
    return df

def get_transfer_receipt_headers(engine, start_date, end_date):
    query = text(f"""
        SELECT [Transfer Order No_], [External Document No_]
        FROM [dbo].[Eastern Decorator Sdn_ Bhd_$Transfer Receipt Header]
        WHERE [Posting Date] BETWEEN '{start_date}' AND '{end_date}'
    """)
    df = pd.read_sql(query, engine)
    df['External Document No_'] = df['External Document No_'].str.strip().str.upper()
    return df

def get_truck_load_errors(engine, start_date, end_date):
    query = text(f"""
        SELECT tl.TONo, tl.HostHeaderNo, tl.TruckLoadStsCode, tl.UnloadClosedDt, tl.CreatedDt,
               ship.LogMsg AS ShipErrorMsg, receipt.LogMsg AS ReceiptErrorMsg
        FROM [dbo].[tbTruckLoadHeader] tl
        LEFT JOIN [dbo].[tbIntgNavLog] ship
            ON (tl.TONo = ship.WMSOrderKey OR tl.TruckLoadHeaderId = ship.RefHdrId)
            AND ship.LogTypeCode = 'ws_CA_PostTO-TO(PostShip)' AND ship.LogStsCode = 'E'
        LEFT JOIN [dbo].[tbIntgNavLog] receipt
            ON (tl.TONo = receipt.WMSOrderKey OR tl.TruckLoadHeaderId = receipt.RefHdrId)
            AND receipt.LogTypeCode = 'ws_CA_PostTO-TO(PostReceipt)' AND receipt.LogStsCode = 'E'
        WHERE tl.UnloadClosedDt IS NOT NULL AND tl.SourceFrom = 'TO'
              AND (ship.LogID IS NOT NULL OR receipt.LogID IS NOT NULL)
              AND tl.CreatedDt BETWEEN '{start_date}' AND '{end_date}'
    """)
    return pd.read_sql(query, engine)

def join_and_analyze_data(transfer_df, to_df, shipment_df, receipt_df):
    transfer_df['TONo'] = transfer_df['TONo'].astype(str).str.strip().str.upper()
    to_df['HostHeaderNo'] = to_df['HostHeaderNo'].astype(str).str.strip().str.upper()

    combined = pd.merge(
        transfer_df,
        to_df,
        left_on='TONo',
        right_on='HostHeaderNo',
        how='inner'
    )

    if combined.empty:
        return pd.DataFrame()

    combined['IsPostedShipment'] = combined['HostHeaderNo'].isin(shipment_df['External Document No_'])
    combined['IsPostedReceipt'] = combined['HostHeaderNo'].isin(receipt_df['External Document No_'])
    combined['IsFullyPosted'] = combined['IsPostedShipment'] & combined['IsPostedReceipt']

    # Reorder columns to put error messages first
    error_cols = ['ShipErrorMsg', 'ReceiptErrorMsg']
    other_cols = [col for col in combined.columns if col not in error_cols]
    return combined[error_cols + other_cols].drop_duplicates()

def save_report_to_bytes(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Create display DataFrame with error messages first
        error_cols = ['ShipErrorMsg', 'ReceiptErrorMsg']
        other_cols = [col for col in df.columns if col not in error_cols]
        display_df = df[error_cols + other_cols].copy()
        
        display_df.index = range(1, len(display_df) + 1)  # Start row number from 1
        display_df.to_excel(writer, sheet_name='CATO Data', index=True, index_label='No.')
        
        workbook = writer.book
        worksheet = writer.sheets['CATO Data']

        header_fmt = workbook.add_format({
            'bold': True, 'bg_color': '#4472C4', 'font_color': 'white',
            'text_wrap': True, 'valign': 'top', 'border': 1
        })

        # Write headers with error messages first
        worksheet.write(0, 0, 'No.', header_fmt)
        for i, col in enumerate(['ShipErrorMsg', 'ReceiptErrorMsg'] + [c for c in df.columns if c not in ['ShipErrorMsg', 'ReceiptErrorMsg']], 1):
            worksheet.write(0, i, col, header_fmt)
            
            # Set column widths
            max_len = max(df[col].astype(str).map(len).max() if col in df.columns else len(col), len(col)) + 2
            worksheet.set_column(i, i, min(max_len, 30))
        
        worksheet.set_column(0, 0, 5)

        bool_cols = ['IsPostedShipment', 'IsPostedReceipt', 'IsFullyPosted']
        true_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        false_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})

        for col in bool_cols:
            if col in df.columns:
                idx = df.columns.get_loc(col) + 3  # +3 because we have No. + 2 error columns
                worksheet.conditional_format(1, idx, len(df), idx, {
                    'type': 'cell', 'criteria': '==', 'value': True, 'format': true_fmt
                })
                worksheet.conditional_format(1, idx, len(df), idx, {
                    'type': 'cell', 'criteria': '==', 'value': False, 'format': false_fmt
                })
    output.seek(0)
    return output

def main():
    st.title("CATO (NAVERROR) Report")

    end_date = datetime.now()
    default_start = end_date - timedelta(days=90)

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From Date", default_start)
        refresh_clicked = st.button("🔄 Refresh Data")
    with col2:
        end_date = st.date_input("To Date", end_date)
        download_placeholder = st.empty()

    # Always load data when the page loads or refresh is clicked
    with st.spinner("🔄 Loading TO data..."):
        try:
            nav = get_db_connection('nav')
            to = get_db_connection('to')

            transfer_df = get_transfer_data(nav, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            shipment_df = get_transfer_shipment_headers(nav, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            receipt_df = get_transfer_receipt_headers(nav, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            truck_df = get_truck_load_errors(to, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

            combined_df = join_and_analyze_data(transfer_df, truck_df, shipment_df, receipt_df)

            nav.dispose()
            to.dispose()
        except Exception as e:
            st.error(f"🚨 Error loading data: {str(e)}")
            return

    if combined_df.empty:
        st.warning("⚠️ No data found for the selected date range.")
        return

    # Create display DataFrame with error messages first
    error_cols = ['ShipErrorMsg', 'ReceiptErrorMsg']
    other_cols = [col for col in combined_df.columns if col not in error_cols]
    df_display = combined_df[error_cols + other_cols].copy()
    df_display.index = range(1, len(df_display) + 1)

    excel_bytes = save_report_to_bytes(combined_df)
    with col2:
        download_placeholder.download_button(
            label="📥 Download Excel Report",
            data=excel_bytes,
            file_name=f"CATO_NAVERROR_Report_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    st.dataframe(df_display)

if __name__ == "__main__":
    main()