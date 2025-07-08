import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from io import BytesIO

# Configuration for databases
CONFIG = {
    'nav': {
        'server': 'nav18db',
        'database': 'EDLIVE',
        'username': 'barcode1',
        'password': 'barcode@1433',
        'driver': 'ODBC Driver 17 for SQL Server'
    },
    'orp': {
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

def get_orp_data(engine, start_date, end_date):
    query = text(f"""
    SELECT 
        h.ORPNo,
        h.CounterCode,
        h.CreatedDt AS ORPCreatedDt,
        d.WMSOrderKey,
        d.WMSCfmSts
    FROM 
        [dbo].[tbORPTempHdr] h
    INNER JOIN 
        [dbo].[tbORPTempDtl] d ON h.ORPTempHdrId = d.ORPTempHdrId
    WHERE 
        d.WMSCfmSts = 'Shipped Complete'
        AND h.CreatedDt BETWEEN '{start_date}' AND '{end_date}'
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    df['WMSOrderKey'] = df['WMSOrderKey'].astype(str).str.strip().str.lower()
    return df

def get_truck_load_errors(engine, start_date, end_date):
    query = text(f"""
    SELECT 
        tl.TONo,
        tl.HostHeaderNo,
        tl.TruckLoadStsCode,
        tl.UnloadClosedDt,
        tl.CreatedDt AS TruckCreatedDt,
        ship.LogMsg AS ShipErrorMsg,
        receipt.LogMsg AS ReceiptErrorMsg
    FROM 
        [dbo].[tbTruckLoadHeader] tl
    LEFT JOIN 
        [dbo].[tbIntgNavLog] ship ON tl.TONo = ship.WMSOrderKey
        AND ship.LogTypeCode = 'ws_CA_PostTO-TO(PostShip)'
        AND ship.LogStsCode = 'E'
    LEFT JOIN 
        [dbo].[tbIntgNavLog] receipt ON tl.TONo = receipt.WMSOrderKey
        AND receipt.LogTypeCode = 'ws_CA_PostTO-ORP(PostReceipt)'
        AND receipt.LogStsCode = 'E'
    WHERE 
        tl.UnloadClosedDt IS NOT NULL 
        AND tl.SourceFrom = 'orp'
        AND (ship.logid IS NOT NULL OR receipt.logid IS NOT NULL)
        AND tl.CreatedDt BETWEEN '{start_date}' AND '{end_date}'
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    df['TONo'] = df['TONo'].astype(str).str.strip().str.lower()
    return df

def get_transfer_receipt_data(engine, start_date, end_date):
    query = text(f"""
    SELECT 
        [Transfer Order No_],
        [External Document No_]
    FROM 
        [dbo].[Eastern Decorator Sdn_ Bhd_$Transfer Receipt Header]
    WHERE
        [Posting Date] BETWEEN '{start_date}' AND '{end_date}'
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    df['External Document No_'] = df['External Document No_'].astype(str).str.strip().str.lower()
    return df

def join_and_analyze_data(orp_df, truck_df, receipt_df):
    combined = pd.merge(
        orp_df,
        truck_df,
        left_on='WMSOrderKey',
        right_on='TONo',
        how='inner'
    )
    combined['IsPostedReceipt'] = combined['WMSOrderKey'].isin(receipt_df['External Document No_'])
    combined['IsFullyPosted'] = combined['IsPostedReceipt']

    # Reorder columns to put error messages first
    column_order = [
        'ShipErrorMsg', 'ReceiptErrorMsg',  # Error messages first
        'ORPNo', 'CounterCode', 'ORPCreatedDt', 'WMSOrderKey', 'WMSCfmSts',
        'TruckLoadStsCode', 'UnloadClosedDt', 'IsPostedReceipt'
    ]
    
    # Filter only the columns we want and drop duplicates
    return combined[column_order].drop_duplicates().reset_index(drop=True)

def generate_excel_report(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.index = range(1, len(df) + 1)
        df.to_excel(writer, sheet_name='ORP Data', index=True, index_label='No.')

        workbook = writer.book
        worksheet = writer.sheets['ORP Data']

        header_format = workbook.add_format({
            'bold': True, 'text_wrap': True, 'valign': 'top',
            'fg_color': '#4472C4', 'font_color': 'white', 'border': 1
        })

        for col_num, value in enumerate(df.columns.values, 1):
            worksheet.write(0, col_num, value, header_format)
        worksheet.write(0, 0, 'No.', header_format)

        true_fmt = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        false_fmt = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})

        if 'IsPostedReceipt' in df.columns:
            col_idx = df.columns.get_loc('IsPostedReceipt') + 1
            worksheet.conditional_format(1, col_idx, len(df), col_idx, {
                'type': 'cell', 'criteria': '==', 'value': True, 'format': true_fmt
            })
            worksheet.conditional_format(1, col_idx, len(df), col_idx, {
                'type': 'cell', 'criteria': '==', 'value': False, 'format': false_fmt
            })

        for col_num, column in enumerate(df.columns, 1):
            max_len = max(df[column].astype(str).map(len).max(), len(str(column))) + 2
            worksheet.set_column(col_num, col_num, min(max_len, 30))
        worksheet.set_column(0, 0, 5)

    output.seek(0)
    return output

def main():
    st.title("CAORP (NAVERROR) Report")

    end_date = datetime.now()
    default_start = end_date - timedelta(days=90)

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From Date", default_start)
        refresh_clicked = st.button("🔄 Refresh Data")
    with col2:
        end_date = st.date_input("To Date", end_date)
        download_placeholder = st.empty()

    # Always reload data when the function runs
    with st.spinner("🔄 Loading ORP Data..."):
        try:
            orp_engine = get_db_connection('orp')
            nav_engine = get_db_connection('nav')

            orp_df = get_orp_data(orp_engine, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            truck_df = get_truck_load_errors(orp_engine, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            receipt_df = get_transfer_receipt_data(nav_engine, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

            combined_df = join_and_analyze_data(orp_df, truck_df, receipt_df)

            orp_engine.dispose()
            nav_engine.dispose()
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            combined_df = pd.DataFrame()

    if not combined_df.empty:
        combined_df.index = range(1, len(combined_df) + 1)
        excel_bytes = generate_excel_report(combined_df)

        with col2:
            download_placeholder.download_button(
                label="📥 Download Excel Report",
                data=excel_bytes,
                file_name=f"CAORP_NAVERROR_Report_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        st.dataframe(combined_df)
    else:
        st.warning("⚠️ No ORP data found for the selected date range.")

if __name__ == "__main__":
    main()