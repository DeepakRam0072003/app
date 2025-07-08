import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime, timedelta
from io import BytesIO
import xlsxwriter

# --- Database Configuration ---
CONFIG = {
    'server': 'caappsdb,1435',
    'database': 'BCSSoft_ConAppSys',
    'username': 'Deepak',
    'password': 'Deepak@321',
    'driver': 'ODBC Driver 17 for SQL Server'
}

# --- Establish DB Connection ---
def get_db_engine():
    connection_string = (
        f"mssql+pyodbc://{CONFIG['username']}:%s@{CONFIG['server']}/{CONFIG['database']}?"
        f"driver={quote_plus(CONFIG['driver'])}&TrustServerCertificate=yes"
    ) % quote_plus(CONFIG['password'])
    return create_engine(connection_string)

# --- Fetch ORP Delay Data ---
def fetch_orp_data(engine, start_date, end_date):
    query = text("""
        SELECT
            rh.CounterCode,
            rh.ShipFromCode,
            th.TONo,
            rh.ReceiptNo,
            th.LoadClosedDt, 
            th.UnloadClosedDt,
            rh.ClosedDt AS ClosedReceipt
        FROM 
            [BCSSoft_ConAppSys].[dbo].[tbTruckLoadHeader] th
        LEFT JOIN 
            [BCSSoft_ConAppSys].[dbo].[tbReceiptHeader] rh 
            ON th.TONo = rh.ReceiptNo
        WHERE 
            th.SourceFrom = 'orp' 
            AND th.LoadClosedDt >= :start_date
            AND th.LoadClosedDt <= :end_date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={'start_date': start_date, 'end_date': end_date})

    df['Unload Status'] = df['UnloadClosedDt'].apply(lambda x: 'New' if pd.isna(x) else 'Closed')
    df['Receipt Status'] = df['ClosedReceipt'].apply(lambda x: 'New' if pd.isna(x) else 'Closed')

    df['Load vs Unload Duration (Days)'] = df.apply(
        lambda row: (row['UnloadClosedDt'] - row['LoadClosedDt']).days
        if pd.notna(row['UnloadClosedDt']) and pd.notna(row['LoadClosedDt']) else None, axis=1)
    df['Load vs Unload Duration (Hours)'] = df.apply(
        lambda row: (row['UnloadClosedDt'] - row['LoadClosedDt']).seconds // 3600
        if pd.notna(row['UnloadClosedDt']) and pd.notna(row['LoadClosedDt']) else None, axis=1)
    df['Load vs Unload Duration (Minutes)'] = df.apply(
        lambda row: ((row['UnloadClosedDt'] - row['LoadClosedDt']).seconds % 3600) // 60
        if pd.notna(row['UnloadClosedDt']) and pd.notna(row['LoadClosedDt']) else None, axis=1)

    df['Unload vs Receipt (Days)'] = df.apply(
        lambda row: (row['ClosedReceipt'] - row['UnloadClosedDt']).days
        if pd.notna(row['ClosedReceipt']) and pd.notna(row['UnloadClosedDt']) else None, axis=1)
    df['Unload vs Receipt (Hours)'] = df.apply(
        lambda row: (row['ClosedReceipt'] - row['UnloadClosedDt']).seconds // 3600
        if pd.notna(row['ClosedReceipt']) and pd.notna(row['UnloadClosedDt']) else None, axis=1)
    df['Unload vs Receipt (Minutes)'] = df.apply(
        lambda row: ((row['ClosedReceipt'] - row['UnloadClosedDt']).seconds % 3600) // 60
        if pd.notna(row['ClosedReceipt']) and pd.notna(row['UnloadClosedDt']) else None, axis=1)

    column_order = [
        'CounterCode', 'ShipFromCode', 'TONo', 'ReceiptNo', 'LoadClosedDt', 'UnloadClosedDt',
        'ClosedReceipt', 'Unload Status', 'Receipt Status',
        'Load vs Unload Duration (Days)', 'Load vs Unload Duration (Hours)', 'Load vs Unload Duration (Minutes)',
        'Unload vs Receipt (Days)', 'Unload vs Receipt (Hours)', 'Unload vs Receipt (Minutes)'
    ]
    return df[column_order]

# --- Excel Export ---
def to_excel(df: pd.DataFrame) -> BytesIO:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='ORP Data', index=True, index_label='No.', startrow=2)
        workbook = writer.book
        worksheet = writer.sheets['ORP Data']

        # Merge headers
        load_start = df.columns.get_loc('Load vs Unload Duration (Days)') + 1
        load_end = df.columns.get_loc('Load vs Unload Duration (Minutes)') + 1
        receipt_start = df.columns.get_loc('Unload vs Receipt (Days)') + 1
        receipt_end = df.columns.get_loc('Unload vs Receipt (Minutes)') + 1

        merged_format = workbook.add_format({
            'bold': True, 'font_color': 'white', 'fg_color': '#4472C4',
            'align': 'center', 'valign': 'vcenter', 'border': 1
        })
        header_format = workbook.add_format({
            'bold': True, 'text_wrap': True, 'valign': 'top',
            'fg_color': '#4472C4', 'font_color': 'white', 'border': 1
        })

        worksheet.merge_range(1, load_start, 1, load_end, "Duration to Complete TruckUnload", merged_format)
        worksheet.merge_range(1, receipt_start, 1, receipt_end, "Duration to Complete Receipt", merged_format)

        for col_num, value in enumerate(df.columns.values, 1):
            worksheet.write(2, col_num, value, header_format)
        worksheet.write(2, 0, 'No.', header_format)

        for i, col in enumerate(df.columns):
            width = max(df[col].astype(str).map(len).max(), len(str(col))) + 2
            worksheet.set_column(i + 1, i + 1, min(width, 30))
        worksheet.set_column(0, 0, 5)

    output.seek(0)
    return output

# --- Main Streamlit App ---
def main():
    st.set_page_config(page_title="ORP Delay Report", layout="wide")
    st.title("ORP Delay Report")

    # Default date range: last 3 months
    today = datetime.today()
    default_start = today - timedelta(days=90)

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From Date", default_start)
        refresh_clicked = st.button("🔄 Refresh Data")

    with col2:
        end_date = st.date_input("To Date", today)

    if start_date > end_date:
        st.error("From Date must be before or equal to To Date.")
        return

    # Auto-fetch on load or refresh click
    if 'df' not in st.session_state or refresh_clicked:
        try:
            with st.spinner("🔄 Fetching ORP data..."):
                engine = get_db_engine()
                df = fetch_orp_data(engine, start_date, end_date)
                df.index = range(1, len(df) + 1)
                st.session_state.df = df
        except Exception as e:
            st.error(f"❌ Error loading data: {e}")
            return

    # Display data
    df = st.session_state.get('df', pd.DataFrame())
    if not df.empty:
        st.success(f"✅ Loaded {len(df)} records.")
        st.dataframe(df, use_container_width=True)

        with col2:
            excel_data = to_excel(df)
            st.download_button(
                label="📥 Download Excel Report",
                data=excel_data,
                file_name=f"ORP_Report_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    else:
        st.warning("⚠️ No data found for the selected date range.")

if __name__ == '__main__':
    main()
