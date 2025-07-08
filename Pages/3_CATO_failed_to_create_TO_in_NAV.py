import streamlit as st
import pandas as pd
import pyodbc
from io import BytesIO
from datetime import datetime, timedelta

# DB connection config
server = 'caappsdb,1435'
database = 'BCSSoft_ConAppSys'
username = 'Deepak'
password = 'Deepak@321'

def get_db_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    try:
        return pyodbc.connect(conn_str)
    except Exception as e:
        st.error(f"Database connection failed: {str(e)}")
        return None

def run_query(conn, start_date, end_date):
    query = f"""
    SELECT 
        ISNULL(dc.LogMsg, '') AS FailedToCreateNAVTO_DC,
        ISNULL(cc.LogMsg, '') AS FailedToCreateNAVTO_CC,
        tlh.TruckLoadStsCode,
        tlh.CounterCode AS [Transfer-From],
        toh.ShipToCode,
        tlh.TONo,
        tlh.HostHeaderNo,
        tlh.CreatedDt,
        tlh.NavTONo,
        CASE
            WHEN tlh.NavTONo LIKE 'EDTO%' THEN 'OK'
            ELSE 'Failed'
        END AS [CreationStatus],
        toh.XDock,
        toh.TOPurposeCode
    FROM 
        [BCSSoft_ConAppSys].[dbo].[tbTruckLoadHeader] tlh
    LEFT JOIN 
        [BCSSoft_ConAppSys].[dbo].[tbIntgNavLog] dc
        ON tlh.TruckLoadHeaderId = dc.RefHdrId
        AND dc.LogTypeCode = 'ws_CA_TOCounter2DC'
        AND dc.LogStsCode = 'E'
    LEFT JOIN 
        [BCSSoft_ConAppSys].[dbo].[tbIntgNavLog] cc
        ON tlh.TruckLoadHeaderId = cc.RefHdrId
        AND cc.LogTypeCode = 'ws_CA_TOCount2Count'
        AND cc.LogStsCode = 'E'
    LEFT JOIN 
        [BCSSoft_ConAppSys].[dbo].[tbTOHeader] toh
        ON tlh.TONo = toh.TONo
    WHERE 
        tlh.SourceFrom = 'TO'
        AND tlh.TruckLoadStsCode = 'CLOSED'
        AND tlh.CreatedDt BETWEEN '{start_date}' AND '{end_date}'
        AND ISNULL(tlh.NavTONo, '') = ''
    ORDER BY 
        tlh.CreatedDt DESC
    """
    try:
        df = pd.read_sql(query, conn)
        if 'CreationStatus' not in df.columns:
            df['CreationStatus'] = ''
        return df
    except Exception as e:
        st.error(f"Query execution failed: {str(e)}")
        return pd.DataFrame()

def generate_excel(df):
    # Ensure column order is maintained
    desired_columns = ['FailedToCreateNAVTO_DC', 'FailedToCreateNAVTO_CC'] + \
                     [col for col in df.columns if col not in ['FailedToCreateNAVTO_DC', 'FailedToCreateNAVTO_CC']]
    df = df[desired_columns]
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.index = range(1, len(df) + 1)
        df.to_excel(writer, sheet_name='Report', index=True, index_label='No.')

        workbook = writer.book
        worksheet = writer.sheets['Report']

        header_format = workbook.add_format({
            'bold': True, 'text_wrap': True, 'valign': 'top',
            'fg_color': '#4472C4', 'font_color': 'white', 'border': 1, 'align': 'center'
        })

        for col_num, value in enumerate(df.columns.values, 1):
            worksheet.write(0, col_num, value, header_format)
        worksheet.write(0, 0, 'No.', header_format)

        if 'CreationStatus' in df.columns:
            status_col_idx = df.columns.get_loc('CreationStatus') + 1
            green_fill = workbook.add_format({'bg_color': '#5CB85C', 'font_color': 'white', 'bold': True})
            red_fill = workbook.add_format({'bg_color': '#D9534F', 'font_color': 'white', 'bold': True})

            for row_num in range(1, len(df) + 1):
                value = df.iloc[row_num - 1]['CreationStatus']
                fmt = green_fill if value == 'OK' else red_fill
                worksheet.write(row_num, status_col_idx, value, fmt)

        for col_num, col in enumerate(df.columns, 1):
            max_len = max(df[col].astype(str).map(len).max(), len(str(col)))
            worksheet.set_column(col_num, col_num, max_len + 2)
        worksheet.set_column(0, 0, 5)

    output.seek(0)
    return output

def main():
    st.title("NAV Failed to Create TO (CATO) Report")

    today = datetime.now()
    default_start = today - timedelta(days=90)

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From Date", value=default_start)
        refresh_clicked = st.button("🔄 Refresh Data")
    with col2:
        end_date = st.date_input("To Date", value=today)

    if 'df' not in st.session_state:
        st.session_state.df = pd.DataFrame()
    if 'auto_refresh_done' not in st.session_state:
        st.session_state.auto_refresh_done = False

    # Auto-load data on first run
    if refresh_clicked or (not st.session_state.auto_refresh_done and st.session_state.df.empty):
        conn = get_db_connection()
        if conn:
            df = run_query(conn, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            conn.close()
            st.session_state.df = df
            st.session_state.auto_refresh_done = True
            if not df.empty:
                st.success(f"✅ Loaded {len(df)} blank TO records.")
            else:
                st.warning("No blank TO data found in the selected range.")

    if not st.session_state.df.empty:
        df_display = st.session_state.df.copy()
        # Ensure column order is maintained
        desired_columns = ['FailedToCreateNAVTO_DC', 'FailedToCreateNAVTO_CC'] + \
                         [col for col in df_display.columns if col not in ['FailedToCreateNAVTO_DC', 'FailedToCreateNAVTO_CC']]
        df_display = df_display[desired_columns]
        
        df_display.index = range(1, len(df_display) + 1)
        st.dataframe(df_display, use_container_width=True)

        with col2:
            excel_data = generate_excel(df_display)
            st.download_button(
                label="📥 Download Excel Report",
                data=excel_data,
                file_name=f"CATO_BlankNavTO_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

if __name__ == "__main__":
    main()