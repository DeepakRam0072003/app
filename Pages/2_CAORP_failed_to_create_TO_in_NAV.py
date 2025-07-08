import streamlit as st
import pandas as pd
import pyodbc
from io import BytesIO
from datetime import datetime, timedelta

# Database connection details
server = 'caappsdb,1435'
database = 'BCSSoft_ConAppSys'
username = 'Deepak'
password = 'Deepak@321'

def get_connection():
    conn_str = (
        f"DRIVER={{SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
    )
    return pyodbc.connect(conn_str)

def fetch_orp_data(conn, start_date, end_date):
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    query = f"""
    SELECT DISTINCT
        ln.LogMsg AS FailedToCreateNAVTO,
        orph.CreatedDt, 
        orph.ORPTempHdrId,
        orph.ORPNo,
        orph.CounterCode AS ShipToCounter,
        orph.ORPStatus,
        orpd.NavTONo,
        CASE 
            WHEN orpd.NavTONo IS NOT NULL AND orpd.NavTONo <> '' THEN 'OK'
            ELSE 'Not OK'
        END AS NAVTOCreationStatus,
        orpd.wmsorderkey,
        orpd.WMSCfmSts
    FROM 
        [BCSSoft_ConAppSys].[dbo].[tbORPTempHdr] orph
    JOIN 
        [BCSSoft_ConAppSys].[dbo].[tbORPTempDtl] orpd
        ON orph.ORPTempHdrId = orpd.ORPTempHdrId
    LEFT JOIN 
        [BCSSoft_ConAppSys].[dbo].[tbIntgNavLog] ln
        ON orpd.wmsorderkey = ln.wmsorderkey
        AND ln.LogTypeCode = 'ws_CA_TODc2Counter'
        AND ln.LogStsCode = 'E'
    WHERE
        orph.ORPStatus = 'WMSShipped'
        AND orpd.WMSCfmSts = 'Shipped Complete'
        AND ln.LogMsg IS NOT NULL
        AND orph.CreatedDt BETWEEN '{start_str}' AND '{end_str}'
    ORDER BY 
        orph.CreatedDt DESC;
    """
    
    df = pd.read_sql(query, conn)
    df.drop_duplicates(subset=['wmsorderkey'], inplace=True)
    df.drop(columns=['ORPTempHdrId'], errors='ignore', inplace=True)
    return df

def create_excel(df):
    # Ensure column order is maintained
    desired_column_order = ['FailedToCreateNAVTO'] + [col for col in df.columns if col != 'FailedToCreateNAVTO']
    df = df[desired_column_order]
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
        workbook = writer.book
        worksheet = writer.sheets['Report']

        # Format header
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#4472C4',
            'font_color': 'white',
            'border': 1,
            'align': 'center'
        })

        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # Conditional formatting
        red_fill = workbook.add_format({'bg_color': '#FF0000', 'font_color': 'white'})
        green_fill = workbook.add_format({'bg_color': '#00FF00', 'font_color': 'black'})

        if 'NAVTOCreationStatus' in df.columns:
            col_idx = df.columns.get_loc('NAVTOCreationStatus')
            for row_num in range(1, len(df) + 1):
                value = df.iloc[row_num - 1]['NAVTOCreationStatus']
                if value == 'Not OK':
                    worksheet.write(row_num, col_idx, value, red_fill)
                elif value == 'OK':
                    worksheet.write(row_num, col_idx, value, green_fill)

        # Auto column width
        for col_num, column in enumerate(df.columns):
            max_length = max(df[column].astype(str).map(len).max(), len(str(column)))
            worksheet.set_column(col_num, col_num, max_length + 2)

    output.seek(0)
    return output

def main():
    st.title("NAV Failed to Create TO (CAORP) Report")

    today = datetime.today()
    default_start = today - timedelta(days=90)

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From Date", default_start)
        refresh = st.button("🔄 Refresh Data")
    with col2:
        end_date = st.date_input("To Date", today)

    if start_date > end_date:
        st.error("❌ From Date must be before or equal to To Date.")
        return

    if refresh or 'orp_df' not in st.session_state:
        try:
            with st.spinner("Fetching data from database..."):
                conn = get_connection()
                df_full = fetch_orp_data(conn, start_date, end_date)
                conn.close()
                st.session_state.orp_df = df_full
        except Exception as e:
            st.error(f"❌ Error while fetching data: {e}")
            return

    df_full = st.session_state.orp_df
    df_display = df_full[df_full['NavTONo'].isnull() | (df_full['NavTONo'].astype(str).str.strip() == '')].copy()

    if df_display.empty:
        st.warning("No ORP records found without TO number in the selected range.")
    else:
        # Ensure column order is maintained
        desired_column_order = ['FailedToCreateNAVTO'] + [col for col in df_display.columns if col != 'FailedToCreateNAVTO']
        df_display = df_display[desired_column_order]
        
        df_display.index = range(1, len(df_display) + 1)
        st.success(f"✅ Showing {len(df_display)} records without TO number.")
        st.dataframe(df_display, use_container_width=True)

    with col2:
        if not df_full.empty:
            excel_bytes = create_excel(df_full)
            st.download_button(
                label="📥 Download Full Excel Report",
                data=excel_bytes,
                file_name=f"CA_NAVORP_Creation_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

if __name__ == "__main__":
    main()