import streamlit as st
import pandas as pd
import pyodbc
from datetime import datetime
from dateutil.relativedelta import relativedelta
from io import BytesIO

def create_conn_str(config):
    return (
        f"DRIVER={{{config['driver']}}};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']};"
        f"Timeout={config.get('timeout', 30)};"
    )

def fetch_bcs_data(start_date, end_date, conn_str):
    query = """
    WITH RankedRows AS (
        SELECT 
            h.SOHeaderId,
            h.SONo,
            h.SOTypeCode,
            CONVERT(varchar, h.SODt, 120) AS SODt,
            d.SODetailId,
            l.LogTypeCode,
            l.LogStsCode,
            l.LogMsg,
            ROW_NUMBER() OVER (PARTITION BY h.SOHeaderId ORDER BY d.SODetailId) AS rn
        FROM 
            [BCSSoft_ConAppSys].[dbo].[tbSOHeader] h
        INNER JOIN 
            [BCSSoft_ConAppSys].[dbo].[tbSODetail] d ON h.SOHeaderId = d.SOHeaderId
        INNER JOIN 
            [BCSSoft_ConAppSys].[dbo].[tbIntgNavLog] l ON h.SOHeaderId = l.RefHdrId AND d.SODetailId = l.RefDtlId
        WHERE 
            l.LogTypeCode = 'ws_ItemJournal'
            AND l.LogStsCode = 'E'
            AND h.SODt BETWEEN ? AND ?
    )
    SELECT 
        SOHeaderId,
        SONo,
        SOTypeCode,
        SODt,
        SODetailId,
        LogTypeCode,
        LogStsCode,
        LogMsg
    FROM RankedRows
    WHERE rn = 1
    ORDER BY SOHeaderId
    """
    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql(query, conn, params=[start_date, end_date])
    return df

def sono_to_docno(sono):
    try:
        if not isinstance(sono, str) or not sono.startswith("SO_"):
            return None
        parts = sono[3:].split("_")
        if len(parts) != 3:
            return None
        prefix, date, time = parts
        short_date = date[2:] if date.startswith("20") else date
        return f"{prefix}{short_date}{time}"
    except Exception:
        return None

def fetch_existing_docnos(doc_no_list, conn_str):
    if not doc_no_list:
        return set()
    in_clause = ",".join(f"'{doc}'" for doc in doc_no_list)
    query = f"""
    SELECT DISTINCT [Document No_]
    FROM [EDLIVE].[dbo].[Eastern Decorator Sdn_ Bhd_$Item Ledger Entry]
    WHERE [Entry Type] = '1'
    AND [Document No_] IN ({in_clause})
    """
    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql(query, conn)
    return set(df['Document No_'].dropna().unique())

def create_excel(df):
    export_df = df.copy()
    columns_to_hide = ['LogTypeCode', 'SOHeaderId', 'SODetailId']
    for col in columns_to_hide:
        if col in export_df.columns:
            export_df.drop(col, axis=1, inplace=True)
    
    # Reorder columns to put LogMsg first
    cols = export_df.columns.tolist()
    if 'LogMsg' in cols:
        cols.insert(0, cols.pop(cols.index('LogMsg')))
    export_df = export_df[cols]
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        export_df.to_excel(writer, index=False, sheet_name='Report')
        workbook = writer.book
        worksheet = writer.sheets['Report']

        header_format = workbook.add_format({'bold': True, 'bg_color': '#4F81BD', 'font_color': 'white'})
        green_format = workbook.add_format({'font_color': 'green'})
        red_format = workbook.add_format({'font_color': 'red'})

        for col_num, value in enumerate(export_df.columns):
            worksheet.write(0, col_num, value, header_format)
            max_width = max(export_df[value].astype(str).map(len).max(), len(value)) + 2
            worksheet.set_column(col_num, col_num, max_width)

        if 'NAV Status' in export_df.columns:
            col_index = export_df.columns.get_loc('NAV Status')
            for row_num, value in enumerate(export_df['NAV Status'], start=1):
                fmt = green_format if value == 'Posted' else red_format
                worksheet.write(row_num, col_index, value, fmt)
    output.seek(0)
    return output

def load_data(start_date, end_date):
    bcs_config = {
        'server': 'caappsdb,1435',
        'database': 'BCSSoft_ConAppSys',
        'username': 'Deepak',
        'password': 'Deepak@321',
        'driver': 'ODBC Driver 17 for SQL Server',
        'timeout': 30
    }

    nav_config = {
        'server': 'nav18db',
        'database': 'EDLIVE',
        'username': 'barcode1',
        'password': 'barcode@1433',
        'driver': 'ODBC Driver 17 for SQL Server',
        'timeout': 30
    }

    bcs_conn_str = create_conn_str(bcs_config)
    nav_conn_str = create_conn_str(nav_config)

    with st.spinner("Fetching CA Sales Order data..."):
        df_bcs = fetch_bcs_data(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), bcs_conn_str)

    if df_bcs.empty:
        st.warning("No sales order data found for the selected date range.")
        return None

    df_bcs['Document No_'] = df_bcs['SONo'].apply(sono_to_docno)
    doc_no_list = df_bcs['Document No_'].dropna().unique().tolist()

    with st.spinner(f"Checking {len(doc_no_list)} Document Nos in NAV..."):
        existing_docnos = fetch_existing_docnos(doc_no_list, nav_conn_str)

    df_bcs['NAV Status'] = df_bcs['Document No_'].apply(lambda doc: 'Posted' if doc in existing_docnos else 'Not Posted')
    return df_bcs

def main():
    st.set_page_config(layout="wide", page_title="CA Sales Order Error Report (NAV)")
    st.title("CA Sales Order Error Report (NAV)")

    today = datetime.today()
    default_start = today - relativedelta(months=3)

    # Create two equal columns for date inputs
    col1, col2 = st.columns(2)
    
    with col1:
        start_date = st.date_input("From Date", default_start)
        # Add Refresh Data button under From Date
        refresh_clicked = st.button("Refresh Data")
    
    with col2:
        end_date = st.date_input("To Date", today)
        # Move Download button under To Date
        if 'df_bcs' in st.session_state and st.session_state.df_bcs is not None:
            # Filter only "Not Posted" records for the download
            not_posted_df = st.session_state.df_bcs[st.session_state.df_bcs['NAV Status'] == 'Not Posted']
            if not not_posted_df.empty:
                excel_data = create_excel(not_posted_df)
                st.download_button(
                    label="📥 Download Excel Report",
                    data=excel_data,
                    file_name=f"CA_Sales_Order_Report_NAV_Not_Posted_{today.strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key='download_excel'
                )

    if start_date > end_date:
        st.error("From Date must be before or equal to To Date.")
        return

    # Load data when app starts or when refresh is clicked
    if refresh_clicked or 'df_bcs' not in st.session_state:
        df_bcs = load_data(start_date, end_date)
        st.session_state.df_bcs = df_bcs
    else:
        df_bcs = st.session_state.df_bcs

    if df_bcs is not None:
        # Filter to show only "Not Posted" records
        not_posted_df = df_bcs[df_bcs['NAV Status'] == 'Not Posted']
        
        if not_posted_df.empty:
            st.warning("No records with 'Not Posted' status found.")
            return

        # Prepare display dataframe
        display_df = not_posted_df.copy()
        columns_to_hide = ['LogTypeCode', 'SOHeaderId', 'SODetailId']
        for col in columns_to_hide:
            if col in display_df.columns:
                display_df.drop(col, axis=1, inplace=True)

        # Reorder columns to put LogMsg first
        cols = display_df.columns.tolist()
        if 'LogMsg' in cols:
            cols.insert(0, cols.pop(cols.index('LogMsg')))
        
        display_df = display_df[cols]

        # Reset index to start from 1
        display_df.reset_index(drop=True, inplace=True)
        display_df.index = display_df.index + 1

        st.success(f"Loaded {len(display_df)} records with 'Not Posted' status.")
        st.dataframe(display_df)

if __name__ == "__main__":
    main()