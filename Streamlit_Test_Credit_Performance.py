import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import urllib

def fetch_data():
    connection_string = (
        "DRIVER={Tibero 6 ODBC Driver};"
        "SERVER=192.169.10.51;"
        "PORT=18629;"
        "UID=Apiwat;"
        "PWD=Apiw@2024;"
        "DB=DSTFCC"
    )
    connection_string = urllib.parse.quote_plus(connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}&driver=Tibero+6+ODBC+Driver")
    query = "SELECT * FROM CFNC_IVPG_L WHERE loan_noe = '9010220042000721'"
    with engine.connect() as conn:
        result = conn.execute(query)
        data = result.fetchall()
        columns = result.keys()
    df = pd.DataFrame(data, columns=columns)
    return df

# เรียกใช้ฟังก์ชันดึงข้อมูล
df = fetch_data()

# แสดงผลข้อมูลใน Streamlit
st.title('ข้อมูลจาก Tibero')
st.dataframe(df)