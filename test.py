import streamlit as st
import pandas as pd

# �����ŵ�����ҧ
data = {
    'No.': [1, 2, 3],
    'Name/Surname': ['John Doe', 'Jane Smith', 'Alice Johnson'],
    'Staff ID': ['A123', 'B456', 'C789'],
    'Process Time Stamp 1': ['2025-03-18 08:00', '2025-03-18 08:30', '2025-03-18 09:00'],
    'Process Time Stamp 2': ['2025-03-18 08:15', '2025-03-18 08:45', '2025-03-18 09:15'],
    'Process Time Stamp 3': ['2025-03-18 08:30', '2025-03-18 09:00', '2025-03-18 09:30'],
    'Process Time Stamp 4': ['2025-03-18 08:45', '2025-03-18 09:15', '2025-03-18 09:45'],
    'Process Time Stamp 5': ['2025-03-18 09:00', '2025-03-18 09:30', '2025-03-18 10:00'],
    'Final Result': ['Pass', 'Pending', 'Fail']
}

df = pd.DataFrame(data)

# ���ҧ Dashboard
st.title('Daily Agent Performance')

# Gauges
st.subheader('Digital RL SLA 45 Mins')
st.progress(70)  # �����繵������ҧ

st.subheader('Manual TL SLA 1 Day')
st.progress(50)  # �����繵������ҧ

# Status Boxes
st.subheader('Status Overview')
st.write('Update Data: 8')
st.write('Send to Judgement: 70')
st.write('Approved: 70')
st.write('Pending: 3')
st.write('Rejected: 0')

# Tables
st.subheader('Agent Performance')
st.dataframe(df)

# Credit Evaluation Timestamp Table
st.subheader('Credit Evaluation Timestamp')
credit_data = {
    'Date': ['2025-03-18', '2025-03-18', '2025-03-18'],
    'Stage 1': ['08:00', '08:30', '09:00'],
    'Stage 2': ['08:15', '08:45', '09:15'],
    'Stage 3': ['08:30', '09:00', '09:30'],
    'Stage 4': ['08:45', '09:15', '09:45'],
    'Stage 5': ['09:00', '09:30', '10:00']
}
credit_df = pd.DataFrame(credit_data)
st.dataframe(credit_df)