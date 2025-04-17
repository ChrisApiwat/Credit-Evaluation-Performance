import streamlit as st
import pandas as pd
import sqlite3
import os

# ��駤�Ҫ������ Excel ������
file_path = "Agent Performance.xlsx"

# ���������� Excel �������
if os.path.exists(file_path):
    df = pd.read_excel(file_path, engine='openpyxl')
else:
    st.error("��辺�������� ��سҵ�Ǩ�ͺ GitLab Pipeline")

# ��駤�Ұҹ������ SQLite ����Ѻ�� Target ��е��ҧ�ҹ��ѡ�ҹ
db_file = "agent_data.db"
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# ���ҧ���ҧ����ѧ�����
cursor.execute("""
CREATE TABLE IF NOT EXISTS agent_target (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_name TEXT,
    target INTEGER
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS agent_schedule (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_name TEXT,
    shift TEXT
)
""")

conn.commit()

# UI Streamlit
st.title("?? Dashboard: Agent Performance")

# ? �ʴ������� Agent Performance
st.subheader("Agent Performance Data")
st.dataframe(df)

# ? ���������Ѻ��� Target �ͧ��ѡ�ҹ
st.subheader("?? ��˹� Target �ͧ��ѡ�ҹ")

# �֧�����Ũҡ SQLite
cursor.execute("SELECT * FROM agent_target")
target_data = cursor.fetchall()
target_df = pd.DataFrame(target_data, columns=["ID", "Agent Name", "Target"])
st.dataframe(target_df)

# ���������Ѻ����/��� Target
agent_name = st.text_input("���;�ѡ�ҹ")
target_value = st.number_input("��˹� Target", min_value=0)

if st.button("�ѹ�֡ Target"):
    cursor.execute("INSERT INTO agent_target (agent_name, target) VALUES (?, ?)", (agent_name, target_value))
    conn.commit()
    st.success("�ѹ�֡ Target ���º��������!")

# ? ���������Ѻ��䢵��ҧ�ҹ
st.subheader("?? ��䢵��ҧ�ҹ�ͧ��ѡ�ҹ")

cursor.execute("SELECT * FROM agent_schedule")
schedule_data = cursor.fetchall()
schedule_df = pd.DataFrame(schedule_data, columns=["ID", "Agent Name", "Shift"])
st.dataframe(schedule_df)

# ���������/��䢵��ҧ�ҹ
agent_schedule_name = st.text_input("���;�ѡ�ҹ (��䢵��ҧ�ҹ)")
shift_option = st.selectbox("���͡�Чҹ", ["���", "����", "�֡"])

if st.button("�ѹ�֡���ҧ�ҹ"):
    cursor.execute("INSERT INTO agent_schedule (agent_name, shift) VALUES (?, ?)", (agent_schedule_name, shift_option))
    conn.commit()
    st.success("�ѹ�֡���ҧ�ҹ�����!")

conn.close()
