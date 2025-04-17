import streamlit as st
import pandas as pd
import sqlite3
import os

# ตั้งค่าชื่อไฟล์ Excel ที่จะใช้
file_path = "Agent Performance.xlsx"

# เช็คว่ามีไฟล์ Excel หรือไม่
if os.path.exists(file_path):
    df = pd.read_excel(file_path, engine='openpyxl')
else:
    st.error("ไม่พบไฟล์ข้อมูล กรุณาตรวจสอบ GitLab Pipeline")

# ตั้งค่าฐานข้อมูล SQLite สำหรับเก็บ Target และตารางงานพนักงาน
db_file = "agent_data.db"
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# สร้างตารางถ้ายังไม่มี
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

# ? แสดงข้อมูล Agent Performance
st.subheader("Agent Performance Data")
st.dataframe(df)

# ? ฟอร์มสำหรับแก้ไข Target ของพนักงาน
st.subheader("?? กำหนด Target ของพนักงาน")

# ดึงข้อมูลจาก SQLite
cursor.execute("SELECT * FROM agent_target")
target_data = cursor.fetchall()
target_df = pd.DataFrame(target_data, columns=["ID", "Agent Name", "Target"])
st.dataframe(target_df)

# ฟอร์มสำหรับเพิ่ม/แก้ไข Target
agent_name = st.text_input("ชื่อพนักงาน")
target_value = st.number_input("กำหนด Target", min_value=0)

if st.button("บันทึก Target"):
    cursor.execute("INSERT INTO agent_target (agent_name, target) VALUES (?, ?)", (agent_name, target_value))
    conn.commit()
    st.success("บันทึก Target เรียบร้อยแล้ว!")

# ? ฟอร์มสำหรับแก้ไขตารางงาน
st.subheader("?? แก้ไขตารางงานของพนักงาน")

cursor.execute("SELECT * FROM agent_schedule")
schedule_data = cursor.fetchall()
schedule_df = pd.DataFrame(schedule_data, columns=["ID", "Agent Name", "Shift"])
st.dataframe(schedule_df)

# ฟอร์มเพิ่ม/แก้ไขตารางงาน
agent_schedule_name = st.text_input("ชื่อพนักงาน (แก้ไขตารางงาน)")
shift_option = st.selectbox("เลือกกะงาน", ["เช้า", "บ่าย", "ดึก"])

if st.button("บันทึกตารางงาน"):
    cursor.execute("INSERT INTO agent_schedule (agent_name, shift) VALUES (?, ?)", (agent_schedule_name, shift_option))
    conn.commit()
    st.success("บันทึกตารางงานสำเร็จ!")

conn.close()
