import streamlit as st
import pandas as pd
import requests
from io import BytesIO

# ตั้งค่า GitLab API
GITLAB_PROJECT_ID = "12345678"  # ?? ใส่ Project ID ของคุณ
GITLAB_JOB_ID = "latest"  # ?? ใช้ "latest" เพื่อดึงจาก Pipeline ล่าสุด
FILE_PATH = "Evaluation_List_01-04-2025_08-00-00.xlsx"  # ?? ชื่อไฟล์ใน Artifacts
GITLAB_TOKEN = "your_personal_access_token"  # ?? ใส่ GitLab Token ของคุณ

# URL สำหรับดึง Artifacts
artifact_url = f"https://gitlab.com/api/v4/projects/{GITLAB_PROJECT_ID}/jobs/{GITLAB_JOB_ID}/artifacts/{FILE_PATH}"

# ดึงไฟล์จาก GitLab
st.title("?? Dashboard from GitLab Artifacts")
st.write("กำลังโหลดข้อมูลจาก GitLab...")

headers = {"PRIVATE-TOKEN": GITLAB_TOKEN}
response = requests.get(artifact_url, headers=headers)

if response.status_code == 200:
    st.success("? โหลดข้อมูลสำเร็จ!")
    
    # โหลดไฟล์ Excel จาก GitLab Artifacts
    data = BytesIO(response.content)
    df = pd.read_excel(data, engine="openpyxl")
    
    # แสดงข้อมูล
    st.dataframe(df)
else:
    st.error(f"? โหลดข้อมูลไม่สำเร็จ: {response.status_code}")
