import streamlit as st
import pandas as pd
import requests
from io import BytesIO

# ��駤�� GitLab API
GITLAB_PROJECT_ID = "12345678"  # ?? ��� Project ID �ͧ�س
GITLAB_JOB_ID = "latest"  # ?? �� "latest" ���ʹ֧�ҡ Pipeline ����ش
FILE_PATH = "Evaluation_List_01-04-2025_08-00-00.xlsx"  # ?? �������� Artifacts
GITLAB_TOKEN = "your_personal_access_token"  # ?? ��� GitLab Token �ͧ�س

# URL ����Ѻ�֧ Artifacts
artifact_url = f"https://gitlab.com/api/v4/projects/{GITLAB_PROJECT_ID}/jobs/{GITLAB_JOB_ID}/artifacts/{FILE_PATH}"

# �֧���ҡ GitLab
st.title("?? Dashboard from GitLab Artifacts")
st.write("���ѧ��Ŵ�����Ũҡ GitLab...")

headers = {"PRIVATE-TOKEN": GITLAB_TOKEN}
response = requests.get(artifact_url, headers=headers)

if response.status_code == 200:
    st.success("? ��Ŵ�����������!")
    
    # ��Ŵ��� Excel �ҡ GitLab Artifacts
    data = BytesIO(response.content)
    df = pd.read_excel(data, engine="openpyxl")
    
    # �ʴ�������
    st.dataframe(df)
else:
    st.error(f"? ��Ŵ��������������: {response.status_code}")
