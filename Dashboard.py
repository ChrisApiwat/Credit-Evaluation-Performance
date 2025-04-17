import streamlit as st
import pandas as pd

st.set_page_config(page_title="Credit Performance Dashboard", layout="wide")

# ?? ��Ŵ��쾹ѡ�ҹ (Work Status)
uploaded_work = st.file_uploader("���ҧ�ҹ Credit Evaluation Mar'2025", type=["xlsx"], key="work")
uploaded_performance = st.file_uploader("?? �ѻ��Ŵ��� Performance", type=["xlsx"], key="performance")
uploaded_performance = st.file_uploader("?? �ѻ��Ŵ��� Performance", type=["xlsx"], key="performance")
uploaded_performance = st.file_uploader("?? �ѻ��Ŵ��� Performance", type=["xlsx"], key="performance")
uploaded_performance = st.file_uploader("?? �ѻ��Ŵ��� Performance", type=["xlsx"], key="performance")

if uploaded_work and uploaded_performance:
    # ?? ��Ŵ���ҧ��÷ӧҹ
    df_work = pd.read_excel(uploaded_work, dtype={"Staff ID": str})
    df_performance = pd.read_excel(uploaded_performance, dtype={"Staff ID": str})

    # ������������� Staff ID
    df = df_performance.merge(df_work, on="Staff ID", how="left")

    # �ŧʶҹ��繪��ͷ����ҹ����
    work_status_map = {
        "A04": "Shift",
        "A01": "Normal",
        "OFF": "Off",
        "AL": "Leave",
        "BL": "Sick Leave",
        "SL": "Special Leave"
    }
    df["Work Status"] = df["01/03/2025"].map(work_status_map)

    # ?? ��ͧ੾�о�ѡ�ҹ���ӧҹ���� (������ OFF, AL, BL, SL)
    df_active = df[df["Work Status"].isin(["Shift", "Normal", "Off", "Leave", "Sick Leave", "Special Leave"])]

    # ?? �ʴ��� Dashboard
    st.title("?? Credit Performance Dashboard")
    
    # �ѧ�������ͨѴ��áѺ��
    def color_work_status(val):
        color = 'black' if val in ['Off', 'Leave', 'Sick Leave', 'Special Leave'] else 'black'  # �մ�����Ѻ�������ش�ҹ
        return f'color: {color}'

    # ������ҧ
    styled_df = df_active.style.applymap(color_work_status, subset=["Work Status"])

    # �ʴ����ҧ
    st.subheader("?? ���ҧ��ѡ�ҹ���ӧҹ")
    st.dataframe(styled_df)

    # ?? ��ͧ�����ŵ����ѡ�ҹ
    staff_options = ["������"] + list(df_active["Name+Surname"].unique())
    staff_filter = st.selectbox("?? ���͡��ѡ�ҹ", staff_options)

    # ?? ��ͧ�����ŵ�� Staff ID
    staff_id_options = ["������"] + list(df_active["Staff ID"].astype(str).unique())
    staff_id_filter = st.selectbox("?? ���͡ Staff ID", staff_id_options)

    # ��ͧ������
    if staff_filter != "������":
        df_active = df_active[df_active["Name+Surname"] == staff_filter]

    if staff_id_filter != "������":
        df_active = df_active[df_active["Staff ID"] == staff_id_filter]

    # ?? �ʴ����ҧ�����ž�ѡ�ҹ���ӧҹ��ҹ��
    st.subheader("?? ���ҧ��ѡ�ҹ���ӧҹ")
    st.dataframe(df_active)

    # ?? ��ػ�Ţ�����
    st.subheader("?? ��ػ��")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("?? �ʹ͹��ѵԷ�����", df_active["Approved"].sum())
    col2.metric("? �ʹ¡��ԡ", df_active["Cancel"].sum())
    col3.metric("?? �ʹ��Թ���", df_active["Pending"].sum())
    col4.metric("? �١����ʸ", df_active["Rejected"].sum())

else:
    st.warning("?? ��س��ѻ��Ŵ����� 2 ���������ҹ Dashboard")
