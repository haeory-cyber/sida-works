import streamlit as st
import pandas as pd
import os
from datetime import datetime

# 데이터 저장용 CSV 파일 이름
FILE_PATH = 'requests.csv'

st.title("📝 현장 요청 입력")

# 직원용 입력 폼
with st.form("request_form", clear_on_submit=True):
    item_name = st.text_input("품목명 (예: 딸기, 상추)")
    farmer_name = st.text_input("농가명")
    urgency = st.selectbox("긴급도", ["보통", "긴급", "매우 긴급"])
    memo = st.text_area("메모")
    
    submitted = st.form_submit_button("요청 추가")
    
    if submitted:
        if item_name and farmer_name:
            # 입력된 데이터를 데이터프레임으로 변환
            new_data = pd.DataFrame([{
                "일시": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "품목명": item_name,
                "농가명": farmer_name,
                "긴급도": urgency,
                "메모": memo,
                "상태": "대기중"
            }])
            
            # CSV 파일이 없으면 새로 만들고, 있으면 아래에 추가
            if not os.path.exists(FILE_PATH):
                new_data.to_csv(FILE_PATH, index=False, encoding='utf-8-sig')
            else:
                new_data.to_csv(FILE_PATH, mode='a', header=False, index=False, encoding='utf-8-sig')
            
            st.success("✅ 현장 요청이 저장되었습니다.")
        else:
            st.warning("⚠️ 품목명과 농가명은 필수 입력입니다.")
