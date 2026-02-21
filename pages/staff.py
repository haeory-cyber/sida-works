import streamlit as st
from supabase import create_client, Client

# 1. 수파베이스 연결 설정 (스트림릿 금고에서 열쇠 가져오기)
url: str = st.secrets["supabase"]["url"]
key: str = st.secrets["supabase"]["key"]
supabase: Client = create_client(url, key)

st.title("📝 현장 요청 입력 (수파베이스 연동)")

# 2. 직원용 입력 폼
with st.form("request_form", clear_on_submit=True):
    item_name = st.text_input("품목명 (예: 딸기, 상추)")
    farmer_name = st.text_input("농가명")
    urgency = st.selectbox("긴급도", ["보통", "긴급", "매우 긴급"])
    content = st.text_area("내용")

    submitted = st.form_submit_button("요청 추가")

    if submitted:
        # 3. 수파베이스 'staff_data' 표에 데이터 전송
        try:
            data, count = supabase.table("staff_data").insert({
                "item_name": item_name,
                "farmer_name": farmer_name,
                "urgency": urgency,
                "content": content
            }).execute()
            
            st.success("✅ 현장 요청이 수파베이스에 성공적으로 저장되었습니다!")
        except Exception as e:
            st.error(f"❌ 오류가 발생했습니다: {e}")
