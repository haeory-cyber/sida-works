import streamlit as st
import pandas as pd
import io
import os
import re
import time
import hmac
import hashlib
import uuid
import datetime
import requests
import numpy as np
import plotly.express as px

# ==========================================
# [설정] 서버 파일 경로
# ==========================================
SERVER_CONTACT_FILE = "농가관리 목록_20260208 (전체).xlsx"
SERVER_MEMBER_FILE = "회원관리(전체).xlsx"

# ==========================================
# 0. [공통 함수 및 세션]
# ==========================================
if 'sms_history' not in st.session_state: st.session_state.sms_history = []

def send_coolsms_direct(api_key, api_secret, sender, receiver, text):
    try:
        clean_receiver = re.sub(r'[^0-9]', '', str(receiver))
        clean_sender = re.sub(r'[^0-9]', '', str(sender))
        if not clean_receiver or not clean_sender: return False, {"errorMessage": "번호 오류"}
        date = datetime.datetime.now(datetime.timezone.utc).isoformat()
        salt = str(uuid.uuid4())
        data = date + salt
        signature = hmac.new(api_secret.encode('utf-8'), data.encode('utf-8'), hashlib.sha256).hexdigest()
        headers = {"Authorization": f"HMAC-SHA256 apiKey={api_key}, date={date}, salt={salt}, signature={signature}", "Content-Type": "application/json"}
        res = requests.post("https://api.coolsms.co.kr/messages/v4/send",
                            json={"message": {"to": clean_receiver, "from": clean_sender, "text": text}},
                            headers=headers)
        if res.status_code == 200: return True, res.json()
        else: return False, res.json()
    except Exception as e: return False, {"errorMessage": str(e)}

def send_and_log(sender_name, receiver_phone, msg_text):
    if not st.session_state.api_key:
        st.error("API Key가 없습니다.")
        return False
    ok, res = send_coolsms_direct(
        st.session_state.api_key, st.session_state.api_secret,
        st.session_state.sender_number, receiver_phone, msg_text
    )
    now_str = datetime.datetime.now().strftime("%H:%M:%S")
    st.session_state.sms_history.insert(0, {
        "시간": now_str, "수신자": sender_name, "번호": receiver_phone,
        "결과": "✅ 성공" if ok else "❌ 실패",
        "비고": "" if ok else res.get("errorMessage", str(res))
    })
    return ok

def clean_phone_number(phone):
    if pd.isna(phone) or str(phone).strip() in ['-', '', 'nan']: return ''
    clean_num = re.sub(r'[^0-9]', '', str(phone))
    if clean_num.startswith('10') and len(clean_num) >= 10: clean_num = '0' + clean_num
    return clean_num

@st.cache_data
def load_data_smart(file_obj, type='sales'):
    if file_obj is None: return None, "파일 없음"
    df_raw = None
    try: df_raw = pd.read_excel(file_obj, header=None, engine='openpyxl')
    except:
        try:
            if hasattr(file_obj, 'seek'): file_obj.seek(0)
            df_raw = pd.read_csv(file_obj, header=None, encoding='utf-8')
        except: return None, "읽기 실패"
    target_row_idx = -1
    keywords = ['농가', '공급자', '생산자', '상품', '품목'] if type == 'sales' else \
               ['회원번호', '이름', '휴대전화'] if type == 'member' else ['농가명', '휴대전화', '전화번호']
    for idx, row in df_raw.head(20).iterrows():
        if sum(1 for k in keywords if k in row.astype(str).str.cat(sep=' ')) >= 2:
            target_row_idx = idx; break
    if target_row_idx != -1:
        df_final = df_raw.iloc[target_row_idx+1:].copy()
        df_final.columns = df_raw.iloc[target_row_idx]
        df_final.columns = df_final.columns.astype(str).str.replace(' ', '').str.replace('\n', '')
        df_final = df_final.loc[:, ~df_final.columns.str.contains('^Unnamed')]
        return df_final, None
    else:
        try:
            if hasattr(file_obj, 'seek'): file_obj.seek(0)
            return pd.read_excel(file_obj) if (hasattr(file_obj, 'name') and file_obj.name.endswith('xlsx')) else pd.read_csv(file_obj), "헤더 못 찾음"
        except: return df_raw, "헤더 못 찾음"

def to_clean_number(x):
    try:
        clean_str = re.sub(r'[^0-9.-]', '', str(x))
        return float(clean_str) if clean_str not in ['', '.'] else 0
    except: return 0

def detect_columns(df_columns):
    s_item = next((c for c in df_columns if any(x in c for x in ['상품', '품목'])), None)
    s_qty = next((c for c in df_columns if any(x in c for x in ['판매수량', '수량', '개수'])), None)
    exclude = ['할인', '반품', '취소', '면세', '과세', '부가세']
    candidates = [c for c in df_columns if ('총' in c and ('판매' in c or '매출' in c))] + \
                 [c for c in df_columns if (('판매' in c or '매출' in c) and ('액' in c or '금액' in c))] + \
                 [c for c in df_columns if '금액' in c]
    s_amt = next((c for c in candidates if not any(bad in c for bad in exclude)), None)
    s_farmer = next((c for c in df_columns if any(x in c for x in ['공급자', '농가', '생산자', '거래처'])), None)
    s_spec = next((c for c in df_columns if any(x in c for x in ['규격', '단위', '중량', '용량'])), None)
    return s_item, s_qty, s_amt, s_farmer, s_spec

def to_excel_bytes(df):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return buf.getvalue()

VALID_SUPPLIERS = [
    "(주)가보트레이딩", "(주)열두달", "(주)우리밀", "(주)윈윈농수산", "(주)유기샘",
    "(주)케이푸드", "(주)한누리", "G1상사", "mk코리아", "가가호영어조합법인",
    "고삼농협", "금강향수", "나우푸드", "네니아", "농부생각", "농업회사법인(주)담채원",
    "당암tf", "더테스트키친", "도마령영농조합법인", "두레생협", "또또푸드", "로엘팩토리",
    "맛가마", "산백유통", "새롬식품", "생수콩나물영농조합법인", "슈가랩", "씨글로벌(아라찬)",
    "씨에이치하모니", "언니들공방", "에르코스", "엔젤농장", "우리밀농협", "우신영농조합",
    "유기농산", "유안컴퍼니", "인터뷰베이커리", "자연에찬", "장수이야기", "제로웨이스트존",
    "청양농협조합", "청오건강농업회사법인", "청춘농장", "코레드인터내쇼날", "태경F&B",
    "토종마을", "폴카닷(이은경)", "하대목장", "한산항아리소곡주", "함지박(주)", "행복우리식품영농조합"
]

# ==========================================
# 메인 화면
# ==========================================
st.set_page_config(page_title="시다 워크", page_icon="🤖", layout="wide",
                   initial_sidebar_state="collapsed")  # 모바일: 사이드바 기본 접힘

if 'sent_history' not in st.session_state: st.session_state.sent_history = set()

# ── secrets.toml 우선, 없으면 세션값 사용 ──
def get_secret(key, fallback=''):
    try: return st.secrets.get(key, fallback)
    except: return fallback

if 'api_key' not in st.session_state: st.session_state.api_key = get_secret('SOLAPI_API_KEY')
if 'api_secret' not in st.session_state: st.session_state.api_secret = get_secret('SOLAPI_API_SECRET')
if 'sender_number' not in st.session_state: st.session_state.sender_number = get_secret('SENDER_NUMBER')

# ── 모바일 친화 CSS ──
st.markdown("""
<style>
/* 버튼 크게 */
div.stButton > button {
    height: 3.2rem;
    font-size: 1.1rem;
    font-weight: 700;
    border-radius: 12px;
}
/* 전체 여백 */
.block-container { padding-top: 3rem; padding-bottom: 1rem; }
/* 텍스트 입력 크게 */
input, textarea { font-size: 1rem !important; }
/* 탭 크게 */
.stTabs [data-baseweb="tab"] { font-size: 1rem; padding: 0.6rem 1rem; }
/* 메트릭 크게 */
[data-testid="metric-container"] { font-size: 1.1rem; }
</style>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("## 🤖 시다 워크")
    st.caption("Ver 23.0")
    st.divider()
    password = st.text_input("비밀번호", type="password")
    if password != "poom0118**":
        st.warning("비밀번호를 입력하세요.")
        st.stop()
    st.success("인증 완료")
    st.divider()
    st.markdown("**🔑 솔라피 설정**")
    st.caption("secrets.toml에 저장하면 자동입력")
    st.session_state.api_key = st.text_input("API Key", value=st.session_state.api_key, type="password")
    st.session_state.api_secret = st.text_input("API Secret", value=st.session_state.api_secret, type="password")
    st.session_state.sender_number = st.text_input("발신번호 (숫자만)", value=st.session_state.sender_number)
    # secrets 저장 안내
    if not get_secret('SOLAPI_API_KEY'):
        st.info("💡 GitHub → Settings → Secrets에\nSOLAPI_API_KEY / SOLAPI_API_SECRET / SENDER_NUMBER 저장하면 자동입력")
    st.divider()
    with st.expander("📋 문자 전송 이력", expanded=True):
        if st.session_state.sms_history:
            log_df = pd.DataFrame(st.session_state.sms_history)
            st.dataframe(log_df, hide_index=True, use_container_width=True)
            # 이력 엑셀 다운로드
            st.download_button("📥 이력 엑셀", data=to_excel_bytes(log_df),
                               file_name=f"발송이력_{datetime.datetime.now().strftime('%m%d_%H%M')}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            if st.button("이력 초기화"):
                st.session_state.sms_history = []; st.rerun()
        else:
            st.caption("아직 전송 내역이 없습니다.")

st.title("🤖 시다 워크")
menu = st.radio("", ["📦 발주", "♻️ 제로웨이스트", "📢 이음(마케팅)"], horizontal=True)

# ==========================================
# 📦 발주 탭
# ==========================================
if menu == "📦 발주":
    with st.container(border=True):
        c1, c2, c3, c4 = st.columns(4)
        budget = c1.number_input("💰 예산(원)", value=500000, step=10000)
        safety = c2.slider("안전계수", 1.0, 1.5, 1.1, step=0.1)
        purchase_rate = c3.slider("원가율(%)", 10, 100, 70, step=5) / 100.0
        show_all_data = c4.checkbox("미등록 포함")

    with st.expander("📂 판매 실적 업로드", expanded=True):
        up_sales_list = st.file_uploader("판매 실적 파일", type=['xlsx', 'csv'], accept_multiple_files=True, key='ord_up')

    df_phone_map = pd.DataFrame()
    if os.path.exists(SERVER_CONTACT_FILE):
        try:
            with open(SERVER_CONTACT_FILE, "rb") as f: df_i, _ = load_data_smart(f, 'info')
            if df_i is not None:
                i_name = next((c for c in df_i.columns if '농가명' in c), None)
                i_phone = next((c for c in df_i.columns if '휴대전화' in c or '전화' in c), None)
                if i_name and i_phone:
                    df_i['clean_name'] = df_i[i_name].astype(str).str.replace(' ', '')
                    df_i['clean_phone'] = df_i[i_phone].apply(clean_phone_number)
                    df_phone_map = df_i.drop_duplicates(subset=['clean_name'])[['clean_name', 'clean_phone']]
        except: pass

    df_s = None
    if up_sales_list:
        df_list = []
        for f in up_sales_list:
            d, _ = load_data_smart(f, 'sales')
            if d is not None: df_list.append(d)
        if df_list: df_s = pd.concat(df_list, ignore_index=True)

    if df_s is not None:
        st.divider()
        s_item, s_qty, s_amt, s_farmer, s_spec = detect_columns(df_s.columns.tolist())
        if s_item and s_qty and s_amt:
            def normalize_vendor(name):
                n = str(name).replace(' ', '')
                if '지족' in n and '야채' in n: return '지족점야채'
                if '지족' in n and '과일' in n: return '지족점과일'
                if '지족' in n and '정육' in n: return '지족점정육'
                if '지족' in n and '공동' in n: return '지족점_공동구매'
                if '지족' in n and '매장' in n: return '지족매장'
                return re.sub(r'\(?벌크\)?', '', n)

            if s_farmer:
                valid_set = {v.replace(' ', '') for v in VALID_SUPPLIERS}
                df_s['clean_farmer'] = df_s[s_farmer].apply(normalize_vendor)
                df_s[s_farmer] = df_s['clean_farmer']
                def classify(name):
                    clean = name.replace(' ', '')
                    if "지족(Y)" in name or "지족(y)" in name: return "제외"
                    if "지족" in clean: return "지족(사입)"
                    elif clean in valid_set: return "일반업체"
                    else: return "제외" if not show_all_data else "일반업체(강제)"
                df_s['구분'] = df_s['clean_farmer'].apply(classify)
                df_target = df_s[df_s['구분'] != "제외"].copy()
                if not df_phone_map.empty:
                    df_target = pd.merge(df_target, df_phone_map, left_on='clean_farmer', right_on='clean_name', how='left')
                    df_target.rename(columns={'clean_phone': '전화번호'}, inplace=True)
                else: df_target['전화번호'] = ''
            else:
                df_target = df_s.copy()
                df_target['구분'] = "일반업체"

            df_target[s_qty] = df_target[s_qty].apply(to_clean_number)
            df_target[s_amt] = df_target[s_amt].apply(to_clean_number)
            df_target.loc[(df_target[s_qty] <= 0) & (df_target[s_amt] > 0), s_qty] = 1

            def extract_kg(text):
                text = str(text).lower().replace(' ', '')
                m = re.search(r'([\d\.]+)(kg)', text)
                if m:
                    try: return float(m.group(1))
                    except: pass
                m = re.search(r'([\d\.]+)(g)', text)
                if m:
                    try: return float(m.group(1)) / 1000.0
                    except: pass
                return 0.0

            if s_item:
                def calc_unit_weight(row):
                    w = 0.0
                    if s_spec and pd.notna(row.get(s_spec)): w = extract_kg(row[s_spec])
                    if w == 0 and pd.notna(row.get(s_item)): w = extract_kg(row[s_item])
                    return w
                df_target['__unit_kg'] = df_target.apply(calc_unit_weight, axis=1)
                df_target['__total_kg'] = df_target['__unit_kg'] * df_target[s_qty]
                def make_display_name(x):
                    s = str(x).replace('*', '')
                    return re.sub(r'\(\s*[\d\.]+\s*(?:g|kg|G|KG)\s*\)', '', s).replace('()', '').strip().replace(' ', '')
                def make_parent_name(x):
                    s = str(x).replace('*', '')
                    s = re.sub(r'\(?벌크\)?', '', s)
                    s = re.sub(r'\(?bulk\)?', '', s, flags=re.IGNORECASE)
                    return re.sub(r'\(\s*[\d\.]+\s*(?:g|kg|G|KG)\s*\)', '', s).replace('()', '').strip().replace(' ', '')
                df_target['__display_name'] = df_target[s_item].apply(make_display_name)
                df_target['__clean_parent'] = df_target[s_item].apply(make_parent_name)

            agg_disp = df_target.groupby([s_farmer, '__display_name', '구분', '__clean_parent']).agg(
                {s_qty: 'sum', s_amt: 'sum', '__total_kg': 'sum'}).reset_index()
            if not df_phone_map.empty and s_farmer:
                agg_disp['clean_farmer'] = agg_disp[s_farmer].astype(str).str.replace(' ', '')
                agg_disp = pd.merge(agg_disp, df_phone_map, left_on='clean_farmer', right_on='clean_name', how='left')
                agg_disp.rename(columns={'clean_phone': '전화번호'}, inplace=True)
            else: agg_disp['전화번호'] = ''
            agg_disp.rename(columns={s_farmer: '업체명', '__display_name': '상품명', s_qty: '판매량', s_amt: '총판매액'}, inplace=True)
            agg_disp = agg_disp[agg_disp['총판매액'] > 0].sort_values(by=['업체명', '__clean_parent', '상품명'])
            agg_disp['발주_수량'] = np.ceil(agg_disp['판매량'] * safety)
            agg_disp['발주_중량'] = np.ceil(agg_disp['__total_kg'] * safety)

            # ── 엑셀 다운로드 버튼 ──
            dl_cols = ['업체명', '상품명', '판매량', '총판매액', '발주_수량', '발주_중량', '전화번호']
            st.download_button("📥 발주서 엑셀 다운로드", data=to_excel_bytes(agg_disp[dl_cols]),
                               file_name=f"발주서_{datetime.datetime.now().strftime('%m%d')}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)

            tab1, tab2 = st.tabs(["🏢 외부업체", "🏪 지족 사입"])

            def generate_sms_text(df_source):
                grouped = df_source.groupby('__clean_parent').agg({'발주_수량': 'sum', '발주_중량': 'sum', '__total_kg': 'sum'}).reset_index()
                return [f"- {r['__clean_parent']}: {int(r['발주_중량'])}kg" if r['__total_kg'] > 0 else f"- {r['__clean_parent']}: {int(r['발주_수량'])}개"
                        for _, r in grouped.iterrows()]

            with tab1:
                df_ext = agg_disp[agg_disp['구분'].isin(["일반업체", "일반업체(강제)"])].copy()
                if df_ext.empty: st.info("데이터 없음")
                else:
                    search = st.text_input("🔍 업체명 검색", key="s_ext")
                    targets = [v for v in sorted(df_ext['업체명'].unique()) if search in v] if search else sorted(df_ext['업체명'].unique())
                    for vendor in targets:
                        is_sent = vendor in st.session_state.sent_history
                        v_data = df_ext[df_ext['업체명'] == vendor]
                        default_msg = "\n".join([f"[{vendor} 발주]"] + generate_sms_text(v_data) + ["잘 부탁드립니다!"])
                        with st.expander(f"{'✅' if is_sent else '📩'} {vendor}", expanded=not is_sent):
                            st.dataframe(v_data[['상품명', '판매량', '총판매액']], hide_index=True, use_container_width=True)
                            c1, c2 = st.columns([1, 2])
                            with c1:
                                phone = str(v_data['전화번호'].iloc[0]) if not pd.isna(v_data['전화번호'].iloc[0]) else ''
                                in_phone = st.text_input("📞 번호", value=phone, key=f"p_ext_{vendor}", label_visibility="collapsed")
                                if st.button(f"🚀 발송", key=f"b_ext_{vendor}", type="primary", use_container_width=True):
                                    ok = send_and_log(vendor, clean_phone_number(in_phone), st.session_state.get(f"m_ext_{vendor}", default_msg))
                                    if ok: st.session_state.sent_history.add(vendor); st.success("✅"); time.sleep(1); st.rerun()
                                    else: st.error("❌ 실패")
                            with c2:
                                st.text_area("내용", value=default_msg, height=180, key=f"m_ext_{vendor}", label_visibility="collapsed")

            with tab2:
                df_int = agg_disp[agg_disp['구분'] == "지족(사입)"].copy()
                if df_int.empty: st.info("지족 사입 데이터가 없습니다.")
                else:
                    for main_vendor in ["지족점야채", "지족점과일", "지족매장", "지족점정육", "지족점_공동구매"]:
                        df_m = df_int[df_int['업체명'] == main_vendor]
                        if df_m.empty: continue
                        is_sent = main_vendor in st.session_state.sent_history
                        with st.expander(f"{'✅' if is_sent else '🚚'} {main_vendor} ({df_m['총판매액'].sum():,.0f}원)", expanded=not is_sent):
                            d_show = df_m.copy()
                            d_show['발주'] = d_show.apply(lambda x: f"{int(x['발주_중량'])}kg" if x['__total_kg'] > 0 else f"{int(x['발주_수량'])}개", axis=1)
                            st.dataframe(d_show[['상품명', '발주', '총판매액']].assign(총판매액=d_show['총판매액'].apply(lambda x: f"{x:,.0f}")),
                                         hide_index=True, use_container_width=True)
                            default_msg = "\n".join([f"안녕하세요 {main_vendor}입니다.", "", "[발주 요청]"] + generate_sms_text(df_m) + ["", "잘 부탁드립니다."])
                            c1, c2 = st.columns([1, 2])
                            with c1:
                                ph = str(df_m['전화번호'].iloc[0]) if not pd.isna(df_m['전화번호'].iloc[0]) else ''
                                in_phone = st.text_input("📞 번호", value=ph, key=f"p_v10_{main_vendor}", label_visibility="collapsed")
                                if st.button(f"🚀 발송", key=f"b_v10_{main_vendor}", type="primary", use_container_width=True):
                                    ok = send_and_log(main_vendor, clean_phone_number(in_phone), st.session_state.get(f"m_v10_{main_vendor}", default_msg))
                                    if ok: st.session_state.sent_history.add(main_vendor); st.success("✅"); time.sleep(1); st.rerun()
                                    else: st.error("❌ 실패")
                            with c2:
                                st.text_area("내용", value=default_msg, height=350, key=f"m_v10_{main_vendor}", label_visibility="collapsed")

# ==========================================
# ♻️ 제로웨이스트 탭
# ==========================================
elif menu == "♻️ 제로웨이스트":
    st.markdown("### ♻️ 제로웨이스트 판매 분석")
    st.info("💡 라벨에 '벌크'가 찍힌 상품(무포장) vs 소포장 자동 구분")
    with st.expander("📂 판매 데이터 업로드", expanded=True):
        up_zw_list = st.file_uploader("판매 실적 파일", type=['xlsx', 'csv'], accept_multiple_files=True, key='zw_up')
    if up_zw_list:
        df_list = []
        for f in up_zw_list:
            d, _ = load_data_smart(f, 'sales')
            if d is not None: df_list.append(d)
        if df_list:
            df_zw = pd.concat(df_list, ignore_index=True)
            s_item, s_qty, s_amt, s_farmer, s_spec = detect_columns(df_zw.columns.tolist())
            if s_item and s_amt:
                def get_parent_zw(x):
                    s = str(x)
                    s = re.sub(r'\(?벌크\)?', '', s)
                    s = re.sub(r'\(?bulk\)?', '', s, flags=re.IGNORECASE)
                    return re.sub(r'\(.*?\)', '', s).replace('*', '').replace('()', '').strip().replace(' ', '')
                df_zw['__parent'] = df_zw[s_item].apply(get_parent_zw)
                df_zw[s_amt] = df_zw[s_amt].apply(to_clean_number)
                def get_type_tag(row):
                    i_name = str(row[s_item])
                    f_name = str(row[s_farmer]) if s_farmer and pd.notna(row[s_farmer]) else ""
                    if '벌크' in i_name or 'bulk' in i_name.lower() or '벌크' in f_name: return '벌크(무포장)'
                    return '일반(포장)'
                df_zw['__type'] = df_zw.apply(get_type_tag, axis=1)
                grp = df_zw.groupby(['__parent', '__type'])[s_amt].sum().reset_index()
                parents_with_bulk = grp[grp['__type'] == '벌크(무포장)']['__parent'].unique()
                target_df = grp[grp['__parent'].isin(parents_with_bulk)].copy()
                st.divider()
                if len(parents_with_bulk) == 0:
                    st.info("현재 '벌크(무포장)'로 분류된 데이터가 없습니다.")
                else:
                    # 엑셀 다운로드
                    st.download_button("📥 분석결과 엑셀", data=to_excel_bytes(target_df),
                                       file_name=f"제로웨이스트_{datetime.datetime.now().strftime('%m%d')}.xlsx",
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    st.markdown(f"**총 {len(parents_with_bulk)}개 품목 벌크 판매 비교**")
                    cols = st.columns(2)
                    for i, parent in enumerate(sorted(target_df['__parent'].unique())):
                        subset = target_df[target_df['__parent'] == parent]
                        fig = px.pie(subset, values=s_amt, names='__type', title=f"<b>{parent}</b>", hole=0.4,
                                     color='__type', color_discrete_map={'벌크(무포장)': '#28a745', '일반(포장)': '#dc3545'})
                        fig.update_layout(showlegend=True, height=280, margin=dict(t=40, b=0, l=0, r=0))
                        with cols[i % 2]: st.plotly_chart(fig, use_container_width=True)
            else:
                st.error("데이터 형식을 확인할 수 없습니다.")

# ==========================================
# 📢 마케팅 탭
# ==========================================
elif menu == "📢 이음(마케팅)":
    tab_m0, tab_m1, tab_m2 = st.tabs(["⚡ 특가 긴급발송", "🎯 판매 기반 타겟팅", "🔍 회원 직접 검색"])

    # ── ⚡ 특가 긴급발송 ──
    with tab_m0:
        st.markdown("### ⚡ 생산자 특가 → 단골 즉시 발송")
        st.caption("구글시트 '단골_매칭' → 파일 → CSV 다운로드 후 업로드")
        up_loyal = st.file_uploader("단골_매칭 CSV / Excel", type=['csv', 'xlsx'], key='loyal_up')
        if up_loyal:
            try:
                df_loyal = pd.read_csv(up_loyal, encoding='utf-8-sig') if up_loyal.name.endswith('.csv') else pd.read_excel(up_loyal, engine='openpyxl')
                df_loyal.columns = df_loyal.columns.astype(str).str.strip()
                c_farmer = next((c for c in df_loyal.columns if '농가' in c), None)
                c_item   = next((c for c in df_loyal.columns if '품목' in c), None)
                c_phone  = next((c for c in df_loyal.columns if '연락처' in c or '전화' in c), None)
                c_cnt    = next((c for c in df_loyal.columns if '횟수' in c or '구매' in c), None)
                if not c_farmer or not c_phone:
                    st.error("농가명 / 연락처 컬럼을 찾을 수 없습니다.")
                else:
                    sel_farmer = st.selectbox("📦 농가 선택", sorted(df_loyal[c_farmer].dropna().unique().tolist()), key='loyal_farmer')
                    df_t = df_loyal[df_loyal[c_farmer] == sel_farmer].copy()
                    df_t['__phone'] = df_t[c_phone].apply(clean_phone_number)
                    df_valid = df_t[df_t['__phone'] != ''].reset_index(drop=True)
                    items_str = ', '.join(df_t[c_item].dropna().unique().tolist()) if c_item else ''
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.metric("발송 대상", f"{len(df_valid)}명")
                        if c_cnt: st.metric("평균 구매횟수", f"{df_t[c_cnt].apply(to_clean_number).mean():.1f}회")
                    with col2:
                        if items_str: st.info(f"📋 {items_str}")
                    st.divider()
                    default_msg = f"안녕하세요, 품앗이생협입니다 😊\n{sel_farmer}의 {items_str} 특가 안내드립니다!\n\n자세한 내용은 지족점으로 문의 주세요."
                    msg_input = st.text_area("📝 발송 메시지", value=default_msg, height=150, key='loyal_msg')
                    st.caption(f"💬 {len(msg_input)}자 {'⚠️ 90자 초과 (장문 요금)' if len(msg_input) > 90 else '✅ 단문'}")
                    with st.expander("👥 발송 대상 미리보기"):
                        st.dataframe(df_valid[[c for c in [c_farmer, c_item, c_phone, c_cnt] if c]].head(20), hide_index=True, use_container_width=True)
                    # 엑셀 다운로드
                    st.download_button("📥 대상자 엑셀", data=to_excel_bytes(df_valid[[c for c in [c_farmer, c_item, c_phone, c_cnt] if c]]),
                                       file_name=f"발송대상_{sel_farmer}_{datetime.datetime.now().strftime('%m%d')}.xlsx",
                                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    st.divider()
                    if st.button(f"🚀 {len(df_valid)}명에게 즉시 발송", type="primary", use_container_width=True, key='loyal_send'):
                        if not st.session_state.api_key: st.error("사이드바에 API Key를 입력해주세요.")
                        elif not msg_input.strip(): st.error("메시지를 입력해주세요.")
                        else:
                            bar = st.progress(0)
                            success, fail = 0, 0
                            for i in range(len(df_valid)):
                                ok = send_and_log(
                                    str(df_valid.iloc[i][c_item]) if c_item else sel_farmer,
                                    df_valid.iloc[i]['__phone'], msg_input)
                                if ok: success += 1
                                else: fail += 1
                                bar.progress((i + 1) / len(df_valid))
                                time.sleep(0.3)
                            st.success(f"✅ 완료! 성공 {success}명 / 실패 {fail}명")
            except Exception as e:
                st.error(f"파일 읽기 오류: {e}")
        else:
            st.info("💡 구글시트 '단골_매칭' → 파일 → 다운로드 → CSV 저장 후 업로드")

    # ── 🎯 판매 기반 타겟팅 ──
    with tab_m1:
        with st.expander("📂 타겟팅용 판매 데이터 업로드", expanded=True):
            up_mkt_sales = st.file_uploader("판매내역", type=['xlsx', 'csv'], key='mkt_s')
        df_ms, _ = load_data_smart(up_mkt_sales, 'sales')
        df_mm = None
        if os.path.exists(SERVER_MEMBER_FILE):
            try:
                with open(SERVER_MEMBER_FILE, "rb") as f: df_mm, _ = load_data_smart(f, 'member')
            except: pass
        final_df = pd.DataFrame()
        if df_ms is not None:
            ms_farmer = next((c for c in df_ms.columns if any(x in c for x in ['농가', '공급자'])), None)
            ms_item   = next((c for c in df_ms.columns if any(x in c for x in ['상품', '품목'])), None)
            ms_buyer  = next((c for c in df_ms.columns if any(x in c for x in ['회원', '구매자'])), None)
            if ms_farmer and ms_buyer:
                sel_farmer = st.selectbox("농가 선택", sorted(df_ms[ms_farmer].astype(str).unique()))
                target_df = df_ms[df_ms[ms_farmer] == sel_farmer]
                if ms_item:
                    sel_item = st.selectbox("상품 선택", ["전체"] + sorted(target_df[ms_item].astype(str).unique()))
                    if sel_item != "전체": target_df = target_df[target_df[ms_item] == sel_item]
                loyal = target_df.groupby(ms_buyer).size().reset_index(name='구매횟수').sort_values('구매횟수', ascending=False)
                if df_mm is not None:
                    mm_name  = next((c for c in df_mm.columns if any(x in c for x in ['이름', '회원명'])), None)
                    mm_phone = next((c for c in df_mm.columns if any(x in c for x in ['휴대전화', '전화'])), None)
                    if mm_name and mm_phone:
                        loyal['key'] = loyal[ms_buyer].astype(str).str.replace(' ', '')
                        df_mm['key'] = df_mm[mm_name].astype(str).str.replace(' ', '')
                        final_df = pd.merge(loyal, df_mm.drop_duplicates(subset=['key']), on='key', how='left')[[ms_buyer, mm_phone, '구매횟수']]
                        final_df.columns = ['이름', '전화번호', '구매횟수']
        if not final_df.empty:
            st.divider()
            st.write(f"수신자: {len(final_df)}명")
            st.download_button("📥 대상자 엑셀", data=to_excel_bytes(final_df),
                               file_name=f"타겟팅_{datetime.datetime.now().strftime('%m%d')}.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            msg_txt = st.text_area("보낼 내용", key='mkt_msg')
            if st.button("🚀 전체 발송", type="primary", use_container_width=True, key='mkt_send'):
                if not st.session_state.api_key: st.error("API Key 필요")
                else:
                    bar = st.progress(0)
                    for i, r in enumerate(final_df.itertuples()):
                        send_and_log(r.이름, r.전화번호, msg_txt)
                        bar.progress((i+1)/len(final_df))
                    st.success("발송 완료!")

    # ── 🔍 회원 직접 검색 ──
    with tab_m2:
        df_mm2 = None
        if os.path.exists(SERVER_MEMBER_FILE):
            try:
                with open(SERVER_MEMBER_FILE, "rb") as f: df_mm2, _ = load_data_smart(f, 'member')
            except: pass
        if df_mm2 is not None:
            search_k = st.text_input("이름 또는 전화번호 검색")
            if search_k:
                mm_name  = next((c for c in df_mm2.columns if any(x in c for x in ['이름', '회원명'])), None)
                mm_phone = next((c for c in df_mm2.columns if any(x in c for x in ['휴대전화', '전화'])), None)
                if mm_name and mm_phone:
                    df_mm2['c_name']  = df_mm2[mm_name].astype(str).str.replace(' ', '')
                    df_mm2['c_phone'] = df_mm2[mm_phone].apply(clean_phone_number)
                    res = df_mm2[df_mm2['c_name'].str.contains(search_k) | df_mm2['c_phone'].str.contains(search_k)]
                    if not res.empty:
                        final_df2 = res[[mm_name, mm_phone]].copy()
                        final_df2.columns = ['이름', '전화번호']
                        st.write(f"수신자: {len(final_df2)}명")
                        st.download_button("📥 검색결과 엑셀", data=to_excel_bytes(final_df2),
                                           file_name=f"검색결과_{datetime.datetime.now().strftime('%m%d')}.xlsx",
                                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        msg_txt2 = st.text_area("보낼 내용", key='search_msg')
                        if st.button("🚀 전체 발송", type="primary", use_container_width=True, key='search_send'):
                            if not st.session_state.api_key: st.error("API Key 필요")
                            else:
                                bar = st.progress(0)
                                for i, r in enumerate(final_df2.itertuples()):
                                    send_and_log(r.이름, r.전화번호, msg_txt2)
                                    bar.progress((i+1)/len(final_df2))
                                st.success("발송 완료!")
        else:
            st.info("서버에 회원관리 파일이 없습니다.")
