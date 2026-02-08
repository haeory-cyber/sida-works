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

# ==========================================
# [설정] 서버 파일 경로
# ==========================================
SERVER_CONTACT_FILE = "농가관리 목록_20260208 (전체).xlsx"
SERVER_MEMBER_FILE = "회원관리(전체).xlsx"

# ==========================================
# 0. [공통 함수]
# ==========================================
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
        url = "https://api.coolsms.co.kr/messages/v4/send"
        payload = {"message": {"to": clean_receiver, "from": clean_sender, "text": text}}
        
        res = requests.post(url, json=payload, headers=headers)
        if res.status_code == 200: return True, res.json()
        else: return False, res.json()
    except Exception as e: return False, {"errorMessage": str(e)}

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
        row_str = row.astype(str).str.cat(sep=' ')
        match_cnt = sum(1 for k in keywords if k in row_str)
        if match_cnt >= 2:
            target_row_idx = idx
            break
            
    if target_row_idx != -1:
        df_final = df_raw.iloc[target_row_idx+1:].copy()
        df_final.columns = df_raw.iloc[target_row_idx]
        df_final.columns = df_final.columns.astype(str).str.replace(' ', '').str.replace('\n', '')
        df_final = df_final.loc[:, ~df_final.columns.str.contains('^Unnamed')]
        return df_final, None
    else:
        try:
            if hasattr(file_obj, 'seek'): file_obj.seek(0)
            return pd.read_excel(file_obj) if (hasattr(file_obj, 'name') and file_obj.name.endswith('xlsx')) else pd.read_csv(file_obj), "헤더 못 찾음(기본로드)"
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

# ==========================================
# 2. [일반 발주 업체] (화이트리스트)
# ==========================================
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
st.set_page_config(page_title="시다 워크 (Sida Works)", page_icon="🤖", layout="wide")

if 'sent_history' not in st.session_state: st.session_state.sent_history = set()
if 'api_key' not in st.session_state: st.session_state.api_key = ''
if 'api_secret' not in st.session_state: st.session_state.api_secret = ''
if 'sender_number' not in st.session_state: st.session_state.sender_number = ''

with st.sidebar:
    st.markdown("## 🤖 시다 워크")
    st.caption("Ver 18.24 (벌크꼬리표)") 
    st.divider()
    
    password = st.text_input("비밀번호", type="password")
    if password != "poom0118**":
        st.warning("비밀번호를 입력하세요.")
        st.stop()
    st.success("인증 완료")
    
    st.divider()
    st.session_state.api_key = st.text_input("API Key", value=st.session_state.api_key, type="password")
    st.session_state.api_secret = st.text_input("API Secret", value=st.session_state.api_secret, type="password")
    st.session_state.sender_number = st.text_input("발신번호 (숫자만)", value=st.session_state.sender_number)

st.title("🤖 시다 워크 (Sida Works)")
menu = st.radio("", ["📦 품앗이 오더 (자동 발주)", "📢 품앗이 이음 (마케팅)"], horizontal=True)

if menu == "📦 품앗이 오더 (자동 발주)":
    with st.container(border=True):
        c1, c2, c3, c4 = st.columns(4)
        budget = c1.number_input("💰 예산 (원)", value=500000, step=10000)
        safety = c2.slider("📈 안전 계수", 1.0, 1.5, 1.1, step=0.1)
        purchase_rate = c3.slider("📊 매입 원가율 (%)", 10, 100, 70, step=5) / 100.0
        show_all_data = c4.checkbox("🕵️‍♂️ 모든 데이터 보기 (미등록 업체 포함)")

    with st.expander("📂 **[파일 열기] 판매 실적 업로드**", expanded=True):
        up_sales_list = st.file_uploader("판매 실적 파일", type=['xlsx', 'csv'], accept_multiple_files=True, key='ord_up')
    
    df_phone_map = pd.DataFrame()
    if os.path.exists(SERVER_CONTACT_FILE):
        try:
            with open(SERVER_CONTACT_FILE, "rb") as f:
                df_i, _ = load_data_smart(f, 'info')
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
        for file_obj in up_sales_list:
            d, _ = load_data_smart(file_obj, 'sales')
            if d is not None: df_list.append(d)
        if df_list: df_s = pd.concat(df_list, ignore_index=True)

    if df_s is not None:
        st.divider()
        s_item, s_qty, s_amt, s_farmer, s_spec = detect_columns(df_s.columns.tolist())
        
        if s_item and s_qty and s_amt:
            # ==========================================
            # [시다의 긴급 처방] 거래처명에 '벌크'가 있으면 상품명에 '벌크' 꼬리표 붙이기
            # 거래처명을 통합하기 *전*에 이 작업을 먼저 해야 합니다.
            # ==========================================
            if s_farmer and s_item:
                def tag_bulk_item(row):
                    f_name = str(row[s_farmer])
                    i_name = str(row[s_item])
                    # 거래처명에 '벌크'가 있는데, 상품명에는 없다면? -> 상품명 뒤에 (벌크) 추가
                    if '벌크' in f_name and '벌크' not in i_name:
                        return i_name + "(벌크)"
                    return i_name
                
                df_s[s_item] = df_s.apply(tag_bulk_item, axis=1)

            # ------------------------------------------
            # 이제 안심하고 거래처 통합 등 기존 로직 진행
            # ------------------------------------------
            if s_farmer:
                valid_set = {v.replace(' ', '') for v in VALID_SUPPLIERS}
                df_s['clean_farmer'] = df_s[s_farmer].astype(str).str.replace(' ', '')
                
                # 거래처명 통합: 지족점야채(벌크) -> 지족점야채
                df_s['clean_farmer'] = df_s['clean_farmer'].str.replace(r'\(?벌크\)?', '', regex=True).str.replace(' ', '')

                def classify(name):
                    clean = name.replace(' ', '')
                    if "지족(Y)" in name or "지족(y)" in name: return "제외"
                    if "지족" in clean or "지족" in name: return "지족(사입)" 
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
            
            # 1. kg 단위 추출
            def extract_kg(text):
                text = str(text).lower().replace(' ', '')
                kg_match = re.search(r'([\d\.]+)(kg)', text)
                if kg_match:
                    try: return float(kg_match.group(1))
                    except: pass
                g_match = re.search(r'([\d\.]+)(g)', text)
                if g_match:
                    try: return float(g_match.group(1)) / 1000.0
                    except: pass
                return 0.0

            if s_item:
                # 2. 총 중량 계산
                def calc_unit_weight(row):
                    w = 0.0
                    if s_spec and pd.notna(row.get(s_spec)):
                        w = extract_kg(row[s_spec])
                    if w == 0 and pd.notna(row.get(s_item)):
                        w = extract_kg(row[s_item])
                    return w

                df_target['__unit_kg'] = df_target.apply(calc_unit_weight, axis=1)
                df_target['__total_kg'] = df_target['__unit_kg'] * df_target[s_qty]

                # =======================================================
                # [시다의 이중 이름표 전략]
                # =======================================================
                
                # (1) 화면용 이름: '벌크' 절대 지우지 않음. 무게 숫자만 지움.
                def make_display_name(x):
                    s = str(x)
                    s = re.sub(r'\(\s*[\d\.]+\s*(?:g|kg|G|KG)\s*\)', '', s)
                    s = s.replace('()', '').strip()
                    s = s.replace(' ', '') 
                    return s

                # (2) 문자/정렬용 이름: '벌크'를 지워서 부모(가지)와 똑같게 만듦
                def make_parent_name(x):
                    s = str(x)
                    s = re.sub(r'\(?벌크\)?', '', s)
                    s = re.sub(r'\(?bulk\)?', '', s, flags=re.IGNORECASE)
                    s = re.sub(r'\(\s*[\d\.]+\s*(?:g|kg|G|KG)\s*\)', '', s)
                    s = s.replace('()', '').replace('  ', ' ').strip()
                    s = s.replace(' ', '')
                    return s

                df_target['__display_name'] = df_target[s_item].apply(make_display_name)
                df_target['__clean_parent'] = df_target[s_item].apply(make_parent_name)

            # =======================================================
            # [집계 1: 화면 표시용] 
            # 그룹핑 키: [__display_name] 사용 (벌크 보존)
            # =======================================================
            groupby_disp = [s_farmer, '__display_name', '구분', '__clean_parent'] 
            
            agg_disp = df_target.groupby(groupby_disp).agg({
                s_qty: 'sum',
                s_amt: 'sum',
                '__total_kg': 'sum'
            }).reset_index()

            if not df_phone_map.empty and s_farmer:
                agg_disp['clean_farmer'] = agg_disp[s_farmer].astype(str).str.replace(' ', '')
                agg_disp = pd.merge(agg_disp, df_phone_map, left_on='clean_farmer', right_on='clean_name', how='left')
                agg_disp.rename(columns={'clean_phone': '전화번호'}, inplace=True)
            else: agg_disp['전화번호'] = ''
            
            agg_disp.rename(columns={s_farmer: '업체명', '__display_name': '상품명', s_qty: '판매량', s_amt: '총판매액'}, inplace=True)
            agg_disp = agg_disp[agg_disp['판매량'] > 0]
            
            # [정렬 핵심] 부모이름 -> 본인이름
            agg_disp = agg_disp.sort_values(by=['업체명', '__clean_parent', '상품명'])

            agg_disp['발주_수량'] = np.ceil(agg_disp['판매량'] * safety)
            agg_disp['발주_중량'] = np.ceil(agg_disp['__total_kg'] * safety)

            # =======================================================
            # [집계 2: 문자 발송용] - 부모이름(__clean_parent)으로 재집계
            # =======================================================
            agg_sms = agg_disp.groupby(['업체명', '__clean_parent']).agg({
                '발주_수량': 'sum',
                '발주_중량': 'sum',
                '__total_kg': 'sum'
            }).reset_index()

            tab1, tab2 = st.tabs(["🏢 외부업체 건별 발주", "🏪 지족 사입 (직접 발주)"])
            
            # [문자 생성 함수]
            def make_order_line_sms(row):
                item_name = row['__clean_parent']
                if row['__total_kg'] > 0:
                    qty_str = f"{int(row['발주_중량'])}kg"
                else:
                    qty_str = f"{int(row['발주_수량'])}개" 
                return f"- {item_name}: {qty_str}"

            # --- [탭 1] 일반 업체 ---
            with tab1:
                df_ext = agg_disp[agg_disp['구분'].isin(["일반업체", "일반업체(강제)"])].copy()
                df_ext_sms = agg_sms[agg_sms['업체명'].isin(df_ext['업체명'].unique())].copy()

                if df_ext.empty: st.info("데이터 없음")
                else:
                    search = st.text_input(f"🔍 업체명 검색", key=f"s_ext")
                    all_v = sorted(df_ext['업체명'].unique())
                    targets = [v for v in all_v if search in v] if search else all_v
                    
                    for vendor in targets:
                        is_sent = vendor in st.session_state.sent_history
                        
                        v_data_disp = df_ext[df_ext['업체명'] == vendor]
                        v_data_sms = df_ext_sms[df_ext_sms['업체명'] == vendor]
                        
                        msg_lines = [f"[{vendor} 발주]"]
                        for _, r in v_data_sms.iterrows():
                            msg_lines.append(make_order_line_sms(r))
                        msg_lines.append("잘 부탁드립니다!")
                        default_msg = "\n".join(msg_lines)
                        
                        icon = "✅" if is_sent else "📩"
                        with st.expander(f"{icon} {vendor}", expanded=not is_sent):
                            
                            st.markdown("###### 📊 상세 판매 내역")
                            cols_view = ['상품명', '판매량', '총판매액']
                            v_view = v_data_disp[cols_view].copy()
                            v_view['총판매액'] = v_view['총판매액'].apply(lambda x: f"{x:,.0f}")
                            st.dataframe(v_view, hide_index=True, use_container_width=True)

                            c1, c2 = st.columns([1, 2])
                            with c1:
                                phone = str(v_data_disp['전화번호'].iloc[0]) if not pd.isna(v_data_disp['전화번호'].iloc[0]) else ''
                                in_phone = st.text_input("전화번호", value=phone, key=f"p_ext_{vendor}")
                                if not is_sent and st.button(f"🚀 전송", key=f"b_ext_{vendor}", type="primary"):
                                    if not st.session_state.api_key: st.error("API Key 필요")
                                    else:
                                        ok, _ = send_coolsms_direct(st.session_state.api_key, st.session_state.api_secret, st.session_state.sender_number, clean_phone_number(in_phone), st.session_state.get(f"m_ext_{vendor}", default_msg))
                                        if ok:
                                            st.session_state.sent_history.add(vendor)
                                            st.rerun()
                            with c2: st.text_area("문자 내용 (자동 통합)", value=default_msg, height=150, key=f"m_ext_{vendor}")

            # --- [탭 2] 지족 사입 ---
            with tab2:
                df_int = agg_disp[agg_disp['구분'] == "지족(사입)"].copy()
                df_int_sms = agg_sms[agg_sms['업체명'].isin(df_int['업체명'].unique())].copy()

                if df_int.empty:
                    st.info("지족 사입 데이터가 없습니다.")
                else:
                    target_order = ["지족점야채", "지족점과일", "지족매장", "지족점정육", "지족점_공동구매"]
                    
                    for main_vendor in target_order:
                        df_main_disp = df_int[df_int['업체명'] == main_vendor]
                        if df_main_disp.empty: continue
                        
                        df_main_sms = df_int_sms[df_int_sms['업체명'] == main_vendor]

                        total_sales = df_main_disp['총판매액'].sum()
                        is_sent = main_vendor in st.session_state.sent_history
                        icon = "✅" if is_sent else "🚚"
                        
                        with st.expander(f"{icon} {main_vendor} (통합매출: {total_sales:,.0f}원)", expanded=not is_sent):
                            
                            st.markdown(f"**📦 {main_vendor} 판매 실적 (상세)**")
                            
                            d_show = df_main_disp.copy()
                            d_show['발주표시'] = d_show.apply(lambda x: f"{int(x['발주_중량'])}kg" if x['__total_kg'] > 0 else f"{int(x['발주_수량'])}개", axis=1)
                            d_show['총판매액'] = d_show['총판매액'].apply(lambda x: f"{x:,.0f}")
                            st.dataframe(d_show[['상품명', '발주표시', '총판매액']], hide_index=True, use_container_width=True)
                            
                            st.markdown("##### 📝 발주 문자 작성 (자동 통합됨)")
                            
                            auto_msg_lines = [f"안녕하세요 {main_vendor}입니다.", "", "[발주 요청]"]
                            for _, r in df_main_sms.iterrows(): auto_msg_lines.append(make_order_line_sms(r))
                            auto_msg_lines.append("")
                            auto_msg_lines.append("잘 부탁드립니다.")
                            default_msg = "\n".join(auto_msg_lines)

                            c1, c2 = st.columns([1, 2])
                            with c1:
                                ph = ''
                                if not df_main_disp.empty and not pd.isna(df_main_disp['전화번호'].iloc[0]):
                                    ph = str(df_main_disp['전화번호'].iloc[0])
                                    
                                in_phone = st.text_input("전화번호", value=ph, key=f"p_v10_{main_vendor}")
                                if not is_sent and st.button(f"🚀 전송", key=f"b_v10_{main_vendor}", type="primary"):
                                    if not st.session_state.api_key: st.error("API Key 필요")
                                    else:
                                        # Key 갱신 (v10)
                                        final_msg = st.session_state.get(f"m_v10_{main_vendor}", default_msg)
                                        ok, _ = send_coolsms_direct(st.session_state.api_key, st.session_state.api_secret, st.session_state.sender_number, clean_phone_number(in_phone), final_msg)
                                        if ok:
                                            st.session_state.sent_history.add(main_vendor)
                                            st.rerun()
                            with c2:
                                st.text_area("내용", value=default_msg, height=250, key=f"m_v10_{main_vendor}")

        else: st.error("엑셀 형식을 확인해주세요.")
    else: st.info("판매 데이터를 업로드해주세요.")

elif menu == "📢 품앗이 이음 (마케팅)":
    # 마케팅 기능은 기존과 동일
    with st.expander("📂 **[파일 열기] 타겟팅용 판매 데이터 업로드**", expanded=True):
        up_mkt_sales = st.file_uploader("1. 판매내역 (타겟팅)", type=['xlsx', 'csv'], key='mkt_s')

    df_ms, _ = load_data_smart(up_mkt_sales, 'sales')
    df_mm = None
    if os.path.exists(SERVER_MEMBER_FILE):
        try:
            with open(SERVER_MEMBER_FILE, "rb") as f: df_mm, _ = load_data_smart(f, 'member')
        except: pass

    tab_m1, tab_m2 = st.tabs(["🎯 판매 기반 타겟팅", "🔍 회원 직접 검색"])
    final_df = pd.DataFrame()
    
    with tab_m1:
        if df_ms is not None:
            ms_farmer = next((c for c in df_ms.columns if any(x in c for x in ['농가', '공급자'])), None)
            ms_item = next((c for c in df_ms.columns if any(x in c for x in ['상품', '품목'])), None)
            ms_buyer = next((c for c in df_ms.columns if any(x in c for x in ['회원', '구매자'])), None)
            if ms_farmer and ms_buyer:
                sel_farmer = st.selectbox("농가 선택", sorted(df_ms[ms_farmer].astype(str).unique()))
                target_df = df_ms[df_ms[ms_farmer] == sel_farmer]
                if ms_item:
                    sel_item = st.selectbox("상품 선택", ["전체"] + sorted(target_df[ms_item].astype(str).unique()))
                    if sel_item != "전체": target_df = target_df[target_df[ms_item] == sel_item]
                
                loyal = target_df.groupby(ms_buyer).size().reset_index(name='구매횟수').sort_values('구매횟수', ascending=False)
                if df_mm is not None:
                    mm_name = next((c for c in df_mm.columns if any(x in c for x in ['이름', '회원명'])), None)
                    mm_phone = next((c for c in df_mm.columns if any(x in c for x in ['휴대전화', '전화'])), None)
                    if mm_name and mm_phone:
                        loyal['key'] = loyal[ms_buyer].astype(str).str.replace(' ', '')
                        df_mm['key'] = df_mm[mm_name].astype(str).str.replace(' ', '')
                        final_df = pd.merge(loyal, df_mm.drop_duplicates(subset=['key']), on='key', how='left')[[ms_buyer, mm_phone, '구매횟수']]
                        final_df.columns = ['이름', '전화번호', '구매횟수']
    
    with tab_m2:
        if df_mm is not None:
            search_k = st.text_input("이름 또는 전화번호 검색")
            if search_k:
                mm_name = next((c for c in df_mm.columns if any(x in c for x in ['이름', '회원명'])), None)
                mm_phone = next((c for c in df_mm.columns if any(x in c for x in ['휴대전화', '전화'])), None)
                if mm_name and mm_phone:
                    df_mm['c_name'] = df_mm[mm_name].astype(str).str.replace(' ', '')
                    df_mm['c_phone'] = df_mm[mm_phone].apply(clean_phone_number)
                    res = df_mm[df_mm['c_name'].str.contains(search_k) | df_mm['c_phone'].str.contains(search_k)]
                    if not res.empty:
                        final_df = res[[mm_name, mm_phone]].copy()
                        final_df.columns = ['이름', '전화번호']

    if not final_df.empty:
        st.divider()
        st.write(f"수신자: {len(final_df)}명")
        msg_txt = st.text_area("보낼 내용")
        if st.button("🚀 전체 발송", type="primary"):
            if not st.session_state.api_key: st.error("API Key 필요")
            else:
                bar = st.progress(0)
                for i, r in enumerate(final_df.itertuples()):
                    send_coolsms_direct(st.session_state.api_key, st.session_state.api_secret, st.session_state.sender_number, r.전화번호, msg_txt)
                    bar.progress((i+1)/len(final_df))
                st.success("발송 완료!")
