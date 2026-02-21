import streamlit as st
import pandas as pd
import io, os, re, time, hmac, hashlib, uuid, datetime, requests
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
from supabase import create_client, Client

# ══════════════════════════════════════════
# 설정
# ══════════════════════════════════════════
SERVER_CONTACT_FILE = "농가관리 목록_20260208 (전체).xlsx"
SERVER_MEMBER_FILE  = "회원관리(전체).xlsx"

def get_secret(k, fb=""):
    try: return st.secrets.get(k, fb)
    except: return fb

# ══════════════════════════════════════════
# 유틸 함수
# ══════════════════════════════════════════
def send_sms(api_key, api_secret, sender, receiver, text):
    try:
        to = re.sub(r"[^0-9]", "", str(receiver))
        fr = re.sub(r"[^0-9]", "", str(sender))
        if not to or not fr: return False, {"errorMessage": "번호 오류"}
        date = datetime.datetime.now(datetime.timezone.utc).isoformat()
        salt = str(uuid.uuid4())
        sig  = hmac.new(api_secret.encode(), (date+salt).encode(), hashlib.sha256).hexdigest()
        headers = {
            "Authorization": f"HMAC-SHA256 apiKey={api_key}, date={date}, salt={salt}, signature={sig}",
            "Content-Type": "application/json"
        }
        res = requests.post("https://api.coolsms.co.kr/messages/v4/send",
                            json={"message": {"to": to, "from": fr, "text": text}}, headers=headers)
        return (True, res.json()) if res.status_code == 200 else (False, res.json())
    except Exception as e:
        return False, {"errorMessage": str(e)}

def send_email(sender_email, sender_password, receiver_email, subject, body):
    try:
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.login(sender_email, sender_password)
        server.send_message(msg)
        server.quit()
        return True, "성공"
    except Exception as e:
        return False, str(e)

def send_and_log(name, phone, text, email="", is_email=False):
    if is_email:
        if not st.session_state.get("gmail_user") or not st.session_state.get("gmail_pw"):
            st.error("Gmail 설정이 필요합니다.")
            return False
        ok, res = send_email(
            st.session_state.gmail_user, st.session_state.gmail_pw, email,
            f"[품앗이소비자생활협동조합] {name} 발주 요청", text
        )
        mode_str = "이메일"
        target_str = email
    else:
        if not st.session_state.get("api_key"): 
            st.error("API Key 없음")
            return False
        ok, res = send_sms(
            st.session_state.api_key, st.session_state.api_secret,
            st.session_state.sender_number, phone, text
        )
        mode_str = "문자"
        target_str = phone

    st.session_state.sms_history.insert(0, {
        "시간": datetime.datetime.now().strftime("%H:%M:%S"),
        "수신자": name, "연락처": target_str,
        "방식": mode_str,
        "결과": "✅" if ok else "❌",
        "비고": "" if ok else (res.get("errorMessage", "") if not is_email else res)
    })
    return ok

def clean_phone(phone):
    if pd.isna(phone) or str(phone).strip() in ["-", "", "nan"]: return ""
    n = re.sub(r"[^0-9]", "", str(phone))
    if n.startswith("10") and len(n) >= 10: n = "0" + n
    return n

@st.cache_data
def load_smart(file_obj, ftype="sales"):
    if file_obj is None: return None, "없음"
    df_raw = None
    try:
        df_raw = pd.read_excel(file_obj, header=None, engine="openpyxl")
    except:
        try:
            if hasattr(file_obj, "seek"): file_obj.seek(0)
            df_raw = pd.read_csv(file_obj, header=None, encoding="utf-8")
        except:
            return None, "읽기 실패"

    kws = (["농가","공급자","생산자","상품","품목"] if ftype == "sales"
           else ["회원번호","이름","휴대전화"] if ftype == "member"
           else ["농가명","휴대전화"])
    tgt = -1
    for idx, row in df_raw.head(20).iterrows():
        if sum(1 for k in kws if k in row.astype(str).str.cat(sep=" ")) >= 2:
            tgt = idx; break
    if tgt != -1:
        df = df_raw.iloc[tgt+1:].copy()
        df.columns = df_raw.iloc[tgt]
        df.columns = df.columns.astype(str).str.replace(" ", "").str.replace("\n", "")
        return df.loc[:, ~df.columns.str.contains("^Unnamed")], None
    try:
        if hasattr(file_obj, "seek"): file_obj.seek(0)
        return (pd.read_excel(file_obj) if (hasattr(file_obj, "name") and
                file_obj.name.endswith("xlsx")) else pd.read_csv(file_obj)), "헤더 못 찾음"
    except:
        return df_raw, "헤더 못 찾음"

def to_num(x):
    try:
        s = re.sub(r"[^0-9.-]", "", str(x))
        return float(s) if s not in ["", "."] else 0
    except:
        return 0

def detect_cols(cols):
    excl = ["할인","반품","취소","면세","과세","부가세"]
    s_item   = next((c for c in cols if any(x in c for x in ["상품","품목"])), None)
    s_qty    = next((c for c in cols if any(x in c for x in ["판매수량","수량","개수"])), None)
    cands    = ([c for c in cols if ("총" in c and ("판매" in c or "매출" in c))] +
                [c for c in cols if (("판매" in c or "매출" in c) and ("액" in c or "금액" in c))] +
                [c for c in cols if "금액" in c])
    s_amt    = next((c for c in cands if not any(b in c for b in excl)), None)
    s_farmer = next((c for c in cols if any(x in c for x in ["공급자","농가","생산자","거래처"])), None)
    s_spec   = next((c for c in cols if any(x in c for x in ["규격","단위","중량","용량"])), None)
    s_date   = next((c for c in cols if any(x in c for x in ["일시","날짜","date","Date"])), None)
    s_vat    = next((c for c in cols if any(x in c for x in ["부가세","세액","VAT"])), None)
    return s_item, s_qty, s_amt, s_farmer, s_spec, s_date, s_vat

def to_excel(df):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False)
    return buf.getvalue()

def ext_kg(text):
    text = str(text).lower().replace(" ", "")
    m = re.search(r"([\d\.]+)(kg)", text)
    if m:
        try: return float(m.group(1))
        except: pass
    m = re.search(r"([\d\.]+)(g)", text)
    if m:
        try: return float(m.group(1)) / 1000
        except: pass
    return 0.0

VALID_SUPPLIERS = [
    "(주)가보트레이딩","(주)열두달","(주)우리밀","(주)윈윈농수산","(주)유기샘",
    "(주)케이푸드","(주)한누리","G1상사","mk코리아","가가호영어조합법인",
    "고삼농협","금강향수","나우푸드","네니아","농부생각","농업회사법인(주)담채원",
    "당암tf","더테스트키친","도마령영농조합법인","두레생협","또또푸드","로엘팩토리",
    "맛가마","산백유통","새롬식품","생수콩나물영농조합법인","슈가랩","씨글로벌(아라찬)",
    "씨에이치하모니","언니들공방","에르코스","엔젤농장","우리밀농협","우신영농조합",
    "유기농산","유안컴퍼니","인터뷰베이커리","자연에찬","장수이야기","제로웨이스트존",
    "청양농협조합","청오건강농업회사법인","청춘농장","코레드인터내쇼날","태경F&B",
    "토종마을","폴카닷(이은경)","하대목장","한산항아리소곡주","함지박(주)","행복우리식품영농조합"
]

# ══════════════════════════════════════════
# 세션 초기화
# ══════════════════════════════════════════
for k, v in [
    ("sms_history", []),
    ("sent_history", set()),
    ("auth_passed", False),
    ("api_key", get_secret("SOLAPI_API_KEY", "")),
    ("api_secret", get_secret("SOLAPI_API_SECRET", "")),
    ("sender_number", get_secret("SENDER_NUMBER", "")),
    ("gmail_user", get_secret("GMAIL_USER", "")),
    ("gmail_pw", get_secret("GMAIL_APP_PW", "")),
    ("field_requests", []),
]:
    if k not in st.session_state:
        st.session_state[k] = v

# ══════════════════════════════════════════
# 인증 (보안 강화 및 브라우저 자동완성 지원)
# ══════════════════════════════════════════
saved_pw = get_secret("APP_PASSWORD", "poom0118**")

if not st.session_state.auth_passed:
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;700;900&display=swap');
    * { font-family: 'Noto Sans KR', sans-serif; }
    .login-wrap { display:flex; flex-direction:column; align-items:center; justify-content:center; margin-top: 10vh; }
    .login-title { font-size:2.5rem; font-weight:900; color:#1a1a1a; letter-spacing:-2px; }
    .login-sub { color:#888; margin-top:0.5rem; font-size:0.95rem; margin-bottom: 2rem; }
    </style>
    <div class="login-wrap">
    <div class="login-title">🌿 시다 워크</div>
    <div class="login-sub">품앗이생협 업무 자동화 시스템</div>
    </div>
    """, unsafe_allow_html=True)
    
    with st.form("login_form"):
        pw = st.text_input("비밀번호를 입력하세요 (브라우저 자동완성 지원)", type="password", autocomplete="current-password")
        submitted = st.form_submit_button("입장하기", use_container_width=True)
        if submitted:
            if pw == saved_pw:
                st.session_state.auth_passed = True
                st.rerun()
            elif pw:
                st.error("비밀번호가 다릅니다.")
    st.stop()

# ══════════════════════════════════════════
# 페이지 설정 & 스타일
# ══════════════════════════════════════════
st.set_page_config(page_title="시다 워크", page_icon="🌿", layout="wide", initial_sidebar_state="expanded")
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;700;900&family=Space+Mono&display=swap');
* { font-family: 'Noto Sans KR', sans-serif; }
code, .mono { font-family: 'Space Mono', monospace; }
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 1.5rem 2rem 2rem; }
section[data-testid="stSidebar"] { background: #0f1923; border-right: 1px solid #1e2d3d; }
section[data-testid="stSidebar"] * { color: #c8d6e5 !important; }
section[data-testid="stSidebar"] .stTextInput input { background: #1a2735 !important; border: 1px solid #2d4057 !important; color: #fff !important; border-radius: 8px; }
.main-header { display: flex; align-items: baseline; gap: 12px; margin-bottom: 1.5rem; padding-bottom: 1rem; border-bottom: 2px solid #f0f0f0; }
.main-title { font-size: 1.6rem; font-weight: 900; color: #1a1a1a; letter-spacing: -1px; }
.main-badge { font-size: 0.7rem; font-weight: 700; background: #2d6a4f; color: white; padding: 3px 10px; border-radius: 20px; letter-spacing: 1px; }
.section-label { font-size: 0.75rem; font-weight: 700; color: #888; letter-spacing: 2px; text-transform: uppercase; margin: 1.2rem 0 0.6rem; }
.budget-bar-wrap { background: #f0f0f0; border-radius: 20px; height: 10px; margin: 6px 0; }
.budget-bar { background: linear-gradient(90deg, #27ae60, #2ecc71); border-radius: 20px; height: 10px; transition: width 0.5s; }
.budget-bar.warn { background: linear-gradient(90deg, #e67e22, #f39c12); }
.budget-bar.danger { background: linear-gradient(90deg, #c0392b, #e74c3c); }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════
# 사이드바
# ══════════════════════════════════════════
with st.sidebar:
    st.markdown("### 🌿 시다 워크")
    st.caption("Ver 2.0 · 품앗이생협")
    st.divider()
    st.markdown('<div class="section-label">솔라피 설정 (문자)</div>', unsafe_allow_html=True)
    st.session_state.api_key       = st.text_input("API Key", value=st.session_state.api_key, type="password", label_visibility="collapsed", placeholder="API Key")
    st.session_state.api_secret    = st.text_input("Secret", value=st.session_state.api_secret, type="password", label_visibility="collapsed", placeholder="API Secret")
    st.session_state.sender_number = st.text_input("발신번호", value=st.session_state.sender_number, label_visibility="collapsed", placeholder="발신번호 (숫자만)")
    st.divider()
    st.markdown('<div class="section-label">Gmail 설정 (이메일)</div>', unsafe_allow_html=True)
    st.session_state.gmail_user = st.text_input("Gmail 계정", value=st.session_state.gmail_user, placeholder="example@gmail.com")
    st.session_state.gmail_pw   = st.text_input("앱 비밀번호", value=st.session_state.gmail_pw, type="password", placeholder="16자리 앱 비밀번호")
    st.caption("구글 계정 관리 > 보안 > 2단계 인증 > 앱 비밀번호에서 생성")
    st.divider()
    with st.expander("📋 발송 이력", expanded=False):
        if st.session_state.sms_history:
            log_df = pd.DataFrame(st.session_state.sms_history)
            st.dataframe(log_df, hide_index=True, use_container_width=True)
            if st.button("이력 초기화"):
                st.session_state.sms_history = []; st.rerun()
        else:
            st.caption("아직 전송 내역이 없습니다.")

# ══════════════════════════════════════════
# 메인 헤더
# ══════════════════════════════════════════
st.markdown("""
<div class="main-header">
  <span class="main-title">시다 워크</span>
  <span class="main-badge">v2.0</span>
</div>
""", unsafe_allow_html=True)

menu = st.radio("", ["📦 발주", "♻️ 제로웨이스트", "📢 이음"], horizontal=True, label_visibility="collapsed")
st.markdown("---")

# ══════════════════════════════════════════════════════════════════
# 📦 발주 탭
# ══════════════════════════════════════════════════════════════════
if menu == "📦 발주":
    tab_order, tab_field, tab_send = st.tabs(["🧮 판매데이터 분석", "📍 현장 요청 (실시간)", "📤 발주 발송(농가별)"])

    df_phone_map = pd.DataFrame()
    if os.path.exists(SERVER_CONTACT_FILE):
        try:
            with open(SERVER_CONTACT_FILE, "rb") as f:
                df_ci, _ = load_smart(f, "info")
            if df_ci is not None:
                i_name  = next((c for c in df_ci.columns if "농가명" in c), None)
                i_phone = next((c for c in df_ci.columns if "휴대전화" in c or "전화" in c), None)
                i_email = next((c for c in df_ci.columns if "이메일" in c or "email" in c.lower()), None)
                if i_name and i_phone:
                    df_ci["clean_farmer"]  = df_ci[i_name].astype(str).str.replace(" ", "")
                    df_ci["clean_phone"] = df_ci[i_phone].apply(clean_phone)
                    df_ci["clean_email"] = df_ci[i_email].astype(str) if i_email else ""
                    df_phone_map = df_ci.drop_duplicates(subset=["clean_farmer"])[["clean_farmer", "clean_phone", "clean_email"]]
        except:
            pass

    with tab_order:
        st.markdown('<div class="section-label">💰 유동자금 설정</div>', unsafe_allow_html=True)
        col_b1, col_b2, col_b3 = st.columns([2, 1, 1])
        with col_b1:
            budget = st.number_input("현재 유동자금 (원)", min_value=0, value=st.session_state.get("budget", 30000000), step=100000, format="%d")
            st.session_state.budget = budget
        with col_b2:
            safety = st.slider("안전계수", 1.0, 1.5, 1.1, step=0.1)
        with col_b3:
            period_map = {"최근 1일": 1, "최근 3일": 3, "최근 7일": 7, "최근 14일": 14}
            sel_period = st.selectbox("집계기간", list(period_map.keys()), index=2)
            period_days = period_map[sel_period]

        st.markdown('<div class="section-label">📂 판매 실적 업로드</div>', unsafe_allow_html=True)
        up_sales = st.file_uploader("판매 실적 파일", type=["xlsx", "csv"], accept_multiple_files=True, key="ord_up", label_visibility="collapsed")

        field_reqs_df = pd.DataFrame()
        if st.session_state.field_requests:
            field_reqs_df = pd.DataFrame(st.session_state.field_requests)

        if not field_reqs_df.empty:
            st.markdown('<div class="section-label">📍 현장 요청 반영 중</div>', unsafe_allow_html=True)
            st.dataframe(field_reqs_df, hide_index=True, use_container_width=True)

        if up_sales:
            parts = []
            for f in up_sales:
                d, _ = load_smart(f, "sales")
                if d is not None: parts.append(d)

            if parts:
                df_s = pd.concat(parts, ignore_index=True)
                s_item, s_qty, s_amt, s_farmer, s_spec, s_date, s_vat = detect_cols(df_s.columns.tolist())

                if s_item and s_amt:
                    def norm_name(name):
                        n = str(name).replace(" ", "")
                        if "지족" in n and "야채" in n: return "지족점야채"
                        if "지족" in n and "과일" in n: return "지족점과일"
                        if "지족" in n and "정육" in n: return "지족점정육"
                        if "지족" in n and "공동" in n: return "지족점_공동구매"
                        if "지족" in n and "매장" in n: return "지족매장"
                        return re.sub(r"\(?벌크\)?", "", n)

                    def disp_name(x):
                        s = str(x).replace("*", "")
                        return re.sub(r"\(\s*[\d\.]+\s*(?:g|kg|G|KG)\s*\)", "", s).replace("()", "").strip().replace(" ", "")

                    def parent_name(x):
                        s = str(x).replace("*", "")
                        s = re.sub(r"\(?벌크\)?", "", s)
                        s = re.sub(r"\(?bulk\)?", "", s, flags=re.IGNORECASE)
                        return re.sub(r"\(\s*[\d\.]+\s*(?:g|kg|G|KG)\s*\)", "", s).replace("()", "").strip().replace(" ", "")

                    valid_set = {v.replace(" ", "") for v in VALID_SUPPLIERS}

                    if s_farmer:
                        df_s["clean_farmer"] = df_s[s_farmer].apply(norm_name)
                        df_s[s_farmer] = df_s["clean_farmer"]
                        def classify(name):
                            c = name.replace(" ", "")
                            if "지족(Y)" in name or "지족(y)" in name: return "제외"
                            if "지족" in c: return "지족(사입)"
                            elif c in valid_set: return "일반업체"
                            else: return "일반업체"
                        df_s["구분"] = df_s["clean_farmer"].apply(classify)
                        df_t = df_s[df_s["구분"] != "제외"].copy()
                    else:
                        df_t = df_s.copy()
                        df_t["구분"] = "일반업체"
                        df_t["clean_farmer"] = df_t[s_item].apply(norm_name)

                    df_t[s_qty] = df_t[s_qty].apply(to_num) if s_qty else 1
                    df_t[s_amt] = df_t[s_amt].apply(to_num)
                    df_t.loc[(df_t[s_qty] <= 0) & (df_t[s_amt] > 0), s_qty] = 1
                    
                    if s_vat:
                        df_t[s_vat] = df_t[s_vat].apply(to_num)
                        df_t["과세구분"] = np.where(df_t[s_vat] > 0, "과세", "비과세")
                    else:
                        df_t["과세구분"] = "비과세"

                    if s_date:
                        df_t["__date"] = pd.to_datetime(df_t[s_date], errors="coerce")
                        cutoff = pd.Timestamp.now() - pd.Timedelta(days=period_days)
                        df_t = df_t[df_t["__date"] >= cutoff]

                    df_t["__disp"]   = df_t[s_item].apply(disp_name)
                    df_t["__parent"] = df_t[s_item].apply(parent_name)
                    df_t["__unit_kg"]  = df_t.apply(lambda r: ext_kg(r.get(s_spec, "")) or ext_kg(r[s_item]), axis=1)
                    df_t["__total_kg"] = df_t["__unit_kg"] * df_t[s_qty]

                    farmer_col = s_farmer if s_farmer else "clean_farmer"
                    agg = df_t.groupby([farmer_col, "__disp", "구분", "__parent", "과세구분"]).agg(
                        {s_qty: "sum", s_amt: "sum", "__total_kg": "sum"}
                    ).reset_index()

                    if not df_phone_map.empty:
                        agg["clean_farmer"] = agg[farmer_col].astype(str).str.replace(" ", "")
                        agg = pd.merge(agg, df_phone_map, on="clean_farmer", how="left")
                    else:
                        agg["clean_phone"] = ""
                        agg["clean_email"] = ""

                    agg.rename(columns={farmer_col: "업체명", "__disp": "상품명", s_qty: "판매량", s_amt: "총판매액"}, inplace=True)
                    agg = agg[agg["총판매액"] > 0].sort_values(["업체명", "__parent", "상품명"])

                    agg["발주_수량"] = np.ceil(agg["판매량"] * safety / period_days)
                    agg["발주_중량"] = np.ceil(agg["__total_kg"] * safety / period_days)

                    urgent_items = set()
                    if not field_reqs_df.empty:
                        for _, req in field_reqs_df.iterrows():
                            if req.get("긴급도", "") == "🔴 오늘 필요":
                                urgent_items.add(str(req.get("품목명", "")).replace(" ", ""))

                    farmer_est = agg.groupby("업체명")["총판매액"].sum() * 0.7
                    farmer_est_df = farmer_est.reset_index()
                    farmer_est_df.columns = ["업체명", "예상발주액_업체합계"]
                    agg = pd.merge(agg, farmer_est_df, on="업체명", how="left")
                    agg["예상발주액"] = agg["총판매액"] * 0.7

                    def calc_priority(row):
                        score = row["총판매액"] * 0.7  
                        if row["상품명"].replace(" ", "") in urgent_items: score *= 3  
                        return score

                    agg["우선순위점수"] = agg.apply(calc_priority, axis=1)
                    agg_sorted = agg.sort_values("우선순위점수", ascending=False).copy()
                    agg_sorted["누적발주액"] = agg_sorted["예상발주액"].cumsum()
                    agg_sorted["예산내"] = agg_sorted["누적발주액"] <= budget

                    def priority_label(row):
                        if row["상품명"].replace(" ", "") in urgent_items: return "🔴 긴급"
                        if row["예산내"]: return "🟢 권장"
                        return "⚪ 여유"

                    agg_sorted["발주상태"] = agg_sorted.apply(priority_label, axis=1)

                    est_total = agg_sorted[agg_sorted["예산내"]]["예상발주액"].sum()
                    st.session_state.est_order_total = est_total
                    st.session_state.order_df = agg_sorted  

                    st.success("✅ 판매 데이터 분석 완료! '발주 발송' 탭을 확인하세요.")
                    
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("전체 품목", f"{len(agg_sorted)}건")
                    m2.metric("긴급 품목", f"{(agg_sorted['발주상태']=='🔴 긴급').sum()}건")
                    m3.metric("예산 내 품목", f"{agg_sorted['예산내'].sum()}건")
                    m4.metric("예상 발주액", f"{est_total:,.0f}원")
                    
                    if budget > 0:
                        ratio = min(est_total / budget, 1.0)
                        bar_class = "danger" if ratio > 0.8 else "warn" if ratio > 0.5 else ""
                        pct = int(ratio * 100)
                        st.markdown(f"""
                        <div style="font-size:0.8rem; color:#888; margin-bottom:2px;">
                          예상 발주액: <b>{est_total:,.0f}원</b> / 유동자금: <b>{budget:,.0f}원</b> ({pct}% 사용)
                        </div>
                        <div class="budget-bar-wrap"><div class="budget-bar {bar_class}" style="width:{pct}%"></div></div>
                        """, unsafe_allow_html=True)

    with tab_field:
        st.markdown("""
        <div style="background:#fff9f0; border:1.5px solid #f39c12; border-radius:12px; padding:1rem 1.2rem; margin-bottom:1rem;">
        <b>📍 현장 요청 입력 (임시 저장소)</b><br>
        <span style="font-size:0.85rem; color:#666;">입력된 데이터는 앱 내에 임시로 보관됩니다. (메인 대시보드는 아래 수파베이스 목록을 확인하세요)</span>
        </div>
        """, unsafe_allow_html=True)

        with st.form("field_request_form", clear_on_submit=True):
            fc1, fc2, fc3 = st.columns([3, 2, 2])
            req_item    = fc1.text_input("품목명 (필수) *", placeholder="예: 감자, 두부")
            req_farmer  = fc2.text_input("농가명 (알면 적어주세요)", placeholder="예: 행복농장")
            req_urgent  = fc3.selectbox("긴급도", ["🔴 오늘 필요", "🟡 이번 주", "🟢 여유 있음"])
            req_note    = st.text_input("메모 (추가 전달사항)", placeholder="예: 3번 조합원님 요청")
            submitted   = st.form_submit_button("➕ 요청 추가", type="primary", use_container_width=True)

            if submitted:
                if not req_item:
                    st.warning("품목명은 꼭 적어주셔야 품앗이님들이 알 수 있습니다.")
                else:
                    new_row = [
                        req_item, 
                        req_farmer if req_farmer else "미지정", 
                        req_urgent, 
                        req_note if req_note else "-", 
                        datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                    ]
                    st.session_state.field_requests.append({
                        "품목명": new_row[0], "농가명": new_row[1], "긴급도": new_row[2], "메모": new_row[3], "입력시간": new_row[4]
                    })
                    st.success(f"✅ 임시 저장소에 '{req_item}' 요청이 추가되었습니다!")

        if st.session_state.field_requests:
            st.markdown('<div class="section-label">현재 요청 목록 (임시)</div>', unsafe_allow_html=True)
            st.dataframe(pd.DataFrame(st.session_state.field_requests), hide_index=True, use_container_width=True)
            if st.button("🗑 임시 데이터 초기화", use_container_width=True):
                st.session_state.field_requests = []
                st.rerun()

    with tab_send:
        if "order_df" not in st.session_state or st.session_state.order_df is None:
            st.info("먼저 '판매데이터 분석' 탭에서 파일을 업로드해주세요.")
        else:
            agg_all = st.session_state.order_df
            df_saip = agg_all[agg_all["구분"] == "지족(사입)"]
            df_balju = agg_all[agg_all["구분"] == "일반업체"]
            
            farmer_tax_types = df_balju.groupby("업체명")["과세구분"].unique().apply(
                lambda x: "혼합(과세+비과세)" if len(x) > 1 else (x[0] + " 전용")
            ).reset_index(name="농가_과세유형")
            df_balju = pd.merge(df_balju, farmer_tax_types, on="업체명", how="left")
            
            sub_tab1, sub_tab2 = st.tabs([f"🌾 농가 발주 대상", f"🛒 지족점 사입"])
            
            with sub_tab1:
                tax_type = st.radio("과세 구분 선택", ["비과세 전용", "과세 전용", "혼합(과세+비과세)"], horizontal=True)
                df_balju_tax = df_balju[df_balju["농가_과세유형"] == tax_type]
                
                col_left, col_right = st.columns([1, 2])
                with col_left:
                    st.markdown('<div class="section-label">농가 선택</div>', unsafe_allow_html=True)
                    farmer_list = df_balju_tax["업체명"].unique().tolist()
                    if not farmer_list:
                        st.warning(f"{tax_type} 농가가 없습니다.")
                    else:
                        sel_farmer = st.selectbox("발주할 농가를 선택하세요", farmer_list, label_visibility="collapsed")
                        fd = df_balju_tax[df_balju_tax["업체명"] == sel_farmer]
                        phone = fd["clean_phone"].iloc[0] if "clean_phone" in fd.columns else ""
                        email = fd["clean_email"].iloc[0] if "clean_email" in fd.columns else ""
                        farmer_total = fd["총판매액"].sum()
                        st.markdown(f"**총 판매액:** {farmer_total:,.0f}원")
                        st.markdown(f"**품목 수:** {len(fd)}개")
                        if phone: st.caption(f"📞 {phone}")
                        if email: st.caption(f"📧 {email}")
                
                with col_right:
                    if farmer_list and sel_farmer:
                        st.markdown('<div class="section-label">발주 내역 확인 및 수정</div>', unsafe_allow_html=True)
                        
                        def generate_order_text(df_src):
                            grp = df_src.groupby(["과세구분", "__parent"]).agg({"발주_수량": "sum"}).reset_index()
                            lines = []
                            for _, r in grp.iterrows():
                                prefix = f"[{r['과세구분']}] " if tax_type == "혼합(과세+비과세)" else ""
                                lines.append(f"- {prefix}{r['__parent']}: {int(r['발주_수량'])}개")
                            return lines

                        default_msg = "\n".join(
                            [f"[품앗이소비자생활협동조합 발주 요청]"] +
                            [f"{sel_farmer} 농가님, 안녕하세요."] +
                            [f"조합원님들의 사랑으로 판매된 품목의 추가 발주를 요청드립니다.\n"] +
                            generate_order_text(fd) +
                            ["\n정직한 땀방울에 항상 감사드립니다. 🙏"]
                        )
                        
                        msg_input = st.text_area("발주 문구 및 수량 (자유롭게 수정하세요)", value=default_msg, height=250, key=f"msg_edit_{sel_farmer}")
                        
                        st.markdown('<div class="section-label">발송 정보 입력</div>', unsafe_allow_html=True)
                        c1, c2 = st.columns(2)
                        with c1:
                            in_ph = st.text_input("받는 사람 번호 📞", value=phone or "", key=f"in_ph_{sel_farmer}")
                            if st.button("📱 문자(SMS) 발송", key=f"btn_sms_{sel_farmer}", type="primary", use_container_width=True):
                                if in_ph:
                                    with st.spinner("문자 발송 중..."):
                                        ok = send_and_log(sel_farmer, clean_phone(in_ph), msg_input, is_email=False)
                                        if ok:
                                            st.session_state.sent_history.add(sel_farmer)
                                            st.success("✅ 문자 발송 완료")
                                        else: st.error("❌ 문자 발송 실패")
                                else: st.warning("전화번호를 입력해주세요.")
                                    
                        with c2:
                            in_em = st.text_input("받는 사람 이메일 📧", value=email or "", key=f"in_em_{sel_farmer}")
                            if st.button("📧 이메일 발송", key=f"btn_em_{sel_farmer}", type="secondary", use_container_width=True):
                                if in_em and "@" in in_em:
                                    with st.spinner("이메일 발송 중..."):
                                        ok = send_and_log(sel_farmer, "", msg_input, email=in_em, is_email=True)
                                        if ok:
                                            st.session_state.sent_history.add(sel_farmer)
                                            st.success("✅ 이메일 발송 완료")
                                        else: st.error("❌ 이메일 발송 실패")
                                else: st.warning("올바른 이메일 주소를 입력해주세요.")

            with sub_tab2:
                saip_type = st.radio("사입 분류 선택", ["지족점정육", "지족점야채", "지족점과일", "지족매장"], horizontal=True)
                df_saip_sub = df_saip[df_saip["업체명"] == saip_type]
                
                st.markdown(f"### 🛒 {saip_type} 목록")
                if df_saip_sub.empty: 
                    st.info(f"{saip_type} 사입 데이터가 없습니다.")
                else:
                    show_cols = ["발주상태", "업체명", "상품명", "과세구분", "판매량", "발주_수량", "총판매액"]
                    st.dataframe(df_saip_sub[show_cols], hide_index=True, use_container_width=True)

# ══════════════════════════════════════════
# ♻️ 제로웨이스트 및 📢 이음 코드
# ══════════════════════════════════════════
elif menu == "♻️ 제로웨이스트":
    st.markdown("### ♻️ 제로웨이스트 판매 분석")
    with st.expander("📂 판매 데이터 업로드", expanded=True):
        up_zw = st.file_uploader("판매 실적 파일", type=["xlsx", "csv"], accept_multiple_files=True, key="zw_up")

    if up_zw:
        parts = []
        for f in up_zw:
            d, _ = load_smart(f, "sales")
            if d is not None: parts.append(d)
        if parts:
            df_zw = pd.concat(parts, ignore_index=True)
            s_item, s_qty, s_amt, s_farmer, s_spec, _, _ = detect_cols(df_zw.columns.tolist())
            if s_item and s_amt:
                def parent_zw(x):
                    s = str(x)
                    s = re.sub(r"\(?벌크\)?", "", s)
                    s = re.sub(r"\(?bulk\)?", "", s, flags=re.IGNORECASE)
                    return re.sub(r"\(.*?\)", "", s).replace("*", "").replace("()", "").strip().replace(" ", "")

                df_zw["__parent"] = df_zw[s_item].apply(parent_zw)
                df_zw[s_amt] = df_zw[s_amt].apply(to_num)

                def type_tag(row):
                    i = str(row[s_item])
                    f2 = str(row[s_farmer]) if s_farmer and pd.notna(row.get(s_farmer)) else ""
                    return "벌크(무포장)" if ("벌크" in i or "bulk" in i.lower() or "벌크" in f2) else "일반(포장)"

                df_zw["__type"] = df_zw.apply(type_tag, axis=1)
                grp = df_zw.groupby(["__parent", "__type"])[s_amt].sum().reset_index()
                bulk_items = grp[grp["__type"] == "벌크(무포장)"]["__parent"].unique()
                tdf = grp[grp["__parent"].isin(bulk_items)].copy()

                if len(bulk_items) == 0:
                    st.info("벌크 데이터 없음")
                else:
                    cols = st.columns(2)
                    for i, parent in enumerate(sorted(tdf["__parent"].unique())):
                        sub = tdf[tdf["__parent"] == parent]
                        fig = px.pie(
                            sub, values=s_amt, names="__type",
                            title=f"<b>{parent}</b>", hole=0.4,
                            color="__type",
                            color_discrete_map={"벌크(무포장)": "#27ae60", "일반(포장)": "#e74c3c"}
                        )
                        fig.update_layout(showlegend=True, height=280, margin=dict(t=40, b=0, l=0, r=0))
                        with cols[i % 2]:
                            st.plotly_chart(fig, use_container_width=True)

elif menu == "📢 이음":
    tab_m0, tab_m1, tab_m2 = st.tabs(["⚡ 단골매칭 & 발송", "🎯 판매 기반 타겟팅", "🔍 회원 직접 검색"])

    df_mem = None
    if os.path.exists(SERVER_MEMBER_FILE):
        try:
            with open(SERVER_MEMBER_FILE, "rb") as f: df_mem, _ = load_smart(f, "member")
        except: pass

    with tab_m0:
        st.markdown("### ⚡ 단골매칭 → 즉시 발송")
        with st.expander("📂 판매 데이터 업로드", expanded=True):
            up_loyal = st.file_uploader("판매 실적 파일", type=["xlsx", "csv"], key="loyal_up")
        if up_loyal:
            df_sp, _ = load_smart(up_loyal, "sales")
            if df_sp is not None:
                c_date   = next((c for c in df_sp.columns if any(x in c for x in ["일시","날짜","date","Date"])), None)
                c_farmer = next((c for c in df_sp.columns if any(x in c for x in ["농가","공급자","생산자"])), None)
                c_item   = next((c for c in df_sp.columns if any(x in c for x in ["상품","품목"])), None)
                c_member = (next((c for c in df_sp.columns if "회원번호" in c), None) or next((c for c in df_sp.columns if c == "회원"), None))
                if c_date and c_farmer and c_member:
                    oc1, oc2 = st.columns(2)
                    sel_period2 = oc1.selectbox("분석 기간", ["최근 1개월", "최근 3개월", "최근 6개월"], index=1)
                    min_cnt     = oc2.number_input("최소 구매횟수", min_value=1, max_value=20, value=4)
                    pass 

    with tab_m1: st.write("판매 기반 타겟팅")
    with tab_m2: st.write("회원 직접 검색")

# ══════════════════════════════════════════
# 수파베이스 현장 요청 대시보드 (공통 하단)
# ══════════════════════════════════════════
st.write("---") 
st.subheader("📋 실시간 현장 요청 목록 (수파베이스)")

try:
    # 1. 수파베이스 연결 설정
    url: str = st.secrets["supabase"]["url"]
    key: str = st.secrets["supabase"]["key"]
    supabase: Client = create_client(url, key)

    # 2. staff_data 표에서 데이터를 가져오되, 최신순(created_at 내림차순)으로 정렬
    response = supabase.table("staff_data").select("*").order("created_at", desc=True).execute()
    data = response.data
    
    if data:
        # 3. 가져온 데이터를 엑셀 표(데이터프레임) 형태로 변환
        df = pd.DataFrame(data)
        
        # 보기 좋게 한글 이름으로 열 제목 변경
        df = df.rename(columns={
            "created_at": "접수시간",
            "item_name": "품목명",
            "farmer_name": "농가명",
            "urgency": "긴급도",
            "content": "내용"
        })
        
        # 4. 화면에 표 그리기 (불필요한 id 컬럼은 숨김)
        st.dataframe(df[["접수시간", "품목명", "농가명", "긴급도", "내용"]], use_container_width=True)
    else:
        st.info("들어온 현장 요청이 없습니다.")
        
except Exception as e:
    st.error(f"❌ 수파베이스 데이터를 불러오는 중 오류가 발생했습니다: {e}")
