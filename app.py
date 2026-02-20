import streamlit as st
import pandas as pd
import io, os, re, time, hmac, hashlib, uuid, datetime, requests
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# ══════════════════════════════════════════
# 설정
# ══════════════════════════════════════════
SERVER_CONTACT_FILE = "농가관리 목록_20260208 (전체).xlsx"
SERVER_MEMBER_FILE  = "회원관리(전체).xlsx"
APPSHEET_REQUEST_FILE = "발주요청_appsheet.xlsx"  # 앱시트 연동용 (향후)

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

def send_and_log(name, phone, text):
    if not st.session_state.get("api_key"): st.error("API Key 없음"); return False
    ok, res = send_sms(
        st.session_state.api_key, st.session_state.api_secret,
        st.session_state.sender_number, phone, text
    )
    st.session_state.sms_history.insert(0, {
        "시간": datetime.datetime.now().strftime("%H:%M:%S"),
        "수신자": name, "번호": phone,
        "결과": "✅" if ok else "❌",
        "비고": "" if ok else res.get("errorMessage", "")
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
    return s_item, s_qty, s_amt, s_farmer, s_spec, s_date

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

def get_secret(k, fb=""):
    try: return st.secrets.get(k, fb)
    except: return fb

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
    ("field_requests", []),   # 현장요청 임시저장
]:
    if k not in st.session_state:
        st.session_state[k] = v

# ══════════════════════════════════════════
# 인증
# ══════════════════════════════════════════
saved_pw = get_secret("APP_PASSWORD", "")
url_pw = st.query_params.get("pw", "")
if saved_pw == "poom0118**" or url_pw == "poom0118**":
    st.session_state.auth_passed = True

if not st.session_state.auth_passed:
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;700;900&display=swap');
    * { font-family: 'Noto Sans KR', sans-serif; }
    .login-wrap { display:flex; flex-direction:column; align-items:center; justify-content:center; height:80vh; }
    .login-title { font-size:2.5rem; font-weight:900; color:#1a1a1a; letter-spacing:-2px; }
    .login-sub { color:#888; margin-top:0.5rem; font-size:0.95rem; }
    </style>
    <div class="login-wrap">
    <div class="login-title">🌿 시다 워크</div>
    <div class="login-sub">품앗이생협 업무 자동화 시스템</div>
    </div>
    """, unsafe_allow_html=True)
    pw = st.text_input("비밀번호", type="password", autocomplete="current-password")
    if pw == "poom0118**":
        st.session_state.auth_passed = True
        st.rerun()
    elif pw:
        st.error("비밀번호가 다릅니다.")
    st.stop()

# ══════════════════════════════════════════
# 페이지 설정 & 스타일
# ══════════════════════════════════════════
st.set_page_config(
    page_title="시다 워크",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;700;900&family=Space+Mono&display=swap');

* { font-family: 'Noto Sans KR', sans-serif; }
code, .mono { font-family: 'Space Mono', monospace; }

#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 1.5rem 2rem 2rem; }

/* 사이드바 */
section[data-testid="stSidebar"] {
    background: #0f1923;
    border-right: 1px solid #1e2d3d;
}
section[data-testid="stSidebar"] * { color: #c8d6e5 !important; }
section[data-testid="stSidebar"] .stTextInput input {
    background: #1a2735 !important;
    border: 1px solid #2d4057 !important;
    color: #fff !important;
    border-radius: 8px;
}

/* 메인 헤더 */
.main-header {
    display: flex;
    align-items: baseline;
    gap: 12px;
    margin-bottom: 1.5rem;
    padding-bottom: 1rem;
    border-bottom: 2px solid #f0f0f0;
}
.main-title { font-size: 1.6rem; font-weight: 900; color: #1a1a1a; letter-spacing: -1px; }
.main-badge {
    font-size: 0.7rem; font-weight: 700; background: #2d6a4f;
    color: white; padding: 3px 10px; border-radius: 20px; letter-spacing: 1px;
}

/* 카드 */
.metric-card {
    background: #fff;
    border: 1.5px solid #e8e8e8;
    border-radius: 16px;
    padding: 1.2rem 1.4rem;
    margin-bottom: 0.8rem;
}
.metric-card.urgent { border-color: #e74c3c; background: #fff8f8; }
.metric-card.normal { border-color: #27ae60; background: #f8fff9; }
.metric-card.low    { border-color: #bdc3c7; }

/* 우선순위 뱃지 */
.badge-urgent { background:#e74c3c; color:#fff; padding:2px 10px; border-radius:20px; font-size:0.72rem; font-weight:700; }
.badge-normal { background:#27ae60; color:#fff; padding:2px 10px; border-radius:20px; font-size:0.72rem; font-weight:700; }
.badge-low    { background:#95a5a6; color:#fff; padding:2px 10px; border-radius:20px; font-size:0.72rem; font-weight:700; }

/* 버튼 */
div.stButton > button {
    border-radius: 10px;
    font-weight: 700;
    font-size: 0.9rem;
    border: none;
    transition: all 0.2s;
}
div.stButton > button[kind="primary"] {
    background: #2d6a4f;
    color: white;
}
div.stButton > button[kind="primary"]:hover {
    background: #1e4d38;
    transform: translateY(-1px);
}

/* 탭 */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px;
    border-bottom: 2px solid #f0f0f0;
}
.stTabs [data-baseweb="tab"] {
    font-size: 0.9rem;
    font-weight: 700;
    padding: 0.5rem 1.2rem;
    border-radius: 8px 8px 0 0;
    color: #888;
}
.stTabs [aria-selected="true"] {
    color: #2d6a4f !important;
    background: #f0fdf4 !important;
    border-bottom: 2px solid #2d6a4f !important;
}

/* 구분선 */
.section-label {
    font-size: 0.75rem;
    font-weight: 700;
    color: #888;
    letter-spacing: 2px;
    text-transform: uppercase;
    margin: 1.2rem 0 0.6rem;
}

/* 현장요청 카드 */
.request-card {
    background: #fff9f0;
    border: 1.5px solid #f39c12;
    border-radius: 12px;
    padding: 0.8rem 1rem;
    margin-bottom: 0.5rem;
    display: flex;
    align-items: center;
    gap: 10px;
}

/* 자금 게이지 */
.budget-bar-wrap { background: #f0f0f0; border-radius: 20px; height: 10px; margin: 6px 0; }
.budget-bar { background: linear-gradient(90deg, #27ae60, #2ecc71); border-radius: 20px; height: 10px; transition: width 0.5s; }
.budget-bar.warn { background: linear-gradient(90deg, #e67e22, #f39c12); }
.budget-bar.danger { background: linear-gradient(90deg, #c0392b, #e74c3c); }

/* 농가 그룹 헤더 */
.farmer-header {
    background: #f8f9fa;
    border-left: 4px solid #2d6a4f;
    padding: 0.6rem 1rem;
    border-radius: 0 8px 8px 0;
    margin: 1rem 0 0.5rem;
    font-weight: 700;
    font-size: 0.95rem;
    color: #1a1a1a;
}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════
# 사이드바
# ══════════════════════════════════════════
with st.sidebar:
    st.markdown("### 🌿 시다 워크")
    st.caption("Ver 2.0 · 품앗이생협")
    st.divider()
    st.markdown('<div class="section-label">솔라피 설정</div>', unsafe_allow_html=True)
    st.session_state.api_key       = st.text_input("API Key",    value=st.session_state.api_key,       type="password", label_visibility="collapsed", placeholder="API Key")
    st.session_state.api_secret    = st.text_input("Secret",     value=st.session_state.api_secret,    type="password", label_visibility="collapsed", placeholder="API Secret")
    st.session_state.sender_number = st.text_input("발신번호",   value=st.session_state.sender_number, label_visibility="collapsed", placeholder="발신번호 (숫자만)")
    st.divider()
    with st.expander("📋 문자 전송 이력", expanded=False):
        if st.session_state.sms_history:
            log_df = pd.DataFrame(st.session_state.sms_history)
            st.dataframe(log_df, hide_index=True, use_container_width=True)
            st.download_button("📥 이력 다운로드",
                data=to_excel(log_df),
                file_name=f"발송이력_{datetime.datetime.now().strftime('%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
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

menu = st.radio(
    "", ["📦 발주", "♻️ 제로웨이스트", "📢 이음"],
    horizontal=True, label_visibility="collapsed"
)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════
# 📦 발주 탭 — 품앗이 방식 (현장요청 + 판매데이터 + 유동자금)
# ══════════════════════════════════════════════════════════════════
if menu == "📦 발주":

    tab_order, tab_field, tab_send = st.tabs(["🧮 발주서 생성", "📍 현장 요청", "📤 발주 발송"])

    # ── 농가 연락처 로드 ──
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
                    df_phone_map = df_ci.drop_duplicates(subset=["clean_farmer"])[
                    ["clean_farmer", "clean_phone", "clean_email"]
]
        except:
            pass

    # ══════════════════════
    # 탭1: 발주서 생성
    # ══════════════════════
    with tab_order:
        # ── 유동자금 입력 ──
        st.markdown('<div class="section-label">💰 유동자금 설정</div>', unsafe_allow_html=True)
        col_b1, col_b2, col_b3 = st.columns([2, 1, 1])
        with col_b1:
            budget = st.number_input(
                "현재 유동자금 (원)",
                min_value=0, value=st.session_state.get("budget", 3000000),
                step=100000, format="%d",
                help="발주 우선순위 산정에 사용됩니다 (판매금액 70% 회전율 기준)"
            )
            st.session_state.budget = budget
        with col_b2:
            safety = st.slider("안전계수", 1.0, 1.5, 1.1, step=0.1, help="발주량 = 평균판매량 × 안전계수")
        with col_b3:
            period_map = {"최근 1일": 1, "최근 3일": 3, "최근 7일": 7, "최근 14일": 14}
            sel_period = st.selectbox("집계기간", list(period_map.keys()), index=2)
            period_days = period_map[sel_period]

        # 유동자금 게이지 (임시 — 발주 합계 대비)
        if budget > 0:
            est_order_total = st.session_state.get("est_order_total", 0)
            ratio = min(est_order_total / budget, 1.0) if budget > 0 else 0
            bar_class = "danger" if ratio > 0.8 else "warn" if ratio > 0.5 else ""
            pct = int(ratio * 100)
            st.markdown(f"""
            <div style="font-size:0.8rem; color:#888; margin-bottom:2px;">
              예상 발주액: <b>{est_order_total:,.0f}원</b> / 유동자금: <b>{budget:,.0f}원</b> ({pct}% 사용)
            </div>
            <div class="budget-bar-wrap">
              <div class="budget-bar {bar_class}" style="width:{pct}%"></div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown('<div class="section-label">📂 판매 실적 업로드</div>', unsafe_allow_html=True)
        up_sales = st.file_uploader(
            "판매 실적 파일",
            type=["xlsx", "csv"],
            accept_multiple_files=True,
            key="ord_up",
            label_visibility="collapsed"
        )

        # ── 현장요청 병합 표시 ──
        field_reqs = st.session_state.get("field_requests", [])
        if field_reqs:
            st.markdown('<div class="section-label">📍 현장 요청 반영 중</div>', unsafe_allow_html=True)
            req_df = pd.DataFrame(field_reqs)
            st.dataframe(req_df, hide_index=True, use_container_width=True)

        if up_sales:
            parts = []
            for f in up_sales:
                d, _ = load_smart(f, "sales")
                if d is not None: parts.append(d)

            if parts:
                df_s = pd.concat(parts, ignore_index=True)
                s_item, s_qty, s_amt, s_farmer, s_spec, s_date = detect_cols(df_s.columns.tolist())

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
                            else: return "제외"

                        df_s["구분"] = df_s["clean_farmer"].apply(classify)
                        df_t = df_s[df_s["구분"] != "제외"].copy()
                    else:
                        df_t = df_s.copy()
                        df_t["구분"] = "일반업체"
                        df_t["clean_farmer"] = df_t[s_item].apply(norm_name)

                    df_t[s_qty] = df_t[s_qty].apply(to_num) if s_qty else 1
                    df_t[s_amt] = df_t[s_amt].apply(to_num)
                    df_t.loc[(df_t[s_qty] <= 0) & (df_t[s_amt] > 0), s_qty] = 1

                    # 기간 필터
                    if s_date:
                        df_t["__date"] = pd.to_datetime(df_t[s_date], errors="coerce")
                        cutoff = pd.Timestamp.now() - pd.Timedelta(days=period_days)
                        df_t = df_t[df_t["__date"] >= cutoff]

                    df_t["__disp"]   = df_t[s_item].apply(disp_name)
                    df_t["__parent"] = df_t[s_item].apply(parent_name)
                    df_t["__unit_kg"]  = df_t.apply(
                        lambda r: ext_kg(r.get(s_spec, "")) or ext_kg(r[s_item]), axis=1)
                    df_t["__total_kg"] = df_t["__unit_kg"] * df_t[s_qty]

                    farmer_col = s_farmer if s_farmer else "clean_farmer"
                    agg = df_t.groupby([farmer_col, "__disp", "구분", "__parent"]).agg(
                        {s_qty: "sum", s_amt: "sum", "__total_kg": "sum"}
                    ).reset_index()

                    # 연락처 병합
                    if not df_phone_map.empty:
                        agg["clean_farmer"] = agg[farmer_col].astype(str).str.replace(" ", "")
                        agg = pd.merge(agg, df_phone_map, on="clean_farmer", how="left")
                    else:
                        agg["clean_phone"] = ""
                        agg["clean_email"] = ""

                    agg.rename(columns={
                        farmer_col: "업체명", "__disp": "상품명",
                        s_qty: "판매량", s_amt: "총판매액"
                    }, inplace=True)
                    agg = agg[agg["총판매액"] > 0].sort_values(["업체명", "__parent", "상품명"])

                    # 발주량 계산
                    agg["발주_수량"] = np.ceil(agg["판매량"] * safety / period_days)
                    agg["발주_중량"] = np.ceil(agg["__total_kg"] * safety / period_days)

                    # ── 현장요청 가중치 반영 ──
                    urgent_items = set()
                    for req in field_reqs:
                        if req.get("긴급도") == "🔴 오늘 필요":
                            urgent_items.add(req.get("품목명", "").replace(" ", ""))

                    # ── 유동자금 기반 우선순위 ──
                    # 농가별 예상 발주액 = 총판매액의 70%
                    farmer_est = agg.groupby("업체명")["총판매액"].sum() * 0.7
                    farmer_est_df = farmer_est.reset_index()
                    farmer_est_df.columns = ["업체명", "예상발주액"]
                    agg = pd.merge(agg, farmer_est_df, on="업체명", how="left")

                    # 우선순위 점수
                    def calc_priority(row):
                        score = row["총판매액"] * 0.7  # 기본: 판매금액 70%
                        if row["상품명"].replace(" ", "") in urgent_items:
                            score *= 3  # 긴급요청 3배 가중치
                        return score

                    agg["우선순위점수"] = agg.apply(calc_priority, axis=1)

                    # 누적 예상 발주액 (우선순위 순)
                    agg_sorted = agg.sort_values("우선순위점수", ascending=False).copy()
                    agg_sorted["누적발주액"] = agg_sorted["예상발주액"].cumsum()
                    agg_sorted["예산내"] = agg_sorted["누적발주액"] <= budget

                    # 우선순위 레이블
                    def priority_label(row):
                        if row["상품명"].replace(" ", "") in urgent_items: return "🔴 긴급"
                        if row["예산내"]: return "🟢 권장"
                        return "⚪ 여유"

                    agg_sorted["발주상태"] = agg_sorted.apply(priority_label, axis=1)

                    # 예상 발주 합계 저장
                    est_total = agg_sorted[agg_sorted["예산내"]]["예상발주액"].sum()
                    st.session_state.est_order_total = est_total
                    st.session_state.order_df = agg_sorted  # 발송 탭에서 사용

                    # ── 결과 표시 ──
                    st.divider()

                    # 요약 메트릭
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("전체 품목", f"{len(agg_sorted)}건")
                    m2.metric("긴급 품목", f"{(agg_sorted['발주상태']=='🔴 긴급').sum()}건")
                    m3.metric("예산 내 품목", f"{agg_sorted['예산내'].sum()}건")
                    m4.metric("예상 발주액", f"{est_total:,.0f}원")

                    # 발주서 필터
                    show_status = st.multiselect(
                        "발주 상태 필터",
                        ["🔴 긴급", "🟢 권장", "⚪ 여유"],
                        default=["🔴 긴급", "🟢 권장"]
                    )
                    filtered = agg_sorted[agg_sorted["발주상태"].isin(show_status)]

                    # 농가별 그룹 표시
                    for farmer in filtered["업체명"].unique():
                        fd = filtered[filtered["업체명"] == farmer]
                        phone = fd["clean_phone"].iloc[0] if "clean_phone" in fd.columns else ""
                        email = fd["clean_email"].iloc[0] if "clean_email" in fd.columns else ""
                        farmer_total = fd["총판매액"].sum()

                        with st.expander(
                            f"🌾 {farmer}  |  {len(fd)}품목  |  {farmer_total:,.0f}원",
                            expanded=(fd["발주상태"] == "🔴 긴급").any()
                        ):
                            show_cols = ["발주상태", "상품명", "판매량", "발주_수량", "발주_중량", "총판매액"]
                            disp = fd[show_cols].copy()
                            disp["총판매액"] = disp["총판매액"].apply(lambda x: f"{x:,.0f}")
                            st.dataframe(disp, hide_index=True, use_container_width=True)

                            # 문자 미리보기
                            def sms_lines_f(df_src):
                                grp = df_src.groupby("__parent").agg(
                                    {"발주_수량": "sum", "발주_중량": "sum", "__total_kg": "sum"}
                                ).reset_index()
                                lines = []
                                for _, r in grp.iterrows():
                                    if r["__total_kg"] > 0:
                                        lines.append(f"- {r['__parent']}: {int(r['발주_중량'])}kg")
                                    else:
                                        lines.append(f"- {r['__parent']}: {int(r['발주_수량'])}개")
                                return lines

                            msg = "\n".join(
                                [f"[품앗이생협 발주 요청]"] +
                                sms_lines_f(fd) +
                                ["감사합니다 🙏"]
                            )

                            c1, c2 = st.columns([1, 2])
                            with c1:
                                in_ph = st.text_input("📞", value=phone or "", key=f"ph_{farmer}", label_visibility="collapsed")
                                in_em = st.text_input("📧", value=email or "", key=f"em_{farmer}", label_visibility="collapsed", placeholder="이메일 (없으면 문자)")
                                if st.button("🚀 발송", key=f"send_{farmer}", type="primary", use_container_width=True):
                                    if in_em and "@" in in_em:
                                        # 이메일 발송
                                        try:
                                            st.info(f"이메일 발송: {in_em} (서버 설정 필요)")
                                        except:
                                            st.error("이메일 발송 실패")
                                    elif in_ph:
                                        ok = send_and_log(farmer, clean_phone(in_ph), st.session_state.get(f"msg_{farmer}", msg))
                                        if ok:
                                            st.session_state.sent_history.add(farmer)
                                            st.success("✅ 발송 완료")
                                            time.sleep(0.5)
                                            st.rerun()
                                        else:
                                            st.error("❌ 발송 실패")
                                    else:
                                        st.warning("연락처를 입력해주세요")
                            with c2:
                                st.text_area(
                                    "발주 내용",
                                    value=msg, height=160,
                                    key=f"msg_{farmer}",
                                    label_visibility="collapsed"
                                )

                    # 다운로드
                    dl_cols = ["발주상태", "업체명", "상품명", "판매량", "발주_수량", "발주_중량", "총판매액", "예상발주액"]
                    dl_cols = [c for c in dl_cols if c in agg_sorted.columns]
                    st.download_button(
                        "📥 발주서 전체 다운로드",
                        data=to_excel(agg_sorted[dl_cols]),
                        file_name=f"발주서_{datetime.datetime.now().strftime('%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
                else:
                    st.error("데이터 컬럼을 인식할 수 없습니다. 파일을 확인해주세요.")

    # ══════════════════════
    # 탭2: 현장 요청 (앱시트 대체 — 직접 입력)
    # ══════════════════════
    with tab_field:
        st.markdown("""
        <div style="background:#fff9f0; border:1.5px solid #f39c12; border-radius:12px; padding:1rem 1.2rem; margin-bottom:1rem;">
        <b>📍 현장 요청 입력</b><br>
        <span style="font-size:0.85rem; color:#666;">매장에서 떨어진 물건, 조합원 요청 등을 여기 입력하면 발주서에 자동 반영됩니다.</span>
        </div>
        """, unsafe_allow_html=True)

        with st.form("field_request_form", clear_on_submit=True):
            fc1, fc2, fc3 = st.columns([3, 2, 2])
            req_item    = fc1.text_input("품목명 *", placeholder="예: 감자, 두부, 달걀")
            req_farmer  = fc2.text_input("농가명 (알면)", placeholder="예: 행복농장")
            req_urgent  = fc3.selectbox("긴급도", ["🔴 오늘 필요", "🟡 이번 주", "🟢 여유 있음"])
            req_note    = st.text_input("메모", placeholder="예: 3번 조합원님 요청, 빠르게 필요")
            submitted   = st.form_submit_button("➕ 요청 추가", type="primary", use_container_width=True)

            if submitted and req_item:
                st.session_state.field_requests.append({
                    "품목명": req_item,
                    "농가명": req_farmer or "미지정",
                    "긴급도": req_urgent,
                    "메모": req_note,
                    "입력시간": datetime.datetime.now().strftime("%H:%M")
                })
                st.success(f"✅ '{req_item}' 요청이 추가되었습니다!")

        # 현재 요청 목록
        if st.session_state.field_requests:
            st.markdown('<div class="section-label">현재 요청 목록</div>', unsafe_allow_html=True)
            req_df = pd.DataFrame(st.session_state.field_requests)
            st.dataframe(req_df, hide_index=True, use_container_width=True)

            col_dl, col_cl = st.columns(2)
            with col_dl:
                st.download_button(
                    "📥 요청 목록 저장",
                    data=to_excel(req_df),
                    file_name=f"현장요청_{datetime.datetime.now().strftime('%m%d_%H%M')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            with col_cl:
                if st.button("🗑 전체 초기화", use_container_width=True):
                    st.session_state.field_requests = []
                    st.rerun()

            # 개별 삭제
            with st.expander("개별 삭제"):
                for i, req in enumerate(st.session_state.field_requests):
                    col_r, col_d = st.columns([4, 1])
                    col_r.write(f"{req['긴급도']} {req['품목명']} ({req['농가명']})")
                    if col_d.button("삭제", key=f"del_{i}"):
                        st.session_state.field_requests.pop(i)
                        st.rerun()
        else:
            st.info("아직 현장 요청이 없습니다. 위 양식으로 추가해보세요.")

    # ══════════════════════
    # 탭3: 일괄 발송
    # ══════════════════════
    with tab_send:
        if "order_df" not in st.session_state or st.session_state.order_df is None:
            st.info("먼저 '발주서 생성' 탭에서 판매데이터를 업로드해주세요.")
        else:
            agg_s = st.session_state.order_df
            urgent_only = st.checkbox("긴급 품목만 발송", value=True)
            if urgent_only:
                send_df = agg_s[agg_s["발주상태"] == "🔴 긴급"]
            else:
                send_df = agg_s[agg_s["발주상태"].isin(["🔴 긴급", "🟢 권장"])]

            unsent = [f for f in send_df["업체명"].unique() if f not in st.session_state.sent_history]
            st.metric("미발송 농가", f"{len(unsent)}곳")

            if unsent:
                if st.button(f"🚀 {len(unsent)}곳 일괄 발송", type="primary", use_container_width=True):
                    if not st.session_state.api_key:
                        st.error("사이드바에 API Key를 입력해주세요.")
                    else:
                        bar = st.progress(0)
                        for i, farmer in enumerate(unsent):
                            fd = send_df[send_df["업체명"] == farmer]
                            phone = fd["clean_phone"].iloc[0] if "clean_phone" in fd.columns else ""
                            if not phone: continue
                            items = fd["상품명"].tolist()
                            msg = f"[품앗이생협 발주]\n" + "\n".join([f"- {it}" for it in items]) + "\n감사합니다 🙏"
                            ok = send_and_log(farmer, clean_phone(phone), msg)
                            if ok: st.session_state.sent_history.add(farmer)
                            bar.progress((i + 1) / len(unsent))
                            time.sleep(0.3)
                        st.success("✅ 일괄 발송 완료!")
            else:
                st.success("✅ 모든 농가에 발송이 완료되었습니다!")

# ══════════════════════════════════════════
# ♻️ 제로웨이스트
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
            s_item, s_qty, s_amt, s_farmer, s_spec, _ = detect_cols(df_zw.columns.tolist())
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
                    st.download_button(
                        "📥 분석결과 다운로드",
                        data=to_excel(tdf),
                        file_name=f"제로웨이스트_{datetime.datetime.now().strftime('%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    st.markdown(f"**총 {len(bulk_items)}개 벌크 품목**")
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
            else:
                st.error("데이터 형식 확인 불가")

# ══════════════════════════════════════════
# 📢 이음 (마케팅)
# ══════════════════════════════════════════
elif menu == "📢 이음":
    tab_m0, tab_m1, tab_m2 = st.tabs(["⚡ 단골매칭 & 발송", "🎯 판매 기반 타겟팅", "🔍 회원 직접 검색"])

    # 회원DB 로드
    df_mem = None
    if os.path.exists(SERVER_MEMBER_FILE):
        try:
            with open(SERVER_MEMBER_FILE, "rb") as f:
                df_mem, _ = load_smart(f, "member")
        except:
            pass

    # ── ⚡ 단골매칭 & 즉시발송 ──
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
                c_member = (next((c for c in df_sp.columns if "회원번호" in c), None) or
                            next((c for c in df_sp.columns if c == "회원"), None))

                if not c_date or not c_farmer or not c_member:
                    st.error(f"컬럼 감지 실패. 컬럼 목록: {list(df_sp.columns)}")
                else:
                    with st.container(border=True):
                        oc1, oc2 = st.columns(2)
                        period_map2 = {"최근 1개월": 30, "최근 3개월": 90, "최근 6개월": 180}
                        sel_period2 = oc1.selectbox("분석 기간", list(period_map2.keys()), index=1)
                        min_cnt     = oc2.number_input("최소 구매횟수", min_value=1, max_value=20, value=4)

                    df_sp["__date"] = pd.to_datetime(df_sp[c_date], errors="coerce")
                    df_sp = df_sp.dropna(subset=["__date"])
                    cutoff2 = pd.Timestamp.now() - pd.Timedelta(days=period_map2[sel_period2])
                    df_filtered = df_sp[df_sp["__date"] >= cutoff2].copy()

                    farmers = sorted(df_filtered[c_farmer].dropna().unique().tolist())
                    sel_farmer = st.selectbox("🌾 농가 선택", farmers, key="loyal_farmer")
                    df_f = df_filtered[df_filtered[c_farmer] == sel_farmer].copy()

                    loyal_counts = df_f.groupby(c_member).size().reset_index(name="구매횟수")
                    loyal_counts = loyal_counts[loyal_counts["구매횟수"] >= min_cnt]
                    items_str = ", ".join(df_f[c_item].dropna().unique().tolist()[:5]) if c_item else ""

                    df_valid = pd.DataFrame()
                    mm_name = mm_phone = None
                    if df_mem is not None:
                        mm_id    = next((c for c in df_mem.columns if "회원번호" in c or "아이디" in c), None)
                        mm_phone = next((c for c in df_mem.columns if "휴대전화" in c or "전화" in c), None)
                        mm_name  = next((c for c in df_mem.columns if "이름" in c or "회원명" in c), None)
                        if mm_id and mm_phone:
                            merged = pd.merge(
                                loyal_counts,
                                df_mem[[mm_id, mm_phone] + ([mm_name] if mm_name else [])],
                                left_on=c_member, right_on=mm_id, how="left"
                            )
                            merged["전화번호_정제"] = merged[mm_phone].apply(clean_phone)
                            df_valid = merged[merged["전화번호_정제"] != ""].reset_index(drop=True)

                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.metric("발송 대상", f"{len(df_valid)}명")
                        st.metric("총 구매횟수", f"{loyal_counts['구매횟수'].sum()}회")
                    with col2:
                        if items_str: st.info(f"📋 품목: {items_str}")
                        st.caption(f"{sel_period2} / {min_cnt}회 이상 기준")

                    if not df_valid.empty:
                        show_cols = [c for c in [c_member, mm_name, mm_phone, "구매횟수"] if c]
                        with st.expander("👥 발송 대상 미리보기"):
                            st.dataframe(df_valid[show_cols].head(30), hide_index=True, use_container_width=True)
                        st.download_button(
                            "📥 대상자 엑셀",
                            data=to_excel(df_valid),
                            file_name=f"단골_{sel_farmer}_{datetime.datetime.now().strftime('%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                        st.divider()
                        default_msg = f"안녕하세요, 품앗이생협입니다 😊\n{sel_farmer}의 {items_str} 특가 안내드립니다!\n\n자세한 내용은 지족점으로 문의 주세요."
                        msg_input = st.text_area("📝 발송 메시지", value=default_msg, height=150, key="loyal_msg")
                        st.caption(f"💬 {len(msg_input)}자 {'⚠️ 90자 초과 (장문 요금)' if len(msg_input) > 90 else '✅ 단문'}")

                        if st.button(f"🚀 {len(df_valid)}명에게 즉시 발송", type="primary", use_container_width=True):
                            if not st.session_state.api_key:
                                st.error("사이드바에 API Key를 입력해주세요.")
                            elif not msg_input.strip():
                                st.error("메시지를 입력해주세요.")
                            else:
                                bar = st.progress(0)
                                success, fail = 0, 0
                                for i in range(len(df_valid)):
                                    name_val = str(df_valid.iloc[i].get(mm_name, sel_farmer)) if mm_name else sel_farmer
                                    ok = send_and_log(name_val, df_valid.iloc[i]["전화번호_정제"], msg_input)
                                    if ok: success += 1
                                    else: fail += 1
                                    bar.progress((i + 1) / len(df_valid))
                                    time.sleep(0.3)
                                st.success(f"✅ 완료! 성공 {success}명 / 실패 {fail}명")
                    else:
                        st.warning("조건에 맞는 단골이 없어요. 기간을 늘리거나 횟수를 줄여보세요.")
        else:
            st.info("💡 판매 데이터를 업로드하세요.")

    # ── 🎯 판매 기반 타겟팅 ──
    with tab_m1:
        with st.expander("📂 타겟팅용 판매 데이터", expanded=True):
            up_mkt = st.file_uploader("판매내역", type=["xlsx", "csv"], key="mkt_s")
        df_ms, _ = load_smart(up_mkt, "sales") if up_mkt else (None, None)
        df_mm = None
        if os.path.exists(SERVER_MEMBER_FILE):
            try:
                with open(SERVER_MEMBER_FILE, "rb") as f:
                    df_mm, _ = load_smart(f, "member")
            except:
                pass
        final_df = pd.DataFrame()
        if df_ms is not None:
            ms_farmer = next((c for c in df_ms.columns if any(x in c for x in ["농가","공급자"])), None)
            ms_item   = next((c for c in df_ms.columns if any(x in c for x in ["상품","품목"])), None)
            ms_buyer  = next((c for c in df_ms.columns if any(x in c for x in ["회원","구매자"])), None)
            if ms_farmer and ms_buyer:
                sel_f = st.selectbox("농가 선택", sorted(df_ms[ms_farmer].astype(str).unique()))
                tdf2 = df_ms[df_ms[ms_farmer] == sel_f]
                if ms_item:
                    sel_i = st.selectbox("상품 선택", ["전체"] + sorted(tdf2[ms_item].astype(str).unique()))
                    if sel_i != "전체": tdf2 = tdf2[tdf2[ms_item] == sel_i]
                loyal2 = tdf2.groupby(ms_buyer).size().reset_index(name="구매횟수").sort_values("구매횟수", ascending=False)
                if df_mm is not None:
                    mm_n = next((c for c in df_mm.columns if any(x in c for x in ["이름","회원명"])), None)
                    mm_p = next((c for c in df_mm.columns if any(x in c for x in ["휴대전화","전화"])), None)
                    if mm_n and mm_p:
                        loyal2["key"] = loyal2[ms_buyer].astype(str).str.replace(" ", "")
                        df_mm["key"]  = df_mm[mm_n].astype(str).str.replace(" ", "")
                        final_df = pd.merge(loyal2, df_mm.drop_duplicates(subset=["key"]), on="key", how="left")[[ms_buyer, mm_p, "구매횟수"]]
                        final_df.columns = ["이름", "전화번호", "구매횟수"]
        if not final_df.empty:
            st.write(f"수신자: {len(final_df)}명")
            st.download_button("📥 대상자 엑셀", data=to_excel(final_df),
                file_name=f"타겟팅_{datetime.datetime.now().strftime('%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            msg_txt = st.text_area("보낼 내용", key="mkt_msg")
            if st.button("🚀 전체 발송", type="primary", use_container_width=True):
                if not st.session_state.api_key:
                    st.error("API Key 필요")
                else:
                    bar = st.progress(0)
                    for i, r in enumerate(final_df.itertuples()):
                        send_and_log(r.이름, r.전화번호, msg_txt)
                        bar.progress((i + 1) / len(final_df))
                    st.success("발송 완료!")

    # ── 🔍 회원 직접 검색 ──
    with tab_m2:
        df_mm2 = None
        if os.path.exists(SERVER_MEMBER_FILE):
            try:
                with open(SERVER_MEMBER_FILE, "rb") as f:
                    df_mm2, _ = load_smart(f, "member")
            except:
                pass
        if df_mm2 is not None:
            q = st.text_input("이름 또는 전화번호 검색")
            if q:
                mm_n = next((c for c in df_mm2.columns if any(x in c for x in ["이름","회원명"])), None)
                mm_p = next((c for c in df_mm2.columns if any(x in c for x in ["휴대전화","전화"])), None)
                if mm_n and mm_p:
                    df_mm2["cn"] = df_mm2[mm_n].astype(str).str.replace(" ", "")
                    df_mm2["cp"] = df_mm2[mm_p].apply(clean_phone)
                    res = df_mm2[df_mm2["cn"].str.contains(q) | df_mm2["cp"].str.contains(q)]
                    if not res.empty:
                        fd2 = res[[mm_n, mm_p]].copy()
                        fd2.columns = ["이름", "전화번호"]
                        st.write(f"검색결과: {len(fd2)}명")
                        st.download_button("📥 검색결과 엑셀", data=to_excel(fd2),
                            file_name=f"검색_{datetime.datetime.now().strftime('%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        msg2 = st.text_area("보낼 내용", key="search_msg")
                        if st.button("🚀 전체 발송", type="primary", use_container_width=True):
                            if not st.session_state.api_key:
                                st.error("API Key 필요")
                            else:
                                bar = st.progress(0)
                                for i, r in enumerate(fd2.itertuples()):
                                    send_and_log(r.이름, r.전화번호, msg2)
                                    bar.progress((i + 1) / len(fd2))
                                st.success("발송 완료!")
        else:
            st.info("서버에 회원관리 파일이 없습니다.")
