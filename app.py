import streamlit as st
import pandas as pd
import io, os, re, time, hmac, hashlib, uuid, datetime, requests
import numpy as np
import plotly.express as px

SERVER_CONTACT_FILE = "농가관리 목록_20260208 (전체).xlsx"
SERVER_MEMBER_FILE  = "회원관리(전체).xlsx"

if "sms_history" not in st.session_state: st.session_state.sms_history = []

def send_sms(api_key, api_secret, sender, receiver, text):
    try:
        to = re.sub(r"[^0-9]", "", str(receiver))
        fr = re.sub(r"[^0-9]", "", str(sender))
        if not to or not fr: return False, {"errorMessage": "번호 오류"}
        date = datetime.datetime.now(datetime.timezone.utc).isoformat()
        salt = str(uuid.uuid4())
        sig  = hmac.new(api_secret.encode(), (date+salt).encode(), hashlib.sha256).hexdigest()
        headers = {"Authorization": f"HMAC-SHA256 apiKey={api_key}, date={date}, salt={salt}, signature={sig}", "Content-Type": "application/json"}
        res = requests.post("https://api.coolsms.co.kr/messages/v4/send",
                            json={"message": {"to": to, "from": fr, "text": text}}, headers=headers)
        return (True, res.json()) if res.status_code == 200 else (False, res.json())
    except Exception as e: return False, {"errorMessage": str(e)}

def send_and_log(name, phone, text):
    if not st.session_state.api_key: st.error("API Key 없음"); return False
    ok, res = send_sms(st.session_state.api_key, st.session_state.api_secret, st.session_state.sender_number, phone, text)
    st.session_state.sms_history.insert(0, {"시간": datetime.datetime.now().strftime("%H:%M:%S"), "수신자": name, "번호": phone,
        "결과": "✅" if ok else "❌", "비고": "" if ok else res.get("errorMessage","")})
    return ok

def clean_phone(phone):
    if pd.isna(phone) or str(phone).strip() in ["-","","nan"]: return ""
    n = re.sub(r"[^0-9]", "", str(phone))
    if n.startswith("10") and len(n) >= 10: n = "0" + n
    return n

@st.cache_data
def load_smart(file_obj, type="sales"):
    if file_obj is None: return None, "없음"
    df_raw = None
    try: df_raw = pd.read_excel(file_obj, header=None, engine="openpyxl")
    except:
        try:
            if hasattr(file_obj, "seek"): file_obj.seek(0)
            df_raw = pd.read_csv(file_obj, header=None, encoding="utf-8")
        except: return None, "읽기 실패"
    kws = ["농가","공급자","생산자","상품","품목"] if type=="sales" else ["회원번호","이름","휴대전화"] if type=="member" else ["농가명","휴대전화"]
    tgt = -1
    for idx, row in df_raw.head(20).iterrows():
        if sum(1 for k in kws if k in row.astype(str).str.cat(sep=" ")) >= 2: tgt = idx; break
    if tgt != -1:
        df = df_raw.iloc[tgt+1:].copy()
        df.columns = df_raw.iloc[tgt]
        df.columns = df.columns.astype(str).str.replace(" ","").str.replace("\n","")
        return df.loc[:, ~df.columns.str.contains("^Unnamed")], None
    try:
        if hasattr(file_obj,"seek"): file_obj.seek(0)
        return pd.read_excel(file_obj) if (hasattr(file_obj,"name") and file_obj.name.endswith("xlsx")) else pd.read_csv(file_obj), "헤더 못 찾음"
    except: return df_raw, "헤더 못 찾음"

def to_num(x):
    try:
        s = re.sub(r"[^0-9.-]","",str(x))
        return float(s) if s not in ["","."] else 0
    except: return 0

def detect_cols(cols):
    s_item   = next((c for c in cols if any(x in c for x in ["상품","품목"])), None)
    s_qty    = next((c for c in cols if any(x in c for x in ["판매수량","수량","개수"])), None)
    excl     = ["할인","반품","취소","면세","과세","부가세"]
    cands    = [c for c in cols if ("총" in c and ("판매" in c or "매출" in c))] +                [c for c in cols if (("판매" in c or "매출" in c) and ("액" in c or "금액" in c))] +                [c for c in cols if "금액" in c]
    s_amt    = next((c for c in cands if not any(b in c for b in excl)), None)
    s_farmer = next((c for c in cols if any(x in c for x in ["공급자","농가","생산자","거래처"])), None)
    s_spec   = next((c for c in cols if any(x in c for x in ["규격","단위","중량","용량"])), None)
    return s_item, s_qty, s_amt, s_farmer, s_spec

def to_excel(df):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w: df.to_excel(w, index=False)
    return buf.getvalue()

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

st.set_page_config(page_title="시다 워크", page_icon="🤖", layout="wide", initial_sidebar_state="collapsed")
st.markdown("""<style>
div.stButton > button { height:3.2rem; font-size:1.1rem; font-weight:700; border-radius:12px; }
.block-container { padding-top:3rem; padding-bottom:1rem; }
input, textarea { font-size:1rem !important; }
.stTabs [data-baseweb="tab"] { font-size:1rem; padding:0.6rem 1rem; }
#MainMenu {visibility:hidden;}
footer {visibility:hidden;}
header {visibility:hidden;}
</style>""", unsafe_allow_html=True)

if "sent_history" not in st.session_state: st.session_state.sent_history = set()

def get_secret(k, fb=""):
    try: return st.secrets.get(k, fb)
    except: return fb

if "api_key"       not in st.session_state: st.session_state.api_key       = get_secret("SOLAPI_API_KEY")
if "api_secret"    not in st.session_state: st.session_state.api_secret    = get_secret("SOLAPI_API_SECRET")
if "sender_number" not in st.session_state: st.session_state.sender_number = get_secret("SENDER_NUMBER")

with st.sidebar:
    st.markdown("## 🤖 시다 워크")
    st.caption("Ver 24.0")
    st.divider()
    saved_pw = get_secret("APP_PASSWORD", "")
    if saved_pw == "poom0118**":
        st.success("인증 완료 (자동)")
    else:
        pw = st.text_input("비밀번호", type="password", autocomplete="current-password")
        if pw != "poom0118**": st.warning("비밀번호를 입력하세요."); st.stop()
        st.success("인증 완료")
    st.divider()
    st.markdown("**🔑 솔라피 설정**")
    st.session_state.api_key       = st.text_input("API Key",       value=st.session_state.api_key,       type="password")
    st.session_state.api_secret    = st.text_input("API Secret",    value=st.session_state.api_secret,    type="password")
    st.session_state.sender_number = st.text_input("발신번호 (숫자만)", value=st.session_state.sender_number)
    st.divider()
    with st.expander("📋 문자 전송 이력", expanded=True):
        if st.session_state.sms_history:
            log_df = pd.DataFrame(st.session_state.sms_history)
            st.dataframe(log_df, hide_index=True, use_container_width=True)
            st.download_button("📥 이력 엑셀", data=to_excel(log_df),
                file_name=f"발송이력_{datetime.datetime.now().strftime('%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            if st.button("이력 초기화"): st.session_state.sms_history = []; st.rerun()
        else: st.caption("아직 전송 내역이 없습니다.")

st.title("🤖 시다 워크")
menu = st.radio("", ["📦 발주", "♻️ 제로웨이스트", "📢 이음"], horizontal=True)

# ══════════════════════════════════════════
# 📦 발주
# ══════════════════════════════════════════
if menu == "📦 발주":
    with st.container(border=True):
        c1,c2,c3,c4 = st.columns(4)
        safety    = c2.slider("안전계수", 1.0, 1.5, 1.1, step=0.1)
        show_all  = c4.checkbox("미등록 포함")

    with st.expander("📂 판매 실적 업로드", expanded=True):
        up_sales = st.file_uploader("판매 실적 파일", type=["xlsx","csv"], accept_multiple_files=True, key="ord_up")

    df_phone_map = pd.DataFrame()
    if os.path.exists(SERVER_CONTACT_FILE):
        try:
            with open(SERVER_CONTACT_FILE,"rb") as f: df_i, _ = load_smart(f,"info")
            if df_i is not None:
                i_name  = next((c for c in df_i.columns if "농가명" in c), None)
                i_phone = next((c for c in df_i.columns if "휴대전화" in c or "전화" in c), None)
                if i_name and i_phone:
                    df_i["clean_name"]  = df_i[i_name].astype(str).str.replace(" ","")
                    df_i["clean_phone"] = df_i[i_phone].apply(clean_phone)
                    df_phone_map = df_i.drop_duplicates(subset=["clean_name"])[["clean_name","clean_phone"]]
        except: pass

    df_s = None
    if up_sales:
        parts = []
        for f in up_sales:
            d, _ = load_smart(f,"sales")
            if d is not None: parts.append(d)
        if parts: df_s = pd.concat(parts, ignore_index=True)

    if df_s is not None:
        st.divider()
        s_item,s_qty,s_amt,s_farmer,s_spec = detect_cols(df_s.columns.tolist())
        if s_item and s_qty and s_amt:
            def norm(name):
                n = str(name).replace(" ","")
                if "지족" in n and "야채" in n: return "지족점야채"
                if "지족" in n and "과일" in n: return "지족점과일"
                if "지족" in n and "정육" in n: return "지족점정육"
                if "지족" in n and "공동" in n: return "지족점_공동구매"
                if "지족" in n and "매장" in n: return "지족매장"
                return re.sub(r"\(?벌크\)?","",n)
            if s_farmer:
                valid_set = {v.replace(" ","") for v in VALID_SUPPLIERS}
                df_s["clean_farmer"] = df_s[s_farmer].apply(norm)
                df_s[s_farmer] = df_s["clean_farmer"]
                def classify(name):
                    c = name.replace(" ","")
                    if "지족(Y)" in name or "지족(y)" in name: return "제외"
                    if "지족" in c: return "지족(사입)"
                    elif c in valid_set: return "일반업체"
                    else: return "제외" if not show_all else "일반업체(강제)"
                df_s["구분"] = df_s["clean_farmer"].apply(classify)
                df_t = df_s[df_s["구분"] != "제외"].copy()
                if not df_phone_map.empty:
                    df_t = pd.merge(df_t, df_phone_map, left_on="clean_farmer", right_on="clean_name", how="left")
                    df_t.rename(columns={"clean_phone":"전화번호"}, inplace=True)
                else: df_t["전화번호"] = ""
            else:
                df_t = df_s.copy(); df_t["구분"] = "일반업체"

            df_t[s_qty] = df_t[s_qty].apply(to_num)
            df_t[s_amt] = df_t[s_amt].apply(to_num)
            df_t.loc[(df_t[s_qty]<=0)&(df_t[s_amt]>0), s_qty] = 1

            def ext_kg(text):
                text = str(text).lower().replace(" ","")
                m = re.search(r"([\d\.]+)(kg)",text)
                if m:
                    try: return float(m.group(1))
                    except: pass
                m = re.search(r"([\d\.]+)(g)",text)
                if m:
                    try: return float(m.group(1))/1000
                    except: pass
                return 0.0

            if s_item:
                def calc_kg(row):
                    w = 0.0
                    if s_spec and pd.notna(row.get(s_spec)): w = ext_kg(row[s_spec])
                    if w==0 and pd.notna(row.get(s_item)): w = ext_kg(row[s_item])
                    return w
                df_t["__unit_kg"]  = df_t.apply(calc_kg, axis=1)
                df_t["__total_kg"] = df_t["__unit_kg"] * df_t[s_qty]
                def disp_name(x):
                    s = str(x).replace("*","")
                    return re.sub(r"\(\s*[\d\.]+\s*(?:g|kg|G|KG)\s*\)","",s).replace("()","").strip().replace(" ","")
                def parent_name(x):
                    s = str(x).replace("*","")
                    s = re.sub(r"\(?벌크\)?","",s); s = re.sub(r"\(?bulk\)?","",s,flags=re.IGNORECASE)
                    return re.sub(r"\(\s*[\d\.]+\s*(?:g|kg|G|KG)\s*\)","",s).replace("()","").strip().replace(" ","")
                df_t["__disp"]   = df_t[s_item].apply(disp_name)
                df_t["__parent"] = df_t[s_item].apply(parent_name)

            agg = df_t.groupby([s_farmer,"__disp","구분","__parent"]).agg({s_qty:"sum",s_amt:"sum","__total_kg":"sum"}).reset_index()
            if not df_phone_map.empty and s_farmer:
                agg["clean_farmer"] = agg[s_farmer].astype(str).str.replace(" ","")
                agg = pd.merge(agg, df_phone_map, left_on="clean_farmer", right_on="clean_name", how="left")
                agg.rename(columns={"clean_phone":"전화번호"}, inplace=True)
            else: agg["전화번호"] = ""
            agg.rename(columns={s_farmer:"업체명","__disp":"상품명",s_qty:"판매량",s_amt:"총판매액"}, inplace=True)
            agg = agg[agg["총판매액"]>0].sort_values(["업체명","__parent","상품명"])
            agg["발주_수량"] = np.ceil(agg["판매량"]*safety)
            agg["발주_중량"] = np.ceil(agg["__total_kg"]*safety)

            dl_cols = ["업체명","상품명","판매량","총판매액","발주_수량","발주_중량","전화번호"]
            st.download_button("📥 발주서 엑셀", data=to_excel(agg[dl_cols]),
                file_name=f"발주서_{datetime.datetime.now().strftime('%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

            tab1, tab2 = st.tabs(["🏢 외부업체","🏪 지족 사입"])

            def sms_lines(df_src):
                grp = df_src.groupby("__parent").agg({"발주_수량":"sum","발주_중량":"sum","__total_kg":"sum"}).reset_index()
                return [f"- {r['__parent']}: {int(r['발주_중량'])}kg" if r["__total_kg"]>0 else f"- {r['__parent']}: {int(r['발주_수량'])}개"
                        for _,r in grp.iterrows()]

            with tab1:
                df_ext = agg[agg["구분"].isin(["일반업체","일반업체(강제)"])].copy()
                if df_ext.empty: st.info("데이터 없음")
                else:
                    q = st.text_input("🔍 업체명 검색", key="s_ext")
                    targets = [v for v in sorted(df_ext["업체명"].unique()) if q in v] if q else sorted(df_ext["업체명"].unique())
                    for vendor in targets:
                        is_sent = vendor in st.session_state.sent_history
                        vd  = df_ext[df_ext["업체명"]==vendor]
                        msg = "\n".join([f"[{vendor} 발주]"]+sms_lines(vd)+["잘 부탁드립니다!"])
                        with st.expander(f"{'✅' if is_sent else '📩'} {vendor}", expanded=not is_sent):
                            st.dataframe(vd[["상품명","판매량","총판매액"]], hide_index=True, use_container_width=True)
                            c1,c2 = st.columns([1,2])
                            with c1:
                                ph = str(vd["전화번호"].iloc[0]) if not pd.isna(vd["전화번호"].iloc[0]) else ""
                                in_ph = st.text_input("📞", value=ph, key=f"p_{vendor}", label_visibility="collapsed")
                                if st.button("🚀 발송", key=f"b_{vendor}", type="primary", use_container_width=True):
                                    ok = send_and_log(vendor, clean_phone(in_ph), st.session_state.get(f"m_{vendor}", msg))
                                    if ok: st.session_state.sent_history.add(vendor); st.success("✅"); time.sleep(1); st.rerun()
                                    else: st.error("❌ 실패")
                            with c2:
                                st.text_area("내용", value=msg, height=180, key=f"m_{vendor}", label_visibility="collapsed")

            with tab2:
                df_int = agg[agg["구분"]=="지족(사입)"].copy()
                if df_int.empty: st.info("지족 사입 데이터가 없습니다.")
                else:
                    for mv in ["지족점야채","지족점과일","지족매장","지족점정육","지족점_공동구매"]:
                        dm = df_int[df_int["업체명"]==mv]
                        if dm.empty: continue
                        is_sent = mv in st.session_state.sent_history
                        with st.expander(f"{'✅' if is_sent else '🚚'} {mv} ({dm['총판매액'].sum():,.0f}원)", expanded=not is_sent):
                            d2 = dm.copy()
                            d2["발주"] = d2.apply(lambda x: f"{int(x['발주_중량'])}kg" if x["__total_kg"]>0 else f"{int(x['발주_수량'])}개", axis=1)
                            st.dataframe(d2[["상품명","발주","총판매액"]].assign(총판매액=d2["총판매액"].apply(lambda x:f"{x:,.0f}")), hide_index=True, use_container_width=True)
                            msg = "\n".join([f"안녕하세요 {mv}입니다.","","[발주 요청]"]+sms_lines(dm)+["","잘 부탁드립니다."])
                            c1,c2 = st.columns([1,2])
                            with c1:
                                ph = str(dm["전화번호"].iloc[0]) if not pd.isna(dm["전화번호"].iloc[0]) else ""
                                in_ph = st.text_input("📞", value=ph, key=f"p2_{mv}", label_visibility="collapsed")
                                if st.button("🚀 발송", key=f"b2_{mv}", type="primary", use_container_width=True):
                                    ok = send_and_log(mv, clean_phone(in_ph), st.session_state.get(f"m2_{mv}", msg))
                                    if ok: st.session_state.sent_history.add(mv); st.success("✅"); time.sleep(1); st.rerun()
                                    else: st.error("❌ 실패")
                            with c2:
                                st.text_area("내용", value=msg, height=350, key=f"m2_{mv}", label_visibility="collapsed")

# ══════════════════════════════════════════
# ♻️ 제로웨이스트
# ══════════════════════════════════════════
elif menu == "♻️ 제로웨이스트":
    st.markdown("### ♻️ 제로웨이스트 판매 분석")
    with st.expander("📂 판매 데이터 업로드", expanded=True):
        up_zw = st.file_uploader("판매 실적 파일", type=["xlsx","csv"], accept_multiple_files=True, key="zw_up")
    if up_zw:
        parts = []
        for f in up_zw:
            d, _ = load_smart(f,"sales")
            if d is not None: parts.append(d)
        if parts:
            df_zw = pd.concat(parts, ignore_index=True)
            s_item,s_qty,s_amt,s_farmer,s_spec = detect_cols(df_zw.columns.tolist())
            if s_item and s_amt:
                def parent_zw(x):
                    s = str(x)
                    s = re.sub(r"\(?벌크\)?","",s); s = re.sub(r"\(?bulk\)?","",s,flags=re.IGNORECASE)
                    return re.sub(r"\(.*?\)","",s).replace("*","").replace("()","").strip().replace(" ","")
                df_zw["__parent"] = df_zw[s_item].apply(parent_zw)
                df_zw[s_amt] = df_zw[s_amt].apply(to_num)
                def type_tag(row):
                    i = str(row[s_item]); f2 = str(row[s_farmer]) if s_farmer and pd.notna(row[s_farmer]) else ""
                    return "벌크(무포장)" if ("벌크" in i or "bulk" in i.lower() or "벌크" in f2) else "일반(포장)"
                df_zw["__type"] = df_zw.apply(type_tag, axis=1)
                grp = df_zw.groupby(["__parent","__type"])[s_amt].sum().reset_index()
                bulk_items = grp[grp["__type"]=="벌크(무포장)"]["__parent"].unique()
                tdf = grp[grp["__parent"].isin(bulk_items)].copy()
                st.divider()
                if len(bulk_items)==0:
                    st.info("벌크 데이터 없음")
                else:
                    st.download_button("📥 분석결과 엑셀", data=to_excel(tdf),
                        file_name=f"제로웨이스트_{datetime.datetime.now().strftime('%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                    st.markdown(f"**총 {len(bulk_items)}개 품목**")
                    cols = st.columns(2)
                    for i,parent in enumerate(sorted(tdf["__parent"].unique())):
                        sub = tdf[tdf["__parent"]==parent]
                        fig = px.pie(sub, values=s_amt, names="__type", title=f"<b>{parent}</b>", hole=0.4,
                                     color="__type", color_discrete_map={"벌크(무포장)":"#28a745","일반(포장)":"#dc3545"})
                        fig.update_layout(showlegend=True, height=280, margin=dict(t=40,b=0,l=0,r=0))
                        with cols[i%2]: st.plotly_chart(fig, use_container_width=True)
            else: st.error("데이터 형식 확인 불가")

# ══════════════════════════════════════════
# 📢 이음(마케팅)
# ══════════════════════════════════════════
elif menu == "📢 이음":
    tab_m0, tab_m1, tab_m2 = st.tabs(["⚡ 단골매칭 & 발송", "🎯 판매 기반 타겟팅", "🔍 회원 직접 검색"])

    # ── ⚡ 단골매칭 & 즉시발송 ──
    with tab_m0:
        st.markdown("### ⚡ 단골매칭 → 즉시 발송")

        df_mem = None
        if os.path.exists(SERVER_MEMBER_FILE):
            try:
                with open(SERVER_MEMBER_FILE,"rb") as f: df_mem, _ = load_smart(f,"member")
            except: pass

        with st.expander("📂 판매 데이터 업로드 (직매장_농가별_판매.xlsx)", expanded=True):
            up_loyal = st.file_uploader("판매 실적 파일", type=["xlsx","csv"], key="loyal_up")

        if up_loyal:
            df_sp, _ = load_smart(up_loyal, "sales")
            if df_sp is not None:
                c_date   = next((c for c in df_sp.columns if any(x in c for x in ["일시","날짜","date","Date"])), None)
                c_farmer = next((c for c in df_sp.columns if any(x in c for x in ["농가","공급자","생산자"])), None)
                c_item   = next((c for c in df_sp.columns if any(x in c for x in ["상품","품목"])), None)
                c_member = next((c for c in df_sp.columns if "회원번호" in c), None) or next((c for c in df_sp.columns if c == "회원"), None)

                if not c_date or not c_farmer or not c_member:
                    st.error(f"컬럼 감지 실패. 실제 컬럼: {list(df_sp.columns)}")
                else:
                    st.divider()
                    with st.container(border=True):
                        st.markdown("**🔧 매칭 조건 설정**")
                        oc1, oc2 = st.columns(2)
                        period_map = {"최근 1개월": 30, "최근 3개월": 90, "최근 6개월": 180, "최근 1년": 365}
                        sel_period = oc1.selectbox("📅 분석 기간", list(period_map.keys()), index=1)
                        min_cnt    = oc2.number_input("🔁 최소 구매횟수", min_value=1, max_value=20, value=4)

                    df_sp["__date"] = pd.to_datetime(df_sp[c_date], errors="coerce")
                    df_sp = df_sp.dropna(subset=["__date"])
                    cutoff = pd.Timestamp.now() - pd.Timedelta(days=period_map[sel_period])
                    df_filtered = df_sp[df_sp["__date"] >= cutoff].copy()

                    farmers = sorted(df_filtered[c_farmer].dropna().unique().tolist())
                    sel_farmer = st.selectbox("🌾 농가 선택", farmers, key="loyal_farmer")
                    df_f = df_filtered[df_filtered[c_farmer] == sel_farmer].copy()

                    loyal_counts = df_f.groupby(c_member).size().reset_index(name="구매횟수")
                    loyal_counts = loyal_counts[loyal_counts["구매횟수"] >= min_cnt]
                    items_str = ", ".join(df_f[c_item].dropna().unique().tolist()[:5]) if c_item else ""

                    df_valid = pd.DataFrame()
                    mm_name = None
                    mm_phone = None
                    if df_mem is not None:
                        mm_id    = next((c for c in df_mem.columns if "회원번호" in c or "아이디" in c), None)
                        mm_phone = next((c for c in df_mem.columns if "휴대전화" in c or "전화" in c), None)
                        mm_name  = next((c for c in df_mem.columns if "이름" in c or "회원명" in c), None)
                        st.caption(f"🔍 회원DB: {list(df_mem.columns)[:8]} | mm_id={mm_id} | mm_phone={mm_phone} | c_member={c_member}")
                        if mm_id and mm_phone:
                            merged = pd.merge(loyal_counts,
                                              df_mem[[mm_id, mm_phone]+([mm_name] if mm_name else [])],
                                              left_on=c_member, right_on=mm_id, how="left")
                            merged["전화번호_정제"] = merged[mm_phone].apply(clean_phone)
                            df_valid = merged[merged["전화번호_정제"] != ""].reset_index(drop=True)
                    else:
                        st.warning("서버에 회원관리 파일이 없어요.")

                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.metric("발송 대상", f"{len(df_valid)}명")
                        st.metric("총 구매횟수", f"{loyal_counts['구매횟수'].sum()}회")
                    with col2:
                        if items_str: st.info(f"📋 품목: {items_str}")
                        st.caption(f"{sel_period} / {min_cnt}회 이상 기준")

                    if not df_valid.empty:
                        show_cols = [c for c in [c_member, mm_name, mm_phone, "구매횟수"] if c]
                        with st.expander("👥 발송 대상 미리보기"):
                            st.dataframe(df_valid[show_cols].head(30), hide_index=True, use_container_width=True)
                        st.download_button("📥 대상자 엑셀", data=to_excel(df_valid),
                            file_name=f"단골_{sel_farmer}_{datetime.datetime.now().strftime('%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        st.divider()
                        default_msg = f"안녕하세요, 품앗이생협입니다 😊\n{sel_farmer}의 {items_str} 특가 안내드립니다!\n\n자세한 내용은 지족점으로 문의 주세요."
                        msg_input = st.text_area("📝 발송 메시지", value=default_msg, height=150, key="loyal_msg")
                        st.caption(f"💬 {len(msg_input)}자 {'⚠️ 90자 초과 (장문 요금)' if len(msg_input)>90 else '✅ 단문'}")
                        if st.button(f"🚀 {len(df_valid)}명에게 즉시 발송", type="primary", use_container_width=True, key="loyal_send"):
                            if not st.session_state.api_key: st.error("사이드바에 API Key를 입력해주세요.")
                            elif not msg_input.strip(): st.error("메시지를 입력해주세요.")
                            else:
                                bar = st.progress(0)
                                success, fail = 0, 0
                                for i in range(len(df_valid)):
                                    name_val = str(df_valid.iloc[i].get(mm_name, sel_farmer)) if mm_name else sel_farmer
                                    ok = send_and_log(name_val, df_valid.iloc[i]["전화번호_정제"], msg_input)
                                    if ok: success += 1
                                    else: fail += 1
                                    bar.progress((i+1)/len(df_valid))
                                    time.sleep(0.3)
                                st.success(f"✅ 완료! 성공 {success}명 / 실패 {fail}명")
                    else:
                        st.warning("조건에 맞는 단골이 없어요. 기간을 늘리거나 횟수를 줄여보세요.")
        else:
            st.info("💡 직매장 농가별 판매 엑셀을 업로드하세요.")

    # ── 🎯 판매 기반 타겟팅 ──
    with tab_m1:
        with st.expander("📂 타겟팅용 판매 데이터 업로드", expanded=True):
            up_mkt = st.file_uploader("판매내역", type=["xlsx","csv"], key="mkt_s")
        df_ms, _ = load_smart(up_mkt, "sales")
        df_mm = None
        if os.path.exists(SERVER_MEMBER_FILE):
            try:
                with open(SERVER_MEMBER_FILE,"rb") as f: df_mm, _ = load_smart(f,"member")
            except: pass
        final_df = pd.DataFrame()
        if df_ms is not None:
            ms_farmer = next((c for c in df_ms.columns if any(x in c for x in ["농가","공급자"])), None)
            ms_item   = next((c for c in df_ms.columns if any(x in c for x in ["상품","품목"])), None)
            ms_buyer  = next((c for c in df_ms.columns if any(x in c for x in ["회원","구매자"])), None)
            if ms_farmer and ms_buyer:
                sel_f = st.selectbox("농가 선택", sorted(df_ms[ms_farmer].astype(str).unique()))
                tdf = df_ms[df_ms[ms_farmer]==sel_f]
                if ms_item:
                    sel_i = st.selectbox("상품 선택", ["전체"]+sorted(tdf[ms_item].astype(str).unique()))
                    if sel_i != "전체": tdf = tdf[tdf[ms_item]==sel_i]
                loyal = tdf.groupby(ms_buyer).size().reset_index(name="구매횟수").sort_values("구매횟수", ascending=False)
                if df_mm is not None:
                    mm_n = next((c for c in df_mm.columns if any(x in c for x in ["이름","회원명"])), None)
                    mm_p = next((c for c in df_mm.columns if any(x in c for x in ["휴대전화","전화"])), None)
                    if mm_n and mm_p:
                        loyal["key"] = loyal[ms_buyer].astype(str).str.replace(" ","")
                        df_mm["key"] = df_mm[mm_n].astype(str).str.replace(" ","")
                        final_df = pd.merge(loyal, df_mm.drop_duplicates(subset=["key"]), on="key", how="left")[[ms_buyer,mm_p,"구매횟수"]]
                        final_df.columns = ["이름","전화번호","구매횟수"]
        if not final_df.empty:
            st.divider()
            st.write(f"수신자: {len(final_df)}명")
            st.download_button("📥 대상자 엑셀", data=to_excel(final_df),
                file_name=f"타겟팅_{datetime.datetime.now().strftime('%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            msg_txt = st.text_area("보낼 내용", key="mkt_msg")
            if st.button("🚀 전체 발송", type="primary", use_container_width=True, key="mkt_send"):
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
                with open(SERVER_MEMBER_FILE,"rb") as f: df_mm2, _ = load_smart(f,"member")
            except: pass
        if df_mm2 is not None:
            q = st.text_input("이름 또는 전화번호 검색")
            if q:
                mm_n = next((c for c in df_mm2.columns if any(x in c for x in ["이름","회원명"])), None)
                mm_p = next((c for c in df_mm2.columns if any(x in c for x in ["휴대전화","전화"])), None)
                if mm_n and mm_p:
                    df_mm2["cn"] = df_mm2[mm_n].astype(str).str.replace(" ","")
                    df_mm2["cp"] = df_mm2[mm_p].apply(clean_phone)
                    res = df_mm2[df_mm2["cn"].str.contains(q)|df_mm2["cp"].str.contains(q)]
                    if not res.empty:
                        fd = res[[mm_n,mm_p]].copy(); fd.columns = ["이름","전화번호"]
                        st.write(f"수신자: {len(fd)}명")
                        st.download_button("📥 검색결과 엑셀", data=to_excel(fd),
                            file_name=f"검색_{datetime.datetime.now().strftime('%m%d')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        msg2 = st.text_area("보낼 내용", key="search_msg")
                        if st.button("🚀 전체 발송", type="primary", use_container_width=True, key="search_send"):
                            if not st.session_state.api_key: st.error("API Key 필요")
                            else:
                                bar = st.progress(0)
                                for i, r in enumerate(fd.itertuples()):
                                    send_and_log(r.이름, r.전화번호, msg2)
                                    bar.progress((i+1)/len(fd))
                                st.success("발송 완료!")
        else:
            st.info("서버에 회원관리 파일이 없습니다.")
