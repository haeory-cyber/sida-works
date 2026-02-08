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
    "고삼농협", "금강향
