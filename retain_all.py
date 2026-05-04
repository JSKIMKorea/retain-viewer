"""
Azure Retain 전체 자동화 (DART 캐시 활용)
==========================================
1. Azure SQL → 엑셀 2개
2. dart_cache의 DART 데이터 로드 (API 호출 없음, 빠름)
3. HTML 뷰어에 임베드 → index.html
4. GitHub 자동 업로드

DART 데이터 갱신은 dart_update.py를 별도 실행하세요.

사용법: 더블클릭 또는 python retain_all.py
"""

import pyodbc, pandas as pd, requests as req_lib
from datetime import date, datetime, timezone, timedelta
import os, sys, json, re, base64

# ============================================================
# 환경 감지: GitHub Actions vs 로컬 PC
# ============================================================
IS_CI = os.getenv("GITHUB_ACTIONS") == "true"

if not IS_CI:
    # 로컬 PC: .env 파일에서 비밀번호 로드
    from dotenv import load_dotenv
    _SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    _ENV_DIR = os.path.join(_SCRIPT_DIR, "000.Azure&API")
    _ENV_LOADED = False
    for _env_name in [".env", ".env.txt", "env.txt", "env"]:
        _env_path = os.path.join(_ENV_DIR, _env_name)
        if os.path.exists(_env_path):
            load_dotenv(_env_path)
            _ENV_LOADED = True
            break
    if not _ENV_LOADED:
        print(f"\n❌ .env 파일을 찾을 수 없습니다!")
        print(f"   확인할 경로: {_ENV_DIR}\\.env")
        input("Enter..."); sys.exit(1)
else:
    # GitHub Actions: 환경변수가 이미 설정되어 있음 (Secrets)
    _SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    print("🤖 GitHub Actions 환경 감지")

# ============================================================
# 설정
# ============================================================
DB_CONFIG = {
    "server":   os.getenv("AZURE_SQL_SERVER"),
    "database": os.getenv("AZURE_SQL_DATABASE"),
    "username": os.getenv("AZURE_SQL_USERNAME"),
    "password": os.getenv("AZURE_SQL_PASSWORD"),
}
if not DB_CONFIG["server"]:
    print("\n❌ DB 접속 정보가 없습니다!")
    if not IS_CI: input("Enter...")
    sys.exit(1)

if IS_CI:
    # GitHub Actions: 리포 루트 기준 경로
    OUTPUT_DIR    = _SCRIPT_DIR
    DART_CACHE    = os.path.join(_SCRIPT_DIR, "dart_cache", "dart_details.json")
    NEWS_CACHE    = os.path.join(_SCRIPT_DIR, "news_cache", "daily_news.json")
    HTML_TEMPLATE = os.path.join(_SCRIPT_DIR, "Project_Allocation_Viewer.html")
    INDEX_OUTPUT  = os.path.join(_SCRIPT_DIR, "index.html")
    BCRYPT_VENDOR = os.path.join(_SCRIPT_DIR, "vendor", "bcrypt.min.js")
else:
    # 로컬 PC: 기존 경로
    OUTPUT_DIR    = os.path.join(_SCRIPT_DIR, "Raw data")
    DART_CACHE    = os.path.join(_SCRIPT_DIR, "dart_cache", "dart_details.json")
    NEWS_CACHE    = os.path.join(_SCRIPT_DIR, "news_cache", "daily_news.json")
    HTML_TEMPLATE = os.path.join(_SCRIPT_DIR, "02.html", "Project_Allocation_Viewer.html")
    INDEX_OUTPUT  = os.path.join(OUTPUT_DIR, "index.html")
    BCRYPT_VENDOR = os.path.join(_SCRIPT_DIR, "02.html", "vendor", "bcrypt.min.js")

# 사용자 관리 — 기업정보포털과 동일한 5컬럼(이메일/사번/이름/부서/활성) 구조
USERS_FILE = os.path.join(_SCRIPT_DIR, "2.사용자관리", "users.xlsx")

GITHUB_REPO   = os.getenv("GITHUB_REPO", "")
GITHUB_TOKEN  = os.getenv("GITHUB_TOKEN", "")
GITHUB_FILE   = "index.html"

# ============================================================
# Azure SQL
# ============================================================
def get_conn():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};Encrypt=yes;TrustServerCertificate=no;")

SQL = """
WITH src AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY YMD, PRJTCD, EMPNO, RETAIN, startdate, enddate, PRJTNM
    ORDER BY (SELECT NULL)
  ) AS DUP_SEQ
  FROM BI_STAFFREPORT_RETAIN_V
)
SELECT src.YMD, src.PRJTCD, src.EMPNO, src.RETAIN, src.PRJTNM,
  src.startdate AS ASSIGN_START, src.enddate AS ASSIGN_END,
  src.DUP_SEQ,
  e.EMPNM, e.ORG_NM AS CM_NM, g.GRADNM,
  COALESCE(ep.EMPNM,p.PTRNM,'') AS CHARGPTR,
  COALESCE(em.EMPNM,p.MGRNM,'') AS CHARGMGR
FROM src
INNER JOIN BI_STAFFREPORT_EMP_V e ON src.EMPNO=e.EMPNO
INNER JOIN BI_STAFFREPORT_GRADE_V g ON e.GRADCD=g.GRADCD
LEFT JOIN BI_STAFFREPORT_PRJT_V p ON src.PRJTCD=p.PRJTCD
LEFT JOIN BI_STAFFREPORT_EMP_V ep ON p.CHARGPTR=ep.EMPNO
LEFT JOIN BI_STAFFREPORT_EMP_V em ON p.CHARGMGR=em.EMPNO
WHERE e.ORG_NM IN ('Global CMAAS','IOA','Global IPO','Assurance NGH')"""

def fetch_data():
    print("Azure SQL 연결 중...")
    conn = get_conn()
    try:
        print("데이터 조회 중...")
        df = pd.read_sql(SQL, conn)
        print(f"  → {len(df):,}건 조회 완료")
        etl = pd.read_sql("SELECT TOP 1 CREATED_DATE FROM BI_STAFFREPORT_RETAIN_V ORDER BY CREATED_DATE DESC", conn)
    finally:
        conn.close()
    du = pd.to_datetime(etl.iloc[0,0]).strftime("%Y.%m.%d %H:%M:%S") if len(etl)>0 and pd.notna(etl.iloc[0,0]) else "?"
    print(f"  → 원본 갱신: {du}")
    return df, du

# ============================================================
# 데이터 가공
# ============================================================
def process_data(df):
    df = df.rename(columns={"EMPNM":"이름","GRADNM":"직급","EMPNO":"사번","CM_NM":"소속",
        "PRJTNM":"Project Name","YMD":"Start Date","RETAIN":"Time (Hours)",
        "CHARGMGR":"PM","PRJTCD":"Job Code","CHARGPTR":"EL"})
    df["End Date"]=df["Start Date"]
    df["Start Date"]=pd.to_datetime(df["Start Date"]); df["End Date"]=pd.to_datetime(df["End Date"])
    # DUP_SEQ from SQL CTE (소스 테이블 레벨 중복 번호)
    if "DUP_SEQ" not in df.columns:
        df["DUP_SEQ"]=1
    df["DUP_SEQ"]=pd.to_numeric(df["DUP_SEQ"],errors="coerce").fillna(1).astype(int)
    # Assign period → base_aid (다일 기간만, 단일일은 빈값)
    if "ASSIGN_START" in df.columns and "ASSIGN_END" in df.columns:
        df["ASSIGN_START"]=pd.to_datetime(df["ASSIGN_START"],errors="coerce")
        df["ASSIGN_END"]=pd.to_datetime(df["ASSIGN_END"],errors="coerce")
        multi=df["ASSIGN_START"]!=df["ASSIGN_END"]
        df["_base_aid"]=""
        df.loc[multi,"_base_aid"]=df.loc[multi,"ASSIGN_START"].dt.strftime("%Y%m%d").fillna("")+"~"+df.loc[multi,"ASSIGN_END"].dt.strftime("%Y%m%d").fillna("")
    else:
        df["_base_aid"]=""
    # Assign ID = base_aid + DUP_SEQ (DUP_SEQ>1이면 접미사 추가)
    df["Assign ID"]=df["_base_aid"]
    dup_mask=df["DUP_SEQ"]>1
    df.loc[dup_mask,"Assign ID"]=df.loc[dup_mask,"_base_aid"]+"_"+df.loc[dup_mask,"DUP_SEQ"].astype(str)
    df=df.drop(columns=["_base_aid","DUP_SEQ","ASSIGN_START","ASSIGN_END"],errors="ignore")
    df["사번"]=pd.to_numeric(df["사번"],errors="coerce")
    print(f"  → 컬럼 정리: {len(df):,}건")
    df["Client Code"]=df["Job Code"].astype(str).str.split("-").str[0]
    df["Client Code"]=pd.to_numeric(df["Client Code"],errors="coerce")
    # JOIN 아티팩트 제거 (DUP_SEQ 포함했으므로 소스 중복은 보존)
    dedup=["이름","직급","사번","소속","Project Name","Start Date","End Date","Time (Hours)","PM","Job Code","EL","Assign ID"]
    df=df.drop_duplicates(subset=dedup); print(f"  → 중복제거: {len(df):,}건")
    df["Project Name"]=df["Project Name"].fillna("기타Admin(교육 등)")
    grp=["이름","직급","사번","소속","Project Name","PM","Job Code","EL","Start Date","End Date","Assign ID"]
    agg=df.groupby(grp,dropna=False).agg(**{"Time (Hours)":("Time (Hours)","sum")}).reset_index()
    agg=agg.sort_values("Start Date").reset_index(drop=True)
    print(f"  → Summarize: {len(agg):,}건")
    return agg
    agg=df.groupby(grp,dropna=False).agg(**{"Time (Hours)":("Time (Hours)","sum")}).reset_index()
    agg=agg.sort_values("Start Date").reset_index(drop=True)
    print(f"  → Summarize: {len(agg):,}건")
    return agg

# ============================================================
# DART 캐시 로드 (API 호출 없음)
# ============================================================
def load_dart_cache():
    if not os.path.exists(DART_CACHE):
        print("\n⚠ DART 캐시 없음 → dart_update.py를 먼저 실행하세요.")
        print(f"  경로: {DART_CACHE}")
        return None
    with open(DART_CACHE, "r", encoding="utf-8") as f:
        data = json.load(f)
    updated = data.get("updated", "?")
    companies = len(data.get("companies", {}))
    projs = len(data.get("projMap", {}))
    print(f"\nDART 캐시 로드 완료")
    print(f"  → 기업 {companies}개, 프로젝트 매핑 {projs}개")
    print(f"  → 최종 갱신: {updated}")
    return data

# ============================================================
# 뉴스 캐시 로드 (없으면 None — 뉴스 없이 정상 동작)
# ============================================================
def load_news_cache():
    if not os.path.exists(NEWS_CACHE):
        print("\n⚠ 뉴스 캐시 없음 (선택사항) → news_update.py로 생성 가능")
        return None
    with open(NEWS_CACHE, "r", encoding="utf-8") as f:
        data = json.load(f)
    news_date = data.get("date", "?")
    sections = len(data.get("sections", []))
    total = sum(len(s.get("items", [])) for s in data.get("sections", []))
    print(f"\n뉴스 캐시 로드 완료")
    print(f"  → {sections}개 카테고리, {total}건 기사")
    print(f"  → 날짜: {news_date}")
    return data

# ============================================================
# 엑셀 저장
# ============================================================
def upath(fp):
    if not os.path.exists(fp): return fp
    b,e=os.path.splitext(fp); n=1
    while os.path.exists(f"{b}({n}){e}"): n+=1
    return f"{b}({n}){e}"

def save_excels(df):
    ts=datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d"); rd=f"_{ts}"
    df["Start Date"]=df["Start Date"].dt.date; df["End Date"]=df["End Date"].dt.date; df["FileRunDate"]=rd
    cols=["이름","직급","사번","소속","Project Name","Start Date","End Date","Time (Hours)","PM","Job Code","EL","Assign ID","FileRunDate"]
    df=df[cols]; os.makedirs(OUTPUT_DIR,exist_ok=True)
    p1=upath(os.path.join(OUTPUT_DIR,f"Data_output(Excel){rd}.xlsx"))
    df.to_excel(p1,sheet_name="Sheet1",index=False); print(f"\n✅ 전체: {p1} ({len(df):,}건)")
    d25=df[df["Start Date"]>=date(2025,1,1)].copy()
    p2=upath(os.path.join(OUTPUT_DIR,f"Data_Output(Excel)_after 2025{rd}.xlsx"))
    d25.to_excel(p2,sheet_name="Sheet1",index=False); print(f"✅ 2025+: {p2} ({len(d25):,}건)")
    return df, d25

# ============================================================
# 사용자 인증 — 기업정보포털과 동일한 방식
#   1) users.xlsx (azure_auto + manual_add 시트) 읽기
#   2) 활성=Y 사용자만 bcrypt 해시 생성 → PORTAL_USERS
#   3) HTML에 로그인 오버레이 + 인라인 bcrypt.js + 세션 쿠키 인증 주입
# ============================================================
def build_users():
    """
    users.xlsx 두 시트(manual_add, azure_auto) 병합 → bcrypt 해시 사용자 목록.
    이메일 중복 시 manual_add 우선. 사번 평문은 HTML에 포함되지 않음.
    """
    if not os.path.exists(USERS_FILE):
        print(f"\n⚠ users.xlsx 없음: {USERS_FILE}")
        print("  → update_users.py를 먼저 실행하세요. 로그인 불가 상태로 빌드됩니다.")
        return []

    try:
        import bcrypt as _bcrypt
    except ImportError:
        print("\n⚠ bcrypt 패키지가 설치되지 않았습니다 (pip install bcrypt). 로그인 불가 상태로 빌드.")
        return []

    try:
        sheets = pd.read_excel(USERS_FILE, sheet_name=None, dtype=str)
    except Exception as e:
        print(f"\n⚠ users.xlsx 읽기 실패: {e}")
        return []

    # 시트 우선순위: manual_add → azure_auto
    sheet_order = ["manual_add", "azure_auto"] + [s for s in sheets if s not in ("manual_add", "azure_auto")]
    seen = set()
    users = []
    counts = {"manual_add": 0, "azure_auto": 0, "기타": 0}

    for sheet_name in sheet_order:
        if sheet_name not in sheets:
            continue
        df = sheets[sheet_name].fillna("")
        for _, row in df.iterrows():
            if str(row.get("활성", "")).strip().upper() != "Y":
                continue
            email = str(row.get("이메일", "")).strip().lower()
            sabun = str(row.get("사번", "")).strip()
            if not email or not sabun or email in seen:
                continue
            seen.add(email)
            h = _bcrypt.hashpw(sabun.encode("utf-8"),
                               _bcrypt.gensalt(rounds=10, prefix=b"2a")).decode()
            users.append({
                "email": email,
                "hash":  h,
                "name":  str(row.get("이름", "")).strip(),
                "dept":  str(row.get("부서", "")).strip(),
            })
            key = sheet_name if sheet_name in counts else "기타"
            counts[key] += 1

    summary = " / ".join(f"{k}: {v}명" for k, v in counts.items() if v)
    print(f"  → 사용자 로드: {len(users)}명  ({summary})")
    return users


def _read_bcrypt_inline():
    """templates/vendor/bcrypt.min.js 인라인. 회사망 CDN 차단 환경 대응."""
    if not os.path.exists(BCRYPT_VENDOR):
        print(f"⚠ bcrypt.min.js 없음: {BCRYPT_VENDOR} → CDN fallback 사용")
        return ""
    with open(BCRYPT_VENDOR, "r", encoding="utf-8") as f:
        return f.read()


# ── 로그인 오버레이 (CSS + HTML + JS). PwC Orange 톤 / Noto Sans KR ──
LOGIN_INJECT_CSS = """
<style id="grv-login-style">
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@400;500;600;700&display=swap');
#grv-login-wrap{position:fixed;inset:0;z-index:99999;display:flex;align-items:center;justify-content:center;background:linear-gradient(135deg,#FFF5ED 0%,#FFFFFF 50%,#FFCDA8 100%);font-family:'Noto Sans KR',sans-serif;padding:30px 20px;overflow-y:auto}
#grv-login-wrap *{box-sizing:border-box;font-family:'Noto Sans KR',sans-serif}
.grv-login-container{max-width:1080px;width:100%}
.grv-login-hero{text-align:center;margin-bottom:36px}
.grv-login-hero h1{font-size:54px;font-weight:700;color:#1a1a1a;margin-bottom:16px;letter-spacing:-1.5px;line-height:1.15}
.grv-login-hero h1 .accent{color:#FD5108}
.grv-login-hero-sub{font-size:13px;color:#1a1a1a;max-width:760px;margin:0 auto;line-height:1.7}
.grv-login-hero-sub strong{color:#C44608;font-weight:700}
@media(max-width:640px){.grv-login-hero h1{font-size:36px}}
.grv-login-grid{display:grid;grid-template-columns:1fr 1fr;gap:24px;align-items:stretch}
@media(max-width:840px){.grv-login-grid{grid-template-columns:1fr}}
.grv-login-info{background:#fff;border-radius:12px;padding:24px 28px;box-shadow:0 12px 40px rgba(0,0,0,.08);border-top:4px solid #FD5108;display:flex;flex-direction:column}
.grv-login-info-section{padding:14px 16px;background:#FFF5ED;border-radius:8px;margin-bottom:10px;border-left:3px solid #FD5108}
.grv-login-info-section:last-child{margin-bottom:0}
.grv-login-info-section h3{font-size:13px;font-weight:700;color:#1a1a1a;margin:0 0 6px;display:flex;align-items:center;gap:6px}
.grv-login-info-section p{font-size:11px;color:#1a1a1a;line-height:1.65;margin:0}
.grv-login-info-section strong{color:#C44608;font-weight:700}
.grv-login-features{display:grid;grid-template-columns:1fr 1fr;gap:6px 14px;margin-top:8px}
.grv-login-feature{display:flex;align-items:center;gap:6px;font-size:11px;color:#1a1a1a;font-weight:500}
.grv-login-box{background:#fff;border-radius:12px;padding:32px 30px;box-shadow:0 12px 40px rgba(0,0,0,.12);border-top:4px solid #FD5108;display:flex;flex-direction:column}
.grv-login-logo{text-align:center;margin-bottom:24px}
.grv-login-logo h2{font-size:22px;font-weight:700;color:#1a1a1a;margin:0 0 4px;letter-spacing:-.3px}
.grv-login-logo p{font-size:12px;color:#A1A8B3;margin:0}
.grv-form-group{margin-bottom:14px}
.grv-form-group label{display:block;font-size:12px;font-weight:600;color:#1a1a1a;margin-bottom:6px}
.grv-form-group input{width:100%;padding:10px 12px;border:1px solid #DFE3E6;border-radius:8px;font-size:14px;outline:none;transition:border .15s,box-shadow .15s;background:#fff;color:#1a1a1a;font-family:'Noto Sans KR',sans-serif}
.grv-form-group input:focus{border-color:#FD5108;box-shadow:0 0 0 3px rgba(253,81,8,.15)}
.grv-pw-wrap{position:relative}
.grv-pw-wrap input{padding-right:40px}
.grv-pw-toggle{position:absolute;right:8px;top:50%;transform:translateY(-50%);background:transparent;border:none;cursor:pointer;font-size:16px;padding:4px 8px;color:#A1A8B3;opacity:.7}
.grv-pw-toggle:hover{opacity:1}
.grv-pw-toggle.on{opacity:1;color:#FD5108}
#grv-login-btn{width:100%;padding:12px;background:#FD5108;color:#fff;border:none;border-radius:8px;font-size:15px;font-weight:600;cursor:pointer;margin-top:8px;transition:background .15s,transform .1s;font-family:'Noto Sans KR',sans-serif}
#grv-login-btn:hover{background:#C44608}
#grv-login-btn:active{transform:scale(.98)}
#grv-login-btn:disabled{background:#FFAA72;cursor:not-allowed}
#grv-login-err{color:#C44608;font-size:13px;text-align:center;margin-top:12px;display:none;line-height:1.5}
.grv-login-tips{margin-top:14px;padding:12px 14px;background:#FFF5ED;border:1px solid #FFCDA8;border-radius:8px;font-size:11px;line-height:1.6;color:#1a1a1a}
.grv-login-tips h4{font-size:12px;color:#C44608;font-weight:700;margin:0 0 5px;display:flex;align-items:center;gap:5px}
.grv-login-tips ul{margin:6px 0 0;padding-left:16px}
.grv-login-tips li{margin-bottom:3px}
.grv-login-tips strong{color:#C44608;font-weight:700}
.grv-login-footer{margin-top:auto;padding-top:14px;border-top:1px solid #DFE3E6;font-size:10px;color:#A1A8B3;text-align:center;line-height:1.5}
/* 로그인 전 메인 앱 숨김 — 로그인 성공 시 클래스 제거 */
body.grv-locked #appWrap,
body.grv-locked .custom-tip,
body.grv-locked .loading-overlay{display:none !important}
body.grv-locked{overflow:hidden}
</style>
"""

LOGIN_INJECT_HTML = """
<div id="grv-login-wrap" role="dialog" aria-modal="true" aria-labelledby="grv-login-title">
  <div class="grv-login-container">
    <div class="grv-login-hero">
      <h1 id="grv-login-title">Global <span class="accent">Retain</span> Viewer</h1>
      <p class="grv-login-hero-sub">
        <strong>Global IPO · CMAAS · IOA · Assurance NGH</strong> 인원의 어싸인 현황을 한 화면에서 조회할 수 있는 사내 전용 뷰어입니다.
        TalentLink 데이터를 매일 1회 가공하여 제공합니다.
      </p>
    </div>
    <div class="grv-login-grid">
      <div class="grv-login-info">
        <div class="grv-login-info-section">
          <h3>📋 제공 기능</h3>
          <div class="grv-login-features">
            <div class="grv-login-feature">📅 어싸인 현황(간트)</div>
            <div class="grv-login-feature">📄 개인별 프로젝트 내역</div>
            <div class="grv-login-feature"><svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="margin-right:4px;flex-shrink:0"><circle cx="11" cy="11" r="7"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>프로젝트별 검색</div>
            <div class="grv-login-feature">🟢 Available 조회</div>
            <div class="grv-login-feature">📰 업계 기사모음</div>
            <div class="grv-login-feature">🏢 DART 공시정보</div>
          </div>
        </div>
        <div class="grv-login-info-section" style="border-left-color:#A1A8B3;background:#F5F7F8">
          <h3>🔐 보안 안내</h3>
          <p>회사 PWC 이메일 + 사번 6자리로 로그인합니다. 사번은 <strong>bcrypt 해시</strong>로 변환되어 저장되며 평문이 외부에 노출되지 않습니다.
          어싸인 자료는 사내 인원에 한해 공유 가능한 자료이므로 외부 유출에 유의해주세요.</p>
        </div>
      </div>
      <div class="grv-login-box">
        <div class="grv-login-logo">
          <h2>로그인</h2>
          <p>회사 계정으로 접속하세요</p>
        </div>
        <div class="grv-form-group">
          <label for="grv-login-email">회사 계정 이메일 (@pwc.com)</label>
          <input type="email" id="grv-login-email" placeholder="hong.gd@pwc.com" autocomplete="email">
        </div>
        <div class="grv-form-group">
          <label for="grv-login-pw">사번 (6자리)</label>
          <div class="grv-pw-wrap">
            <input type="password" id="grv-login-pw" placeholder="••••••" maxlength="10" autocomplete="current-password">
            <button type="button" class="grv-pw-toggle" id="grv-pw-toggle" tabindex="-1" aria-label="사번 보기/숨기기">👁</button>
          </div>
        </div>
        <button id="grv-login-btn">로그인</button>
        <p id="grv-login-err">이메일 또는 사번이 올바르지 않습니다.</p>
        <div class="grv-login-tips">
          <h4>💡 사용 안내</h4>
          <ul>
            <li>회사 PWC 이메일(@pwc.com) + 사번 6자리 입력</li>
            <li>👁 아이콘 클릭 시 사번 표시/숨김 전환</li>
            <li>등록되지 않은 사용자는 관리자에게 문의해주세요.</li>
          </ul>
          <p style="margin-top:8px"><strong>🔐 자동 로그아웃</strong>: 브라우저 창을 종료하면 자동으로 로그아웃됩니다. 탭만 닫고 다시 접속하실 경우 로그인 상태가 유지됩니다.</p>
        </div>
        <p class="grv-login-footer">© 2026 JS KIM · 오류 및 개선 의견: jeesoo.j.kim@pwc.com</p>
      </div>
    </div>
  </div>
</div>
"""

LOGIN_INJECT_JS_TEMPLATE = """
<!-- ── bcryptjs 인라인 (CDN 차단 환경 대응) ── -->
<script id="grv-bcrypt-inline">
try {
  /*__GRV_BCRYPT_JS__*/
  if (typeof bcrypt === 'undefined' && typeof dcodeIO !== 'undefined' && dcodeIO.bcrypt) {
    window.bcrypt = dcodeIO.bcrypt;
  }
} catch(e) { window.__grvBcryptErr = e.message; }
</script>
<script id="grv-auth">
(function(){
  var PORTAL_USERS = __GRV_USERS_JSON__;
  var BUILD_TIME   = "__GRV_BUILD_TIME__";
  var AUTH_COOKIE  = 'retain_user';

  // 로그인 전 본문 숨김
  document.body.classList.add('grv-locked');

  function setAuthCookie(info){
    document.cookie = AUTH_COOKIE + '=' + encodeURIComponent(JSON.stringify(info)) + '; path=/; SameSite=Lax';
  }
  function getAuthCookie(){
    var m = document.cookie.match(new RegExp('(?:^|; )' + AUTH_COOKIE + '=([^;]*)'));
    if (!m) return null;
    try { return JSON.parse(decodeURIComponent(m[1])); } catch(e){ return null; }
  }
  function clearAuthCookie(){
    document.cookie = AUTH_COOKIE + '=; path=/; max-age=0; SameSite=Lax';
  }
  window.grvLogout = function(){
    clearAuthCookie();
    location.reload();
  };

  function showApp(user){
    document.body.classList.remove('grv-locked');
    var w = document.getElementById('grv-login-wrap');
    if (w) w.style.display = 'none';
    // 헤더에 사용자 표시 + 로그아웃 버튼
    try { mountUserBadge(user); } catch(e){}
  }

  function mountUserBadge(user){
    if (document.getElementById('grv-user-badge')) return;
    var holder = document.querySelector('.header .header-right') || document.querySelector('.header');
    if (!holder) return;
    var dept = user.dept ? ' · ' + user.dept : '';
    var badge = document.createElement('div');
    badge.id = 'grv-user-badge';
    badge.style.cssText = 'display:inline-flex;align-items:center;gap:8px;padding:6px 12px;border:1px solid #e2e8f0;border-radius:8px;background:#fff;font-size:12px;color:#475569;white-space:nowrap;font-family:\\'Noto Sans KR\\',sans-serif';
    badge.innerHTML = '<span style="color:#1a1a1a;font-weight:600">' + (user.name||'') + '</span><span style="color:#94a3b8">' + dept + '</span>'
      + '<button type="button" id="grv-logout-btn" style="margin-left:4px;padding:4px 10px;border:1px solid #FFCDA8;border-radius:6px;background:#FFF5ED;color:#C44608;font-size:11px;font-weight:600;cursor:pointer;font-family:\\'Noto Sans KR\\',sans-serif">로그아웃</button>';
    holder.appendChild(badge);
    document.getElementById('grv-logout-btn').addEventListener('click', window.grvLogout);
  }

  // 자동 로그인 (세션 쿠키 유효 + 임베딩 사용자 목록에 존재)
  var cached = getAuthCookie();
  if (cached && cached.email) {
    var stillValid = PORTAL_USERS.some(function(u){ return u.email === cached.email; });
    if (stillValid) { showApp(cached); }
    else { clearAuthCookie(); }
  }

  function $(id){ return document.getElementById(id); }

  $('grv-login-btn').addEventListener('click', doLogin);
  $('grv-login-pw').addEventListener('keydown', function(e){ if(e.key==='Enter') doLogin(); });
  $('grv-login-email').addEventListener('keydown', function(e){ if(e.key==='Enter') $('grv-login-pw').focus(); });
  $('grv-pw-toggle').addEventListener('click', function(){
    var inp = $('grv-login-pw'); var btn = $('grv-pw-toggle');
    if (inp.type === 'password') { inp.type='text'; btn.textContent='🙈'; btn.classList.add('on'); }
    else { inp.type='password'; btn.textContent='👁'; btn.classList.remove('on'); }
  });

  function doLogin(){
    var email = ($('grv-login-email').value||'').trim().toLowerCase();
    var pw    = ($('grv-login-pw').value||'').trim();
    var btn   = $('grv-login-btn');
    var err   = $('grv-login-err');
    err.style.display='none';
    btn.disabled=true; btn.textContent='확인 중...';

    var diag = [];
    diag.push('빌드: ' + BUILD_TIME);
    diag.push('typeof bcrypt: ' + (typeof bcrypt));
    if (window.__grvBcryptErr) diag.push('bcrypt 로드 에러: ' + String(window.__grvBcryptErr).substring(0,200));

    var user = PORTAL_USERS.find(function(u){ return u.email === email; });
    var ok = false, failReason = '';

    if (!user) {
      failReason = '일치하는 이메일을 찾지 못했습니다';
      diag.push('이메일 매칭: 실패 (입력: ' + email + ')');
      finish();
      return;
    }
    diag.push('이메일 매칭: 성공');
    diag.push('해시 길이: ' + (user.hash ? user.hash.length : 'NULL'));
    diag.push('사번 길이: ' + pw.length);

    if (typeof bcrypt === 'undefined') {
      failReason = 'bcrypt 라이브러리 미로드 (회사망 차단 가능성)';
      finish();
      return;
    }

    try {
      ok = bcrypt.compareSync(pw, user.hash);
      diag.push('compareSync 결과: ' + ok);
    } catch(e){
      diag.push('compareSync 에러: ' + e.message);
    }

    if (!ok) {
      // async fallback
      bcrypt.compare(pw, user.hash, function(e2, res){
        if (res) { ok = true; }
        else if (e2) diag.push('compare(async) 에러: ' + e2.message);
        else diag.push('compare(async) 결과: false');
        if (!ok && !failReason) failReason = '사번이 일치하지 않습니다';
        finish();
      });
      return;
    }
    finish();

    function finish(){
      if (ok) {
        var info = {email:user.email, name:user.name, dept:user.dept};
        setAuthCookie(info);
        showApp(info);
      } else {
        err.innerHTML = '<div>이메일 또는 사번이 올바르지 않습니다. (' + (failReason||'사번이 일치하지 않습니다') + ')</div>'
          + '<details style="margin-top:8px;font-size:11px;color:#A1A8B3;text-align:left">'
          + '<summary style="cursor:pointer">진단 정보 보기</summary>'
          + '<pre style="margin-top:6px;background:#F5F7F8;padding:8px;border-radius:4px;white-space:pre-wrap;word-break:break-all">'
          + diag.join('\\n') + '</pre></details>';
        err.style.display = 'block';
        btn.disabled = false; btn.textContent = '로그인';
        console.warn('[로그인 실패]', diag.join(' | '));
      }
    }
  }
})();
</script>
"""


def inject_login(html, users, build_time):
    """로그인 오버레이 + bcrypt 인라인 + 인증 스크립트를 HTML에 주입."""
    bcrypt_js = _read_bcrypt_inline()
    users_json = json.dumps(users, ensure_ascii=False, separators=(",", ":"))
    js = (LOGIN_INJECT_JS_TEMPLATE
          .replace("/*__GRV_BCRYPT_JS__*/", bcrypt_js)
          .replace("__GRV_USERS_JSON__", users_json)
          .replace("__GRV_BUILD_TIME__", build_time))
    # CSS는 </head> 직전, HTML/JS는 <body> 직후에 삽입
    if "</head>" in html:
        html = html.replace("</head>", LOGIN_INJECT_CSS + "</head>", 1)
    else:
        html = LOGIN_INJECT_CSS + html
    if "<body>" in html:
        html = html.replace("<body>", "<body>\n" + LOGIN_INJECT_HTML + js, 1)
    else:
        html = LOGIN_INJECT_HTML + js + html
    return html


# ============================================================
# DART 표시용 CSS + JS
# ============================================================
DART_INJECT = """
<style>
.proj-summary.dart-layout{flex-wrap:nowrap;align-items:stretch;gap:16px}
.dart-card{border:1px solid #e2e8f0;border-radius:8px;padding:12px 14px;background:#fff;flex:1;min-width:220px;max-width:340px;align-self:stretch}
.dart-card .dt{font-size:11px;color:#1e293b;font-weight:700;letter-spacing:0.3px;margin-bottom:6px;display:flex;align-items:center;gap:6px}
.dart-link{margin-left:auto;font-size:9px;color:#fff;background:#2563eb;text-decoration:none;display:inline-flex;align-items:center;gap:3px;padding:3px 10px;border-radius:4px;font-weight:600;letter-spacing:0.2px;white-space:nowrap;position:relative}
.dart-link:hover{background:#1d4ed8}
.dart-link .dart-tip{display:none;position:absolute;top:calc(100% + 8px);right:0;background:#1e293b;color:#fff;padding:10px 14px;border-radius:8px;font-size:11px;font-weight:400;white-space:nowrap;z-index:9999;box-shadow:0 4px 12px rgba(0,0,0,0.15);line-height:1.6;letter-spacing:0}
.dart-link .dart-tip::before{content:'';position:absolute;top:-6px;right:16px;border-left:6px solid transparent;border-right:6px solid transparent;border-bottom:6px solid #1e293b}
.dart-link:hover .dart-tip{display:block}
.dart-tip-img{margin:8px 0 2px;display:block}
.dbg{display:inline-block;padding:1px 6px;border-radius:3px;font-size:9px;font-weight:600}
.dbg-Y{background:#dbeafe;color:#2563eb}.dbg-K{background:#fce7f3;color:#be185d}.dbg-N{background:#fef3c7;color:#b45309}.dbg-E{background:#f1f5f9;color:#64748b}
.drow{font-size:10px;color:#475569;line-height:1.7}
.drow a{color:#3b82f6;text-decoration:none}.drow a:hover{text-decoration:underline}
.dfin{display:flex;flex-wrap:nowrap;gap:3px;margin:4px 0}
.dfi{background:#f8fafc;border:1px solid #e2e8f0;border-radius:4px;padding:2px 5px;font-size:8px;white-space:nowrap;flex:1;min-width:0;text-align:center}
.dfi .k{color:#94a3b8}.dfi .v{font-family:'JetBrains Mono',monospace;font-weight:600}
.ddisc{margin-top:4px;font-size:10px;line-height:1.6}
.ddisc .dd{color:#94a3b8;font-size:9px;font-family:'JetBrains Mono',monospace;margin-right:4px}
.ddisc a{color:#334155;text-decoration:none}.ddisc a:hover{color:#3b82f6;text-decoration:underline}
.daudit{margin-top:4px;font-size:10px;line-height:1.7;border-top:1px dashed #e2e8f0;padding-top:4px}
.daudit .da-title{font-size:9px;color:#94a3b8;margin-bottom:2px}
.daudit .da-row{color:#475569;padding-left:2px}
.daudit .da-opinion{font-weight:600;color:#1e293b}
.daudit .da-auditor{color:#94a3b8}
.dart-footer{font-size:8px;color:#cbd5e1;margin-top:6px;border-top:1px solid #f1f5f9;padding-top:4px}
.proj-invest-box{flex:2;border:1px solid #e2e8f0;border-radius:8px;background:#fff;padding:12px 14px;min-width:0}
.pib-title{font-size:11px;color:#1e293b;font-weight:700;letter-spacing:0.3px;margin-bottom:10px}
.pib-cards{display:flex;gap:10px;flex-wrap:wrap}
.pib-cards .proj-summary-card{border:none;background:#f8fafc;margin:0;padding:10px 14px}
</style>
<script>
var DART_DATA=%%DART_JSON%%;
(function(){
if(!DART_DATA||!DART_DATA.projMap)return;
var dartUpdated=DART_DATA.updated||'';
function fmtDt(s){return s&&s.length===8?s.slice(0,4)+'.'+s.slice(4,6)+'.'+s.slice(6,8):s||'';}
function addComma(s){return String(s).replace(/\\B(?=(\\d{3})+(?!\\d))/g,',');}
function fmtAmt(s){if(!s)return'-';var n=parseInt(String(s).replace(/,/g,''));if(isNaN(n))return s;if(Math.abs(n)>=1e12)return addComma((n/1e12).toFixed(1))+'조';if(Math.abs(n)>=1e8)return addComma(Math.round(n/1e8))+'억';if(Math.abs(n)>=1e4)return addComma(Math.round(n/1e4))+'만';return addComma(n);}
function mkCard(pn){
  var skip=['기타Admin','New Staff','Refresh Off','Admin','교육'];
  for(var i=0;i<skip.length;i++){if(pn.indexOf(skip[i])>=0)return'';}
  var cn=DART_DATA.projMap[pn];
  if(!cn)return'<div class="dart-card"><div class="dt" style="color:#cbd5e1">DART 공시정보</div><div style="font-size:10px;color:#94a3b8;padding:4px 0">해당 고객사의 DART 등록 정보를 찾지 못했습니다.</div><div class="dart-footer">dart_update.py로 갱신 시 반영될 수 있습니다.</div></div>';
  var d=DART_DATA.companies[cn];
  if(!d)return'<div class="dart-card"><div class="dt" style="color:#cbd5e1">DART 공시정보</div><div style="font-size:10px;color:#94a3b8;padding:4px 0">'+cn+' — DART 상세정보 없음</div></div>';
  var mkt=(d.info&&d.info.market)||'비상장';
  var cls=mkt==='유가증권'?'Y':mkt==='코스닥'?'K':mkt==='코넥스'?'N':'E';
  var searchName=(d.info&&d.info.dart_name)?d.info.dart_name:cn;
  var h='<div class="dart-card"><div class="dt">DART 공시정보 <span class="dbg dbg-'+cls+'">'+mkt+'</span>';
  if(d.stock_code)h+='<span style="font-size:9px;color:#b0b8c4">'+d.stock_code+'</span>';
  h+='<a class="dart-link" href="https://dart.fss.or.kr/dsab001/main.do?option=corp&textCrpNm='+encodeURIComponent(searchName)+'&startDt=20240101&endDt='+new Date().toISOString().slice(0,10).replace(/-/g,'')+'&publicType=&sort=date&series=desc" target="_blank">DART 공시자료 링크 ↗<span class="dart-tip">클릭 시 DART 사이트로 이동됩니다.<br>이동 후 DART 페이지 상의 아래 버튼을 클릭하세요.<br><img class="dart-tip-img" src="data:image/png;base64,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" width="84" height="23" alt="검색"></span></a>';
  h+='</div>';
  if(d.info){var i=d.info;var parts=[];
    if(i.ceo)parts.push('대표: '+i.ceo);
    if(i.est_dt)parts.push('설립: '+fmtDt(i.est_dt));
    if(parts.length)h+='<div class="drow">'+parts.join(' · ')+'</div>';
    if(i.induty_nm)h+='<div class="drow">업종: '+i.induty_nm+'</div>';
    if(i.acc_mt)h+='<div class="drow">결산월: '+i.acc_mt+'월</div>';
    if(i.adres)h+='<div class="drow" style="font-size:9px;color:#94a3b8">'+i.adres+'</div>';
    if(i.hm_url)h+='<div class="drow"><a href="'+(i.hm_url.indexOf("http")===0?i.hm_url:"https://"+i.hm_url)+'" target="_blank">'+i.hm_url+'</a></div>';
  }
  if(d.fin){var f=d.fin;
    h+='<div style="font-size:9px;color:#94a3b8;margin-top:4px">'+f.year+'년 기말 '+f.fs_div+' 재무제표 (최신 공시내역)</div><div class="dfin">';
    [["자산총계","#3b82f6"],["매출액","#10b981"],["영업손익","#f59e0b"],["당기순손익","#8b5cf6"]].forEach(function(x){
      var v=f[x[0]]||(x[0]==='영업손익'?f['영업이익']:'')|| (x[0]==='당기순손익'?f['당기순이익']:'');
      if(v)h+='<div class="dfi"><span class="k">'+x[0]+' </span><span class="v" style="color:'+x[1]+'">'+fmtAmt(v)+'</span></div>';
    });h+='</div>';
  }
  if(d.audit&&(d.audit.items&&d.audit.items.length||d.audit.fee||d.audit.hours)){
    h+='<div class="daudit"><div class="da-title">감사의견 ('+d.audit.year+'년 사업보고서)</div>';
    if(d.audit.items)d.audit.items.forEach(function(a){
      h+='<div class="da-row"><span class="da-opinion">'+a.opinion+'</span>';
      if(a.auditor)h+=' <span class="da-auditor">('+a.auditor+')</span>';
      h+='</div>';
    });
    if(d.audit.fee||d.audit.hours){var fp=[];
      if(d.audit.fee)fp.push('감사보수: '+addComma(d.audit.fee)+'백만원');
      if(d.audit.hours)fp.push('감사시간: '+addComma(d.audit.hours)+'시간');
      h+='<div class="da-row" style="margin-top:2px;font-size:9px;color:#64748b">'+fp.join('  |  ')+'</div>';
    }
    h+='</div>';
  }
  if(d.disc&&d.disc.length){h+='<div class="ddisc">';
    d.disc.slice(0,3).forEach(function(x){h+='<div><span class="dd">'+fmtDt(x.date)+'</span><a href="'+x.url+'" target="_blank">'+x.title+'</a></div>';});
    h+='</div>';
  }
  if(!d.info&&!d.fin&&!d.audit&&!d.disc){h+='<div style="font-size:10px;color:#94a3b8;padding:4px 0">'+cn+' — DART 매칭 완료 (공시 상세정보 없음)</div>';}
  if(d.info&&!d.fin&&!d.audit){h+='<div style="font-size:9px;color:#b0b8c4;margin-top:5px;padding-top:5px;border-top:1px dashed #e2e8f0;line-height:1.5">※ 재무정보·감사의견·감사보수는 상장법인 및 사업보고서 제출대상 법인에 한해 조회됩니다.</div>';}
  h+='<div class="dart-footer">공시정보 갱신: '+dartUpdated+'</div>';
  h+='</div>';return h;
}
function inject(el){
  if(el.querySelector('.dart-card'))return;
  var sec=el.closest('.detail-section');if(!sec)return;
  var hdr=sec.querySelector('.detail-year-header');if(!hdr)return;
  var ti=hdr.querySelector('[title]');if(!ti)return;
  var pn=ti.getAttribute('title');if(!pn)return;
  var card=mkCard(pn);if(!card)return;
  var cards=Array.from(el.querySelectorAll(':scope > .proj-summary-card'));
  if(cards.length&&!el.querySelector('.proj-invest-box')){
    var box=document.createElement('div');box.className='proj-invest-box';
    var tt=document.createElement('div');tt.className='pib-title';tt.textContent='프로젝트 투입내역';box.appendChild(tt);
    var inner=document.createElement('div');inner.className='pib-cards';
    cards.forEach(function(c){inner.appendChild(c);});
    box.appendChild(inner);el.appendChild(box);
  }
  el.classList.add('dart-layout');
  el.insertAdjacentHTML('afterbegin',card);
}
function processNode(n){
  if(!n||!n.querySelectorAll)return;
  if(n.classList&&n.classList.contains('proj-summary'))inject(n);
  n.querySelectorAll('.proj-summary').forEach(inject);
}
new MutationObserver(function(ms){ms.forEach(function(m){m.addedNodes.forEach(processNode);});}).observe(document.body,{childList:true,subtree:true});
document.querySelectorAll('.proj-summary').forEach(inject);
document.addEventListener('click',function(e){
  var hdr=e.target.closest('.detail-year-header');
  if(!hdr)return;
  setTimeout(function(){
    var sec=hdr.closest('.detail-section');
    if(sec)sec.querySelectorAll('.proj-summary').forEach(inject);
  },400);
});
})();
</script>
"""

# ============================================================
# HTML 빌드
# ============================================================
def build_html(df25, du="", dart=None, news=None, users=None):
    print("\nHTML 뷰어 생성...")
    recs=[]
    for _,r in df25.iterrows():
        recs.append({"name":str(r["이름"]) if pd.notna(r["이름"]) else "","rank":str(r["직급"]) if pd.notna(r["직급"]) else "",
            "dept":str(r["소속"]) if pd.notna(r["소속"]) else "","project":str(r["Project Name"]) if pd.notna(r["Project Name"]) else "",
            "startDate":str(r["Start Date"]) if pd.notna(r["Start Date"]) else "","endDate":str(r["End Date"]) if pd.notna(r["End Date"]) else "",
            "hours":float(r["Time (Hours)"]) if pd.notna(r["Time (Hours)"]) else 0,"pm":str(r["PM"]) if pd.notna(r["PM"]) else "",
            "el":str(r["EL"]) if pd.notna(r["EL"]) else "","jobCode":str(int(r["사번"])) if pd.notna(r["사번"]) else "",
            "projCode":str(r["Job Code"]) if pd.notna(r["Job Code"]) else "",
            "assignId":str(r["Assign ID"]) if "Assign ID" in r.index and pd.notna(r["Assign ID"]) else ""})
    frd=datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d")
    if not os.path.exists(HTML_TEMPLATE): print(f"  ⚠ 템플릿 없음"); return None,None
    with open(HTML_TEMPLATE,"r",encoding="utf-8") as f: html=f.read()
    js=json.dumps(recs,ensure_ascii=False)
    html=re.sub(r'var EMBEDDED_DATA\s*=\s*null\s*;',f'var EMBEDDED_DATA = {js};',html)
    html=re.sub(r'var EMBEDDED_DATA\s*=\s*\[[\s\S]*?\];',f'var EMBEDDED_DATA = {js};',html)
    html=re.sub(r'var EMBEDDED_FILE_RUN_DATE\s*=\s*null\s*;',f'var EMBEDDED_FILE_RUN_DATE = {json.dumps(frd)};',html)
    html=re.sub(r'var EMBEDDED_FILE_RUN_DATE\s*=\s*"[^"]*"\s*;',f'var EMBEDDED_FILE_RUN_DATE = {json.dumps(frd)};',html,count=1)
    html=re.sub(r'id="loading" style="display:\s*flex\s*"','id="loading" style="display:none"',html)
    # DART 주입
    if dart and dart.get("companies"):
        dj=DART_INJECT.replace("%%DART_JSON%%",json.dumps(dart,ensure_ascii=False))
    else:
        dj='<script>var DART_DATA=null;</script>'
    # 뉴스 주입
    if news and news.get("sections"):
        nj=f'\n<script>var NEWS_DATA={json.dumps(news,ensure_ascii=False)};</script>'
    else:
        nj='\n<script>var NEWS_DATA=null;</script>'
    html=html.replace('</body>',dj+nj+'\n</body>')
    # 로그인 오버레이 주입 (사용자 목록이 있을 때만)
    build_time=datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d %H:%M")
    if users:
        html=inject_login(html, users, build_time)
        print(f"  → 로그인 오버레이 주입: {len(users)}명 등록")
    else:
        print(f"  ⚠ 사용자 목록이 비어있어 로그인 오버레이를 주입하지 않았습니다.")
    # Viewer (로컬 전용)
    vp=None
    if not IS_CI:
        vp=upath(os.path.join(OUTPUT_DIR,"Project_Allocation_Viewer.html"))
        with open(vp,"w",encoding="utf-8") as f: f.write(html)
        print(f"✅ Viewer: {vp} ({os.path.getsize(vp)/1048576:.1f}MB)")
    # index.html (IS_SHARED + 타임스탬프 + TalentLink 정보를 한번에)
    ix=re.sub(r'var IS_SHARED\s*=\s*false\s*;','var IS_SHARED = true;',html)
    ts=datetime.now(timezone(timedelta(hours=9))).strftime("%Y.%m.%d %H:%M:%S")
    ix=re.sub(r'(<div id="saveTimestamp"[^>]*>)([^<]*)(</div>)',rf'\g<1>업데이트: {ts}\3',ix)
    tl_div=f'<div style="position:fixed;bottom:8px;right:12px;font-size:9px;color:#64748b;letter-spacing:-0.3px;z-index:1">TalentLink: {du}</div>'
    ix=ix.replace(dj+nj+'\n</body>',dj+nj+'\n'+tl_div+'\n</body>')
    if IS_CI:
        ip=INDEX_OUTPUT  # 직접 덮어쓰기
    else:
        ip=upath(INDEX_OUTPUT)
    with open(ip,"w",encoding="utf-8") as f: f.write(ix)
    print(f"✅ index.html: {ip} ({os.path.getsize(ip)/1048576:.1f}MB, {len(recs):,}건)")
    return ip,vp

# ============================================================
# GitHub
# ============================================================
def push_gh(ip):
    if not GITHUB_TOKEN:
        print(f"\n⚠ GitHub 토큰 미설정 (.env 확인) → {ip}"); return False
    print("\nGitHub 업로드...")
    api=f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE}"
    h={"Authorization":f"token {GITHUB_TOKEN}","Accept":"application/vnd.github.v3+json"}
    r=req_lib.get(api,headers=h); sha=r.json().get("sha") if r.status_code==200 else None
    with open(ip,"rb") as f: b64=base64.b64encode(f.read()).decode()
    p={"message":f"Update ({datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d')})","content":b64}
    if sha: p["sha"]=sha
    r=req_lib.put(api,headers=h,json=p)
    if r.status_code in(200,201): print("✅ GitHub 완료"); return True
    print(f"❌ GitHub 실패: {r.status_code}"); return False

# ============================================================
# 실행
# ============================================================
if __name__=="__main__":
    start=datetime.now()
    print("="*60)
    print("  Azure Retain 자동화")
    print(f"  {start.strftime('%Y-%m-%d %H:%M:%S')}")
    if IS_CI: print("  (GitHub Actions)")
    print("="*60)
    try:
        # ── 사용자 목록 갱신 (Azure SQL → 2.사용자관리/users.xlsx)
        # 환경변수 SKIP_USER_SYNC=1 또는 GitHub Actions 환경에서는 동기화 스킵
        if os.getenv("SKIP_USER_SYNC") != "1":
            try:
                from update_users import run as _sync_users
                print("\n사용자 DB 동기화 중...")
                _n_active = _sync_users()
                if _n_active < 0:
                    print("  ⚠ 동기화 실패 — 기존 users.xlsx로 빌드 진행")
            except ImportError as _e:
                print(f"\n⚠ update_users 모듈 import 실패: {_e}")
            except Exception as _e:
                print(f"\n⚠ 사용자 동기화 예외 (기존 users.xlsx로 진행): {_e}")
        else:
            print("\n사용자 동기화 스킵 (SKIP_USER_SYNC=1)")

        users=build_users()

        raw,du=fetch_data()
        print("\n가공 중..."); result=process_data(raw)
        if IS_CI:
            # GitHub Actions: 엑셀 저장 불필요, 2025+ 데이터만 사용
            result["Start Date"]=pd.to_datetime(result["Start Date"])
            result["End Date"]=pd.to_datetime(result["End Date"])
            df25=result[result["Start Date"]>=pd.Timestamp("2025-01-01")].copy()
            df25["Start Date"]=df25["Start Date"].dt.date
            df25["End Date"]=df25["End Date"].dt.date
            print(f"  → 2025+: {len(df25):,}건")
        else:
            # 로컬: 엑셀 저장
            df_all,df25=save_excels(result)
        dart=load_dart_cache()
        news=load_news_cache()
        ip,vp=build_html(df25,du,dart,news,users)
        if ip and not IS_CI:
            push_gh(ip)
        # GitHub Actions에서는 워크플로우가 자동 커밋하므로 push_gh 불필요
        print(f"\n⏱ {(datetime.now()-start).total_seconds():.1f}초")
    except Exception as e:
        print(f"\n❌ {e}"); import traceback; traceback.print_exc()
        if not IS_CI: input("\nEnter...")
        sys.exit(1)
    print("\n완료!")
    if not IS_CI: input("Enter...")
