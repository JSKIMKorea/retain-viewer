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
import os, sys, json, re, base64, urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

GH_LOG_TOKEN  = os.getenv("GH_LOG_TOKEN", "")
GH_LOG_REPO   = os.getenv("GH_LOG_REPO", "JSKIMKorea/retain-login-log")

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
-- 'Global': 2026-07 조직개편으로 IPO/CMAAS/IOA의 Manager 미만(SA/A 등)이 통합된 신설 본부
WHERE e.ORG_NM IN ('Global CMAAS','IOA','Global IPO','Assurance NGH','Global')"""

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
    # 직급명 변형 정규화: 'Senior-Associate 1/2' 등 뒤에 붙는 숫자 제거 → 기존 직급 그룹에 합류
    df["직급"]=df["직급"].astype(str).str.replace(r"\s+\d+$","",regex=True)
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
#grv-login-wrap{position:fixed;inset:0;z-index:99999;display:flex;align-items:flex-start;justify-content:center;background:linear-gradient(135deg,#FFF5ED 0%,#FFFFFF 50%,#FFCDA8 100%);font-family:'Noto Sans KR',sans-serif;padding:30px 20px;overflow-y:auto;-webkit-overflow-scrolling:touch;overscroll-behavior:contain}
#grv-login-wrap *{box-sizing:border-box;font-family:'Noto Sans KR',sans-serif}
.grv-login-container{max-width:1080px;width:100%;margin:auto}
.grv-login-hero{text-align:center;margin-bottom:36px}
.grv-login-hero h1{font-size:54px;font-weight:700;color:#1a1a1a;margin-bottom:16px;letter-spacing:-1.5px;line-height:1.15}
.grv-login-hero h1 .accent{color:#FD5108}
.grv-login-hero-sub{font-size:13px;color:#1a1a1a;margin:0 auto;line-height:1.6;white-space:nowrap}
.grv-login-hero-sub strong{color:#C44608;font-weight:700}
@media(max-width:640px){.grv-login-hero h1{font-size:36px}.grv-login-hero-sub{white-space:normal;max-width:90%}}
/* 단일 컬럼: 로그인 박스만 가운데 배치 */
.grv-login-grid{display:flex;justify-content:center;align-items:stretch}
.grv-login-box{background:#fff;border-radius:12px;padding:32px 36px;box-shadow:0 12px 40px rgba(0,0,0,.12);border-top:4px solid #FD5108;display:flex;flex-direction:column;width:100%;max-width:480px}
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
body.grv-locked{overflow:hidden;position:fixed;width:100%;height:100%}

/* ── 헤더 위젯 바 (날씨·사용자·로그아웃·테마 토글) ──
   알림 박스 위에 별도 행으로 배치. width:100% + max-width:560px 로
   알림 박스와 정확히 같은 outer 폭. padding:0 16px 가 notice 의 좌·우 padding 과
   같아 위젯들의 컨텐츠 영역이 notice 텍스트 영역과 일치.
   → 첫 위젯(날씨) 좌측 = "1. 해당..." 시작선
     마지막 위젯(토글) 우측 = "Smart Office System" 끝선
   어느 모니터에서나 비율·정렬 동일하게 유지됨.
   4개 위젯 모두 height:32px / font-size:12px / Noto Sans KR 통일. */
/* ── 위젯 바 :: 공지 박스와 동일한 톤 (Noto Sans KR + soft border + rounded 8px) ──
   각 모듈은 상단 작은 라벨 + 하단 값(아이콘/온도/이름)의 2단 구조.
   외곽 1px #e2e8f0 + border-radius 8px + bg #fafbfc 로 #headerNotice 와 일치. */
#grv-widget-bar{display:flex;align-items:stretch;flex-shrink:0;box-sizing:border-box;border:1px solid #e2e8f0;border-radius:8px;background:#fafbfc;color:#1e293b;font-family:'Noto Sans KR','맑은 고딕','Malgun Gothic',sans-serif}
/* first/last 모듈 라운드 보정 — overflow:hidden 대신 (드롭다운 클리핑 방지) */
#grv-widget-bar > :first-child{border-top-left-radius:7px;border-bottom-left-radius:7px}
#grv-widget-bar > :last-child{border-top-right-radius:7px;border-bottom-right-radius:7px}
#grv-widget-bar > :last-child > .grv-profile-btn{border-top-right-radius:7px;border-bottom-right-radius:7px}
#grv-widget-bar *{box-sizing:border-box;font-family:'Noto Sans KR','맑은 고딕','Malgun Gothic',sans-serif}
/* 공통 모듈 — compact 2-line: 라벨(8px) + 값(11px) */
.grv-mod{position:relative;display:flex;flex-direction:column;justify-content:center;min-height:32px;padding:3px 11px;gap:1px;border:0;background:transparent;color:inherit;font-family:inherit;cursor:pointer;transition:background .15s ease,color .15s ease;text-align:left}
.grv-mod + .grv-mod,.grv-profile-wrap > .grv-profile-btn{border-left:1px solid #e2e8f0}
.grv-mod-label{font-size:8px!important;line-height:1;font-weight:600;letter-spacing:0.04em;color:#94a3b8;white-space:nowrap}
.grv-mod-body{display:inline-flex;align-items:center;gap:5px;font-size:11px;line-height:1.1;font-weight:600;color:#1e293b;white-space:nowrap}
.grv-mod-body svg{display:block}
/* hover: 부드러운 슬레이트 + 파란 액센트 */
.grv-mod-btn:hover,.grv-weather:hover{background:#f1f5f9}
.grv-mod-btn:hover .grv-mod-body,.grv-weather:hover .grv-mod-body,.grv-weather:hover .grv-w-icon{color:#3b82f6}
/* 활성 토글: 연한 파란 배경 */
.grv-mod-btn.active{background:#eff6ff}
.grv-mod-btn.active .grv-mod-label{color:#60a5fa}
.grv-mod-btn.active .grv-mod-body{color:#3b82f6}
/* 날씨 모듈 */
.grv-weather{cursor:default}
.grv-w-icon{display:inline-flex;align-items:center;color:#64748b;transition:color .15s}
.grv-w-temp{font-weight:700;color:inherit}
.grv-weather select{border:0;background:transparent;color:#475569;cursor:pointer;outline:none;padding:0 13px 0 2px;margin-left:2px;max-width:96px;font-family:inherit;font-size:11px;font-weight:500;appearance:none;-webkit-appearance:none;background-image:linear-gradient(45deg,transparent 50%,currentColor 50%),linear-gradient(135deg,currentColor 50%,transparent 50%);background-position:calc(100% - 5px) 56%,calc(100% - 2px) 56%;background-size:4px 4px;background-repeat:no-repeat}
.grv-weather:hover select{color:#3b82f6}
.grv-weather select option{color:#1e293b;background:#fff}
/* 모바일 뷰 토글 */
.grv-mob-pc{display:inline-flex;align-items:center}
.grv-mob-mob{display:none;align-items:center}
body.mobile-view .grv-mob-pc{display:none}
body.mobile-view .grv-mob-mob{display:inline-flex}
/* 현재 보기 모드 토글 (클릭 시 PC/모바일 전환 — 텍스트+아이콘) */
#grv-viewmode .grv-mob-pc,#grv-viewmode .grv-mob-mob{gap:5px}
.grv-vm-text{font-weight:600;letter-spacing:0.01em}
/* 모바일 모드일 때: 활성 상태 강조 (강한 파란 배경) */
body.mobile-view #grv-viewmode{background:#3b82f6!important;color:#fff!important}
body.mobile-view #grv-viewmode .grv-mod-label{color:rgba(255,255,255,0.78)!important}
body.mobile-view #grv-viewmode .grv-mod-body{color:#fff!important}
/* 프로필 모듈 */
.grv-profile-wrap{position:relative;display:flex;flex-shrink:0}
.grv-profile-btn{position:relative;display:flex;flex-direction:column;justify-content:center;min-height:32px;padding:3px 11px;gap:1px;border:0;background:transparent;color:inherit;font-family:inherit;cursor:pointer;transition:background .15s ease,color .15s ease;text-align:left}
.grv-profile-btn:hover,.grv-profile-btn.open{background:#f1f5f9}
.grv-profile-btn:hover .grv-mod-body,.grv-profile-btn.open .grv-mod-body{color:#3b82f6}
.grv-profile-name{font-weight:600;color:inherit}
.grv-profile-chevron{color:#94a3b8;display:flex;align-items:center;transition:transform .2s,color .15s;margin-left:1px}
.grv-profile-btn.open .grv-profile-chevron{transform:rotate(180deg)}
.grv-profile-btn:hover .grv-profile-chevron,.grv-profile-btn.open .grv-profile-chevron{color:#3b82f6}
/* 드롭다운: 부드러운 모서리 + 그림자 */
.grv-dropdown{position:absolute;top:calc(100% + 6px);right:0;min-width:200px;background:#fff;border:1px solid #e2e8f0;border-radius:10px;box-shadow:0 8px 24px rgba(0,0,0,.12);z-index:9998;display:none;overflow:hidden;font-family:'Noto Sans KR','맑은 고딕','Malgun Gothic',sans-serif}
.grv-dropdown.open{display:block}
.grv-dd-header{padding:11px 14px 10px;border-bottom:1px solid #f1f5f9;background:#fafbfc}
.grv-dd-label{font-size:9px!important;font-weight:600;letter-spacing:0.06em;color:#94a3b8;margin-bottom:4px;line-height:1}
.grv-dd-name{font-size:13px!important;font-weight:700;color:#1e293b;line-height:1.3}
.grv-dd-dept{font-size:11px!important;color:#94a3b8;margin-top:2px}
.grv-dd-item{display:flex;align-items:center;gap:8px;padding:10px 14px;font-size:12px!important;font-weight:500;color:#475569;cursor:pointer;transition:background .12s,color .12s;border:0;background:none;width:100%;text-align:left;font-family:inherit}
.grv-dd-item + .grv-dd-item{border-top:1px solid #f1f5f9}
.grv-dd-item:hover{background:#f8fafc;color:#1e293b}
.grv-dd-item.danger{color:#C44608}
.grv-dd-item.danger:hover{background:#FFF5ED;color:#C44608}

/* ── 다크 모드 ── */
html[data-theme="dark"]{color-scheme:dark}
html[data-theme="dark"] body{background:#0f172a;color:#e2e8f0}
html[data-theme="dark"] .header{background:#1e293b;color:#e2e8f0;border-bottom-color:#334155;box-shadow:0 1px 3px rgba(0,0,0,0.3)}
html[data-theme="dark"] .header-title,html[data-theme="dark"] .person-name{color:#e2e8f0}
html[data-theme="dark"] .header-sub,html[data-theme="dark"] .header-build,html[data-theme="dark"] .stat-name,html[data-theme="dark"] .result-count,html[data-theme="dark"] .avail-count{color:#94a3b8}
html[data-theme="dark"] #headerNotice{background:#1e293b !important;border-color:#334155 !important;color:#cbd5e1 !important}
html[data-theme="dark"] #headerNotice div{color:#cbd5e1 !important}
html[data-theme="dark"] .person-section,html[data-theme="dark"] .rank-group,html[data-theme="dark"] .upload-zone,html[data-theme="dark"] .person-header,html[data-theme="dark"] .rank-group-header,html[data-theme="dark"] .tab-bar{background:#1e293b;border-color:#334155;box-shadow:0 1px 3px rgba(0,0,0,0.2)}
html[data-theme="dark"] .person-header:hover,html[data-theme="dark"] .rank-group-header:hover{background:#273449}
html[data-theme="dark"] .person-header.open,html[data-theme="dark"] .rank-group-header.open{border-bottom-color:#334155;background:#273449}
html[data-theme="dark"] .stat-chip{background:#334155;border-color:#475569;color:#cbd5e1}
html[data-theme="dark"] .toolbar,html[data-theme="dark"] .avail-toolbar{background:#0f172a}
html[data-theme="dark"] .search-input,html[data-theme="dark"] .date-input,html[data-theme="dark"] .min-days-input,html[data-theme="dark"] .year-btn,html[data-theme="dark"] .clear-btn,html[data-theme="dark"] .action-btn{background:#1e293b;border-color:#334155;color:#e2e8f0}
html[data-theme="dark"] .search-input::placeholder{color:#64748b}
html[data-theme="dark"] .search-icon{color:#64748b}
html[data-theme="dark"] .gantt-row-even .gantt-cell{background:#f1f5f9 !important}
html[data-theme="dark"] .gantt-row-odd .gantt-cell{background:#e9eef5 !important}
html[data-theme="dark"] .gantt-cell{border-right-color:#c5cdd8 !important;border-bottom-color:#d4dae4 !important}
html[data-theme="dark"] .month-header-cell,html[data-theme="dark"] .week-header-cell{background:#1e293b;color:#94a3b8;border-color:#334155}
html[data-theme="dark"] .week-header-cell.today{background:#1e3a8a;color:#dbeafe}
html[data-theme="dark"] .gantt-cell.today-col{background:#dbeafe !important}
html[data-theme="dark"] .empty-state{color:#64748b}
html[data-theme="dark"] .project-block-name{color:#1e293b !important}
/* 위젯 바 다크 — 공지 박스와 동일 톤 (#1e293b 배경, #334155 테두리) */
html[data-theme="dark"] #grv-widget-bar{background:#1e293b;border-color:#334155;color:#e2e8f0}
html[data-theme="dark"] .grv-mod + .grv-mod,html[data-theme="dark"] .grv-profile-wrap > .grv-profile-btn{border-left-color:#334155}
html[data-theme="dark"] .grv-mod-label{color:#64748b}
html[data-theme="dark"] .grv-mod-body{color:#e2e8f0}
html[data-theme="dark"] .grv-w-icon{color:#94a3b8}
html[data-theme="dark"] .grv-mod-btn:hover,html[data-theme="dark"] .grv-weather:hover,html[data-theme="dark"] .grv-profile-btn:hover,html[data-theme="dark"] .grv-profile-btn.open{background:#273449}
html[data-theme="dark"] .grv-mod-btn:hover .grv-mod-body,html[data-theme="dark"] .grv-weather:hover .grv-mod-body,html[data-theme="dark"] .grv-weather:hover .grv-w-icon,html[data-theme="dark"] .grv-profile-btn:hover .grv-mod-body,html[data-theme="dark"] .grv-profile-btn.open .grv-mod-body{color:#93c5fd}
html[data-theme="dark"] .grv-mod-btn.active{background:#1e3a8a}
html[data-theme="dark"] .grv-mod-btn.active .grv-mod-label{color:#60a5fa}
html[data-theme="dark"] .grv-mod-btn.active .grv-mod-body{color:#dbeafe}
html[data-theme="dark"] .grv-weather select{color:#cbd5e1}
html[data-theme="dark"] .grv-weather:hover select{color:#93c5fd}
html[data-theme="dark"] .grv-weather select option{background:#1e293b;color:#e2e8f0}
html[data-theme="dark"] body.mobile-view #grv-viewmode{background:#1d4ed8!important;color:#fff!important}
html[data-theme="dark"] body.mobile-view #grv-viewmode .grv-mod-label{color:rgba(255,255,255,0.78)!important}
html[data-theme="dark"] body.mobile-view #grv-viewmode .grv-mod-body{color:#fff!important}
html[data-theme="dark"] .grv-profile-chevron{color:#64748b}
html[data-theme="dark"] .grv-profile-btn:hover .grv-profile-chevron,html[data-theme="dark"] .grv-profile-btn.open .grv-profile-chevron{color:#93c5fd}
html[data-theme="dark"] .grv-dropdown{background:#1e293b;border-color:#334155;box-shadow:0 8px 24px rgba(0,0,0,.3)}
html[data-theme="dark"] .grv-dd-header{background:#162033;border-bottom-color:#334155}
html[data-theme="dark"] .grv-dd-label{color:#64748b}
html[data-theme="dark"] .grv-dd-name{color:#e2e8f0}
html[data-theme="dark"] .grv-dd-dept{color:#64748b}
html[data-theme="dark"] .grv-dd-item{color:#94a3b8;background:none}
html[data-theme="dark"] .grv-dd-item + .grv-dd-item{border-top-color:#334155}
html[data-theme="dark"] .grv-dd-item:hover{background:#273449;color:#e2e8f0}
html[data-theme="dark"] .grv-dd-item.danger{color:#fb923c}
html[data-theme="dark"] .grv-dd-item.danger:hover{background:#3a1e0e;color:#fb923c}
html[data-theme="dark"] .frd-date{color:#93c5fd !important;background:none !important;text-decoration:underline;text-decoration-color:rgba(99,102,241,0.7);text-underline-offset:3px;text-decoration-thickness:2px;line-height:1.5}
html[data-theme="dark"] .frd-label{color:#94a3b8 !important}
html[data-theme="dark"] #ganttGuide,html[data-theme="dark"] #detailGuide,html[data-theme="dark"] #projGuide{color:#94a3b8 !important}
html[data-theme="dark"] .rank-group-header{background:#1e293b !important;color:#e2e8f0 !important}
html[data-theme="dark"] .rank-group-title{color:#e2e8f0 !important}
html[data-theme="dark"] .rank-group-count{color:#cbd5e1 !important;background:#334155 !important}
html[data-theme="dark"] #availLeftSticky,html[data-theme="dark"] #availRightSticky{background:#0f172a !important}
html[data-theme="dark"] #headerNotice{color:#94a3b8 !important}
html[data-theme="dark"] .detail-section{background:#1e293b !important;border-color:#334155 !important}
html[data-theme="dark"] .detail-year-header{background:#1e293b !important}
html[data-theme="dark"] .detail-year-header:hover{background:#273449 !important}
html[data-theme="dark"] .detail-year-header.open{background:#273449 !important;border-bottom-color:#334155 !important}
html[data-theme="dark"] .detail-year-title{color:#e2e8f0 !important}
html[data-theme="dark"] .detail-table thead th{background:#0f172a !important;color:#94a3b8 !important;border-color:#334155 !important}
html[data-theme="dark"] .detail-table tbody td{color:#cbd5e1 !important;border-color:#334155 !important}
html[data-theme="dark"] .detail-table tbody tr:hover{background:#273449 !important}
html[data-theme="dark"] #projSearchArea [style*="background:#fff"]{background:#1e293b !important;border-color:#334155 !important}
html[data-theme="dark"] #projSearchArea [style*="color:#475569"]{color:#94a3b8 !important}
html[data-theme="dark"] #projSearchArea [style*="color:#000"]{color:#94a3b8 !important}
html[data-theme="dark"] .avail-table thead th{background:#0f172a !important;color:#94a3b8 !important;border-color:#334155 !important}
html[data-theme="dark"] .avail-table tbody td{color:#cbd5e1 !important;border-color:#334155 !important}
html[data-theme="dark"] .avail-table tbody tr:hover{background:#273449 !important}
html[data-theme="dark"] #newsContent{color:#cbd5e1 !important}
html[data-theme="dark"] .news-link{color:#93c5fd !important}
html[data-theme="dark"] #newsContent [style*="color:#1e293b"]{color:#e2e8f0 !important}
html[data-theme="dark"] #newsContent [style*="color:#64748b"]{color:#94a3b8 !important}
html[data-theme="dark"] #newsContent [style*="color:#475569"]{color:#cbd5e1 !important}
html[data-theme="dark"] #newsContent div[style*="border-bottom"]{border-color:#334155 !important}
html[data-theme="dark"] #newsContent tr{border-color:#334155 !important}
/* ── 모바일 뷰: 위젯 바 축소 ── */
body.mobile-view #grv-widget-bar{flex-wrap:wrap}
body.mobile-view .grv-mod,body.mobile-view .grv-profile-btn{min-height:28px;padding:2px 9px;gap:0}
body.mobile-view .grv-mod-label{font-size:7px!important;letter-spacing:0.02em}
body.mobile-view .grv-mod-body{font-size:10px;gap:4px}
body.mobile-view .grv-weather select{max-width:74px;font-size:10px;padding-right:11px}
body.mobile-view #headerNotice{font-size:12px!important;padding:8px 12px!important}
body.mobile-view #headerNotice>div{font-size:12px!important;line-height:1.5}
</style>
"""

LOGIN_INJECT_HTML = """
<div id="grv-login-wrap" role="dialog" aria-modal="true" aria-labelledby="grv-login-title">
  <div class="grv-login-container">
    <div class="grv-login-hero">
      <h1 id="grv-login-title">Global <span class="accent">Retain</span> Viewer</h1>
      <p class="grv-login-hero-sub"><strong>Global Sector</strong> 인원의 어싸인 현황을 한 화면에서 조회 · 매일 1회 자동 갱신</p>
    </div>
    <div class="grv-login-grid">
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
            <button type="button" class="grv-pw-toggle" id="grv-pw-toggle" tabindex="-1" aria-label="사번 보기/숨기기"><svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg></button>
          </div>
        </div>
        <button id="grv-login-btn">로그인</button>
        <p id="grv-login-err">이메일 또는 사번이 올바르지 않습니다.</p>
        <div class="grv-login-tips">
          <h4><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 18h6"/><path d="M10 22h4"/><path d="M12 2a7 7 0 0 1 7 7c0 2.79-1.63 5.24-4 6.46V17H9v-1.54C6.63 14.24 5 11.79 5 9a7 7 0 0 1 7-7z"/></svg> 사용 안내</h4>
          <ul>
            <li>회사 PwC 이메일(@pwc.com) + 사번 6자리 입력</li>
            <li><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="vertical-align:-2px"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg> 아이콘 클릭 시 사번 표시/숨김 전환</li>
            <li>등록되지 않은 사용자는 관리자에게 문의해주세요.</li>
          </ul>
          <p style="margin-top:8px"><strong><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="vertical-align:-2px"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg> 자동 로그아웃</strong>: 브라우저 창을 종료하면 자동으로 로그아웃됩니다. 탭만 닫고 다시 접속하실 경우 로그인 상태가 유지됩니다.</p>
        </div>
        <p class="grv-login-footer">© 2026 Copyright · 오류 및 개선 의견: <a href="mailto:jeesoo.j.kim@pwc.com?subject=%5BGlobal%20Retain%20Viewer%5D%20%EC%9D%98%EA%B2%AC" style="color:#1d4ed8;text-decoration:none;font-weight:600">jeesoo.j.kim@pwc.com</a></p>
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
  var LOG_TOKEN    = [__GRV_LOG_TOKEN_CODES__].map(function(c){return String.fromCharCode(c);}).join('');
  var LOG_REPO     = '__GRV_LOG_REPO__';
  var THEME_KEY    = 'retain_theme';
  var WEATHER_KEY  = 'retain_weather_region';

  // ── 다크/라이트 테마 적용 (로그인 전부터 즉시 반영) ──
  var savedTheme = localStorage.getItem(THEME_KEY) || 'light';
  document.documentElement.setAttribute('data-theme', savedTheme);

  // ── 날씨 위젯 지역 목록 (lat/lng + 한글 라벨) ──
  // wttr.in·api.open-meteo.com 은 PwC 사내망에서 차단(타임아웃) → MET Norway api.met.no 사용 (CORS *, no-auth).
  var REGIONS = [
    {label:'서울 용산구',  lat:37.5384, lng:126.9654},
    {label:'서울 동작구',  lat:37.5124, lng:126.9393},
    {label:'서울 영등포구', lat:37.5264, lng:126.8962},
    {label:'서울 강남구',  lat:37.5172, lng:127.0473},
    {label:'서울 서초구',  lat:37.4837, lng:127.0324},
    {label:'서울 마포구',  lat:37.5663, lng:126.9019},
    {label:'서울 종로구',  lat:37.5735, lng:126.9788},
    {label:'서울 중구',    lat:37.5641, lng:126.9979},
    {label:'성남 분당',    lat:37.3852, lng:127.1227},
    {label:'성남 판교',    lat:37.3953, lng:127.1112},
    {label:'고양 일산',    lat:37.6585, lng:126.7700},
    {label:'부산',         lat:35.1796, lng:129.0756},
    {label:'대구',         lat:35.8714, lng:128.6014},
    {label:'인천',         lat:37.4563, lng:126.7052},
    {label:'광주',         lat:35.1595, lng:126.8526},
    {label:'대전',         lat:36.3504, lng:127.3845},
    {label:'세종',         lat:36.4801, lng:127.2890},
    {label:'울산',         lat:35.5384, lng:129.3114},
  ];

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
  function logLogin(u){
    if (!LOG_TOKEN) return;
    try {
      var now = new Date();
      var kst = new Date(now.getTime() + 9*60*60*1000);
      var ts  = kst.toISOString().replace('T',' ').slice(0,19) + ' KST';
      var apiUrl = 'https://api.github.com/repos/' + LOG_REPO + '/contents/log.csv';
      var hdrs = {
        'Authorization': 'Bearer ' + LOG_TOKEN,
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
        'Content-Type': 'application/json'
      };
      fetch(apiUrl, {headers: hdrs})
        .then(function(r){ return r.json(); })
        .then(function(data){
          var existing, sha;
          if (data.content) {
            existing = decodeURIComponent(escape(atob(data.content.replace(/\\n/g,''))));
            sha = data.sha;
            if (existing.charCodeAt(0) !== 0xFEFF) existing = '\\ufeff' + existing;
          } else {
            existing = '\\ufeff로그인일시,이메일,이름,본부,사번\\n';
            sha = null;
          }
          var newRow = ts + ',' + u.email + ',' + u.name + ',' + u.dept + ',' + (u.sabun||'') + '\\n';
          var csvLines = existing.split('\\n');
          var csvHeader = csvLines[0] + '\\n';
          var csvData = csvLines.slice(1).filter(function(l){ return l.trim() !== ''; });
          var updated = csvHeader + newRow + (csvData.length ? csvData.join('\\n') + '\\n' : '');
          var encoded = btoa(unescape(encodeURIComponent(updated)));
          var body = {message: '로그인: ' + u.name + ' ' + ts, content: encoded};
          if (sha) body.sha = sha;
          return fetch(apiUrl, {method:'PUT', headers:hdrs, body:JSON.stringify(body)});
        })
        .catch(function(){});
    } catch(e){}
  }
  window.grvLogout = function(){
    clearAuthCookie();
    location.reload();
  };

  function showApp(user){
    document.body.classList.remove('grv-locked');
    var w = document.getElementById('grv-login-wrap');
    if (w) w.style.display = 'none';
    // 로그인 사용자 기준 어싸인 현황 기본 검색 — 사번 정확 일치 (메인 스크립트 로드 전이면 GRV_USER로 전달만)
    window.GRV_USER = user;
    if (window.grvApplyDefaultUser && user && user.sabun) { try { window.grvApplyDefaultUser(user.name, user.sabun); } catch(e){} }
    // 헤더 위젯 바 마운트 (날씨·사용자·로그아웃·테마 토글)
    // headerNotice가 initApp에서 display:block 되기 전에 미리 mount해두면
    // 로그인 직후 바로 표시되고, 안 되어 있어도 polling으로 따라잡음
    try { mountWidgetBar(user); } catch(e){ console.error('[mountWidgetBar]', e); }
  }

  function mountWidgetBar(user){
    var notice = document.getElementById('headerNotice');
    if (!notice) {
      // headerNotice 가 아직 없을 수도 있음 — 짧게 polling
      var tries = 0;
      var iv = setInterval(function(){
        tries++;
        if (document.getElementById('headerNotice') || tries > 40){
          clearInterval(iv);
          if (document.getElementById('headerNotice')) mountWidgetBar(user);
        }
      }, 50);
      return;
    }
    if (document.getElementById('grv-widget-bar')) return; // 중복 방지

    // 헤더 알림 영역을 강제로 표시 (initApp 이전에 로그인된 경우에도)
    if (notice.style.display === 'none' || !notice.style.display) {
      notice.style.display = 'block';
    }

    var bar = document.createElement('div');
    bar.id = 'grv-widget-bar';
    bar.innerHTML = buildWidgetBarHTML(user);
    // 알림 박스 위에 형제로 배치. CSS 의 width:100% + max-width:560px + padding:0 16px
    // 가 notice 와 outer 폭·content 영역을 정확히 일치시킴.
    notice.parentNode.insertBefore(bar, notice);

    // 이벤트 와이어
    wireWeatherWidget();
    wireThemeToggle();

    // 프로필 드롭다운
    var profBtn = document.getElementById('grv-profile-btn');
    var profDD  = document.getElementById('grv-profile-dropdown');
    if (profBtn && profDD) {
      profBtn.addEventListener('click', function(e){
        e.stopPropagation();
        var open = profDD.classList.toggle('open');
        profBtn.classList.toggle('open', open);
      });
      document.addEventListener('click', function(){
        profDD.classList.remove('open');
        profBtn.classList.remove('open');
      });
    }

    // 공지 토글
    var noticeToggle = document.getElementById('grv-notice-toggle');
    if (noticeToggle && notice) {
      var updateNoticeBtn = function(){
        var shown = notice.style.display !== 'none' && notice.style.display !== '';
        noticeToggle.classList.toggle('active', shown);
      };
      noticeToggle.addEventListener('click', function(){
        var shown = notice.style.display !== 'none' && notice.style.display !== '';
        notice.style.display = shown ? 'none' : 'block';
        updateNoticeBtn();
        if (window.updateStickyOffsets) updateStickyOffsets();
      });
      updateNoticeBtn();
    }

    // 로그아웃
    document.getElementById('grv-logout-btn').addEventListener('click', window.grvLogout);

    // 첫 날씨 조회
    fetchWeather(localStorage.getItem(WEATHER_KEY) || REGIONS[0].label);
  }

  function buildWidgetBarHTML(user){
    var savedRegion = localStorage.getItem(WEATHER_KEY) || REGIONS[0].label;
    var opts = REGIONS.map(function(r){
      return '<option value="'+r.label+'"'+(r.label===savedRegion?' selected':'')+'>'+r.label+'</option>';
    }).join('');

    var name     = (user.name||'').replace(/</g,'&lt;');
    var dept     = (user.dept||'').replace(/</g,'&lt;');

    var isDark   = document.documentElement.getAttribute('data-theme')==='dark';
    var sunSvg   = '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>';
    var moonSvg  = '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>';
    var monSvg   = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2" ry="2"/><line x1="8" y1="21" x2="16" y2="21"/><line x1="12" y1="17" x2="12" y2="21"/></svg>';
    var phSvg    = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="2" width="14" height="20" rx="2" ry="2"/><line x1="12" y1="18" x2="12.01" y2="18"/></svg>';
    var infoSvg  = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>';
    var chevSvg  = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 9 12 15 18 9"/></svg>';
    var outSvg   = '<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4"/><polyline points="16 17 21 12 16 7"/><line x1="21" y1="12" x2="9" y2="12"/></svg>';

    // Editorial Mono Bar: 각 모듈 = 상단 트래킹 라벨(한글) + 하단 값(아이콘/숫자/이름)
    return [
      // 현재 보기 모드 (클릭 시 PC/모바일 전환 — body.mobile-view 클래스에 따라 CSS로 아이콘·텍스트 자동 스왑)
      '<button type="button" class="grv-mod grv-mod-btn grv-viewmode" id="grv-viewmode" title="클릭하여 PC/모바일 화면 전환" aria-label="화면 모드 전환" onclick="toggleMobileView()">',
        '<span class="grv-mod-label">현재</span>',
        '<span class="grv-mod-body">',
          '<span class="grv-mob-pc">'+monSvg+'<span class="grv-vm-text">PC용 화면</span></span>',
          '<span class="grv-mob-mob">'+phSvg+'<span class="grv-vm-text">모바일용 화면</span></span>',
        '</span>',
      '</button>',
      // 날씨 모듈
      '<div class="grv-mod grv-weather" id="grv-weather">',
        '<span class="grv-mod-label">날씨</span>',
        '<span class="grv-mod-body">',
          '<span class="grv-w-icon" id="grv-weather-icon"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg></span>',
          '<span class="grv-w-temp" id="grv-weather-temp">--°C</span>',
          '<select id="grv-weather-region" title="지역 선택">'+opts+'</select>',
        '</span>',
      '</div>',
      // 공지 토글
      '<button type="button" class="grv-mod grv-mod-btn" id="grv-notice-toggle" title="공지사항 보기/숨기기" aria-label="공지사항">',
        '<span class="grv-mod-label">공지</span>',
        '<span class="grv-mod-body">'+infoSvg+'</span>',
      '</button>',
      // 테마 토글
      '<button type="button" class="grv-mod grv-mod-btn" id="grv-theme-toggle" title="라이트/다크 전환" aria-label="테마 전환">',
        '<span class="grv-mod-label">테마</span>',
        '<span class="grv-mod-body" id="grv-theme-icon">'+(isDark?moonSvg:sunSvg)+'</span>',
      '</button>',
      // 프로필 + 드롭다운 (아바타 제거 — 이름만 표시)
      '<div class="grv-profile-wrap" id="grv-profile-wrap">',
        '<button type="button" class="grv-profile-btn" id="grv-profile-btn" aria-label="프로필 메뉴">',
          '<span class="grv-mod-label">사용자</span>',
          '<span class="grv-mod-body">',
            '<span class="grv-profile-name">'+name+'</span>',
            '<span class="grv-profile-chevron">'+chevSvg+'</span>',
          '</span>',
        '</button>',
        '<div class="grv-dropdown" id="grv-profile-dropdown">',
          '<div class="grv-dd-header">',
            '<div class="grv-dd-label">로그인 정보</div>',
            '<div class="grv-dd-name">'+name+'</div>',
            (dept?'<div class="grv-dd-dept">'+dept+'</div>':''),
          '</div>',
          '<button type="button" class="grv-dd-item danger" id="grv-logout-btn">'+outSvg+' 로그아웃</button>',
        '</div>',
      '</div>',
    ].join('');
  }

  // ── 날씨 (MET Norway locationforecast 2.0) ──
  // 무료, no-auth, CORS *. api.open-meteo.com 은 PwC 사내망 차단으로 교체.
  // 응답: { properties: { timeseries: [ { data: { instant:{details:{air_temperature}}, next_1_hours:{summary:{symbol_code}} } } ] } }
  function fetchWeather(regionLabel){
    var iconEl = document.getElementById('grv-weather-icon');
    var tempEl = document.getElementById('grv-weather-temp');
    if (!iconEl || !tempEl) return;
    var region = REGIONS.filter(function(r){return r.label===regionLabel})[0] || REGIONS[0];
    iconEl.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>';
    var url = 'https://api.met.no/weatherapi/locationforecast/2.0/compact?lat='+region.lat+'&lon='+region.lng;
    fetch(url).then(function(r){ return r.json(); }).then(function(d){
      var ts  = d && d.properties && d.properties.timeseries && d.properties.timeseries[0];
      var det = ts && ts.data && ts.data.instant && ts.data.instant.details;
      if (!det || det.air_temperature === undefined) throw new Error('no forecast data');
      var temp = Math.round(det.air_temperature);
      var sum  = (ts.data.next_1_hours || ts.data.next_6_hours || {}).summary;
      var sym  = (sum && sum.symbol_code) || '';
      iconEl.innerHTML = weatherIcon(sym);
      iconEl.title = weatherDescKR(sym);
      tempEl.textContent = temp + '°C';
    }).catch(function(e){
      iconEl.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>';
      tempEl.textContent = '--°C';
      console.warn('[weather] met.no fetch 실패', e);
    });
  }
  // met.no symbol_code(문자열) → SVG 아이콘 매핑
  // (참고: https://api.met.no/weatherapi/weathericon/2.0/documentation)
  function weatherIcon(sym){
    var sun     = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>';
    var cloud   = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 10h-1.26A8 8 0 1 0 9 20h9a5 5 0 0 0 0-10z"/></svg>';
    var fog     = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9.59 4.59A2 2 0 1 1 11 8H2m10.59 11.41A2 2 0 1 0 14 16H2m15.73-8.27A2.5 2.5 0 1 1 19.5 12H2"/></svg>';
    var rain    = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="16" y1="13" x2="16" y2="21"/><line x1="8" y1="13" x2="8" y2="21"/><line x1="12" y1="15" x2="12" y2="23"/><path d="M20 16.58A5 5 0 0 0 18 7h-1.26A8 8 0 1 0 4 15.25"/></svg>';
    var snow    = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 17.58A5 5 0 0 0 18 8h-1.26A8 8 0 1 0 4 16.25"/><line x1="8" y1="16" x2="8.01" y2="16"/><line x1="8" y1="20" x2="8.01" y2="20"/><line x1="12" y1="18" x2="12.01" y2="18"/><line x1="12" y1="22" x2="12.01" y2="22"/><line x1="16" y1="16" x2="16.01" y2="16"/><line x1="16" y1="20" x2="16.01" y2="20"/></svg>';
    var thunder = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M19 16.9A5 5 0 0 0 18 7h-1.26a8 8 0 1 0-11.62 9"/><polyline points="13 11 9 17 15 17 11 23"/></svg>';
    if (sym.indexOf('thunder') >= 0) return thunder;
    if (sym.indexOf('snow') >= 0 || sym.indexOf('sleet') >= 0) return snow;
    if (sym.indexOf('rain') >= 0) return rain;
    if (sym.indexOf('fog') >= 0) return fog;
    if (sym.indexOf('clearsky') >= 0 || sym.indexOf('fair') >= 0) return sun;
    return cloud;
  }
  function weatherDescKR(sym){
    if (sym.indexOf('thunder') >= 0) return '천둥번개';
    if (sym.indexOf('sleet') >= 0) return '진눈깨비';
    if (sym.indexOf('snowshowers') >= 0) return '눈 소나기';
    if (sym.indexOf('snow') >= 0) return '눈';
    if (sym.indexOf('rainshowers') >= 0) return '소나기';
    if (sym.indexOf('lightrain') >= 0) return '이슬비';
    if (sym.indexOf('rain') >= 0) return '비';
    if (sym.indexOf('fog') >= 0) return '안개';
    if (sym.indexOf('clearsky') >= 0) return '맑음';
    if (sym.indexOf('fair') >= 0) return '대체로 맑음';
    if (sym.indexOf('partlycloudy') >= 0) return '구름 조금';
    if (sym.indexOf('cloudy') >= 0) return '흐림';
    return '-';
  }
  function wireWeatherWidget(){
    var sel = document.getElementById('grv-weather-region');
    if (!sel) return;
    sel.addEventListener('change', function(){
      localStorage.setItem(WEATHER_KEY, sel.value);
      fetchWeather(sel.value);
    });
  }

  // ── 다크/라이트 토글 ──
  function wireThemeToggle(){
    var btn = document.getElementById('grv-theme-toggle');
    var ico = document.getElementById('grv-theme-icon');
    if (!btn) return;
    var sunSvg  = '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"/><line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/><line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/></svg>';
    var moonSvg = '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>';
    btn.addEventListener('click', function(){
      var cur  = document.documentElement.getAttribute('data-theme') || 'light';
      var next = cur === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', next);
      localStorage.setItem(THEME_KEY, next);
      if (ico) ico.innerHTML = next === 'dark' ? moonSvg : sunSvg;
    });
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
    if (inp.type === 'password') { inp.type='text'; btn.innerHTML='<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/><line x1="1" y1="1" x2="23" y2="23"/></svg>'; btn.classList.add('on'); }
    else { inp.type='password'; btn.innerHTML='<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg>'; btn.classList.remove('on'); }
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
        var info = {email:user.email, name:user.name, dept:user.dept, sabun:pw};
        setAuthCookie(info);
        logLogin(info);
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


def inject_login(html, users, build_time, log_token="", log_repo=""):
    """로그인 오버레이 + bcrypt 인라인 + 인증 스크립트를 HTML에 주입."""
    bcrypt_js = _read_bcrypt_inline()
    users_json = json.dumps(users, ensure_ascii=False, separators=(",", ":"))
    js = (LOGIN_INJECT_JS_TEMPLATE
          .replace("/*__GRV_BCRYPT_JS__*/", bcrypt_js)
          .replace("__GRV_USERS_JSON__", users_json)
          .replace("__GRV_BUILD_TIME__", build_time)
          .replace("__GRV_LOG_TOKEN_CODES__", ",".join(str(ord(c)) for c in log_token) if log_token else "")
          .replace("__GRV_LOG_REPO__", log_repo))
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
  h+='<a class="dart-link" href="https://dart.fss.or.kr/dsab001/main.do?option=corp&textCrpNm='+encodeURIComponent(searchName)+'&startDt=20240101&endDt='+new Date().toISOString().slice(0,10).replace(/-/g,'')+'&publicType=&sort=date&series=desc" target="_blank">DART 공시자료 링크 ↗<span class="dart-tip">클릭 시 DART 사이트로 이동됩니다.<br>이동 후 DART 페이지 상의 아래 버튼을 클릭하세요.<br><img class="dart-tip-img" src="data:image/png;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/4gHYSUNDX1BST0ZJTEUAAQEAAAHIAAAAAAQwAABtbnRyUkdCIFhZWiAH4AABAAEAAAAAAABhY3NwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAA9tYAAQAAAADTLQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAlkZXNjAAAA8AAAACRyWFlaAAABFAAAABRnWFlaAAABKAAAABRiWFlaAAABPAAAABR3dHB0AAABUAAAABRyVFJDAAABZAAAAChnVFJDAAABZAAAAChiVFJDAAABZAAAAChjcHJ0AAABjAAAADxtbHVjAAAAAAAAAAEAAAAMZW5VUwAAAAgAAAAcAHMAUgBHAEJYWVogAAAAAAAAb6IAADj1AAADkFhZWiAAAAAAAABimQAAt4UAABjaWFlaIAAAAAAAACSgAAAPhAAAts9YWVogAAAAAAAA9tYAAQAAAADTLXBhcmEAAAAAAAQAAAACZmYAAPKnAAANWQAAE9AAAApbAAAAAAAAAABtbHVjAAAAAAAAAAEAAAAMZW5VUwAAACAAAAAcAEcAbwBvAGcAbABlACAASQBuAGMALgAgADIAMAAxADb/2wBDAAUDBAQEAwUEBAQFBQUGBwwIBwcHBw8LCwkMEQ8SEhEPERETFhwXExQaFRERGCEYGh0dHx8fExciJCIeJBweHx7/2wBDAQUFBQcGBw4ICA4eFBEUHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh7/wAARCAAhAHkDASIAAhEBAxEB/8QAHAABAAMAAwEBAAAAAAAAAAAAAAUGBwIDBAgB/8QANRAAAQQCAAQEAwMNAAAAAAAAAQACAwQFEQYSITEHExRRIkFhFSNyFiUyM1RWcYGRk5Sy0v/EABsBAAMBAAMBAAAAAAAAAAAAAAADBAIBBQYH/8QALBEAAQQBAgUDAgcAAAAAAAAAAQACAxEEITESE0FRYQWB0bHBIkJxkaHh8f/aAAwDAQACEQMRAD8A4IiL66vGIpJmAzr6nq24XJOr635oqvLNe+9aVn8KMdRd9r8QZGAWIMRA2QRHs5zubX+q63+JvFTsh6httjIObfpgweVr20oZMiZ0jo4Wg8O9mt9aHsqGxMDQ5532pUogg6I0UV+8UKdC1iMPxZQrtrDJtc2aNvbzGkgkf0VBT8acTxh9V48jQpcsfLdwoiunhbFxw63YHB7jA2XlZZnfGwxt1sjZeD7noOqu/G+ewuTxDeEcvxsGzwFsl2+2gZWTSAn7toj0AAeu/oB1O1JPnuimEYaHd6JJA7kBp9tdU6LHD2FxNfQ+Lv8AxY7ex9+gIjeo2aombzxGaJzOdvu3Y6j6heZan45VjLNwrUol9suoCOEsjIdL+iGkN79fZTUfCViPwsfwc61XHEM35xFIyDm0HD4f46Gvbf06pQ9VaII5HjVxquwsgn9B1XIxHOeWDoAf3F17nQLI4sDnZvK8rC5KTzojNFy1XnnjGtvb06t+IdR06j3UcvpjhmnHXqcPNtTOgvwcPyRGo6I7I+553F3YFpDRrueb6L5nTPT885bniq4fkj7Iycbktab3+AfuiIi7NSL8IBGiNgqv+suftc/9wqwqrqXJANJsR3VoREVSUrZ4bcQU8PduUcq1zsZkohFY18tb0f5bKnH+HWJM3rY+Lcb9k75uYvPm8v4dLN0UUuI4yGSJ/CTvpd/2nsmAaGvbdbLUfF2m08N4aTBSx2eH6sfI18Z7SEnZIPbe1ly723bbaTqQsyis53MYuY8pPvpdC3h47sePlk3qdfnyszSCR3EBSmuC3yycR0MaZ52VL9uGC1FHK5gmjLwC13KRsaJWicR4XC4vN2qFPwkymRghcAyzFctcsg0DsaaR89dz2WSQSywTxzwSviljcHsexxa5rgdggjsQpf8AK3ir95s1/nS/9JGViySyB7HUK1FuGvf8JCZDM1jSHD+Afqrb420oaL+H314blYyUucwWLcsxhPT4B5hJbrtoa7dlBcOcT4qk99vNYK1mcoZedl45aaCRjeUAN+H20eu/nr5KAyeUyeUex+SyNu65g0x1iZ0haPYcxOl5FuDD4ccRSmzrqCR1ve7WZJrk42Dt0HQVtsto8Pczg+KstLQiwmSxz6tCR0Uoz9qTlYHMBYBsaaeh1vXwjosXXpx9+/jpnTY+7ZpyuYWOfBK6Nxae4JB7dB0+i8y5xsQY8jy06GtCSaq73J3tEs/MaARqL7eOyIiK1IRVdWhVdS5PRNj6q0IiKpKRERCEREQhEREIRERCEREQhEREIXGX9U/8JUmiLrPUfy+6rxuq/9k=" width="84" height="23" alt="검색"></span></a>';
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
        html=inject_login(html, users, build_time, log_token=GH_LOG_TOKEN, log_repo=GH_LOG_REPO)
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
    tl_div=f'<div style="position:fixed;bottom:8px;right:12px;font-size:9px;color:#64748b;letter-spacing:-0.3px;z-index:1">데이터 기준: {du}</div>'
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
def _gh_req(method, url, headers, timeout=60, retries=3, verify=False, **kwargs):
    """502/503/504 transient 에러 자동 재시도 래퍼 (blob 제외 범용)"""
    import time
    for attempt in range(1, retries + 1):
        r = getattr(req_lib, method)(url, headers=headers, timeout=timeout, verify=verify, **kwargs)
        if r.status_code not in (502, 503, 504) or attempt == retries:
            return r
        wait = 10 * attempt
        print(f"  [{method.upper()}] {r.status_code} — {wait}초 후 재시도 ({attempt}/{retries})...")
        time.sleep(wait)
    return r

def push_gh(ip):
    import time
    if not GITHUB_TOKEN:
        print(f"\n⚠ GitHub 토큰 미설정 (.env 확인) → {ip}"); return False
    print("\nGitHub 업로드...")
    base = f"https://api.github.com/repos/{GITHUB_REPO}"
    h = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}

    with open(ip, "rb") as f: b64 = base64.b64encode(f.read()).decode()

    # 1) blob 생성 — 대용량(~30MB) 업로드, 최대 5회 지수 백오프 재시도
    blob_sha = None
    for attempt in range(1, 6):
        try:
            r = req_lib.post(f"{base}/git/blobs", headers=h,
                             json={"content": b64, "encoding": "base64"},
                             timeout=360, verify=False)
        except Exception as e:
            print(f"  blob 시도 {attempt}/5 예외: {e}")
            if attempt == 5:
                print("❌ blob 최종 실패"); return False
            wait = min(15 * (2 ** (attempt - 1)), 120)
            print(f"  {wait}초 후 재시도...")
            time.sleep(wait)
            continue
        if r.status_code == 201:
            blob_sha = r.json()["sha"]
            break
        print(f"  blob 시도 {attempt}/5 실패: {r.status_code} {r.text[:200]}")
        if r.status_code not in (502, 503, 504) or attempt == 5:
            print("❌ blob 최종 실패"); return False
        wait = min(15 * (2 ** (attempt - 1)), 120)  # 15 → 30 → 60 → 120초
        print(f"  {wait}초 후 재시도...")
        time.sleep(wait)
    print(f"  blob SHA: {blob_sha[:12]}...")

    # 2) 현재 브랜치 최신 커밋 조회
    r = _gh_req('get', f"{base}/git/refs/heads/main", h, timeout=30)
    if r.status_code != 200: print(f"❌ ref 조회 실패: {r.status_code}"); return False
    latest_commit = r.json()["object"]["sha"]

    # 3) 현재 tree SHA 조회
    r = _gh_req('get', f"{base}/git/commits/{latest_commit}", h, timeout=30)
    if r.status_code != 200: print(f"❌ commit 조회 실패: {r.status_code}"); return False
    base_tree = r.json()["tree"]["sha"]

    # 4) 새 tree 생성
    r = _gh_req('post', f"{base}/git/trees", h, timeout=60, json={
        "base_tree": base_tree,
        "tree": [{"path": GITHUB_FILE, "mode": "100644", "type": "blob", "sha": blob_sha}]
    })
    if r.status_code != 201: print(f"❌ tree 실패: {r.status_code} {r.text[:200]}"); return False
    new_tree = r.json()["sha"]

    # 5) 새 커밋 생성
    msg = f"Update ({datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d')})"
    r = _gh_req('post', f"{base}/git/commits", h, timeout=60,
                json={"message": msg, "tree": new_tree, "parents": [latest_commit]})
    if r.status_code != 201: print(f"❌ commit 실패: {r.status_code} {r.text[:200]}"); return False
    new_commit = r.json()["sha"]

    # 6) 브랜치 ref 업데이트
    r = _gh_req('patch', f"{base}/git/refs/heads/main", h, timeout=30,
                json={"sha": new_commit})
    if r.status_code in (200, 201): print("✅ GitHub 완료"); return True
    print(f"❌ ref 업데이트 실패: {r.status_code} {r.text[:200]}"); return False

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
