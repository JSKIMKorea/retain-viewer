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
else:
    # 로컬 PC: 기존 경로
    OUTPUT_DIR    = os.path.join(_SCRIPT_DIR, "Raw data")
    DART_CACHE    = os.path.join(_SCRIPT_DIR, "dart_cache", "dart_details.json")
    NEWS_CACHE    = os.path.join(_SCRIPT_DIR, "news_cache", "daily_news.json")
    HTML_TEMPLATE = os.path.join(_SCRIPT_DIR, "02.html", "Project_Allocation_Viewer.html")
    INDEX_OUTPUT  = os.path.join(OUTPUT_DIR, "index.html")

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
def build_html(df25, du="", dart=None, news=None):
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
        ip,vp=build_html(df25,du,dart,news)
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
