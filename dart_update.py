"""
DART 공시정보 업데이트
======================
Azure의 프로젝트 목록 기준으로 DART 기업정보를 수집하여 dart_cache에 저장합니다.
원할 때 실행하면 최신 공시/재무 정보로 갱신됩니다.

사용법: 더블클릭 또는 python dart_update.py
"""

import pyodbc, pandas as pd, requests as req_lib
from datetime import date, datetime, timezone, timedelta

# 한국시간 헬퍼
def _kst_now():
    return datetime.now(timezone(timedelta(hours=9)))
import os, sys, json, re, zipfile, io, time
import xml.etree.ElementTree as ET

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
DART_API_KEY   = os.getenv("DART_API_KEY", "")
DART_CACHE_DIR = os.path.join(_SCRIPT_DIR, "dart_cache")
DART_ALIASES   = os.path.join(DART_CACHE_DIR, "dart_aliases.json")

# 퍼지 매칭 시 제거할 비즈니스 키워드 (순서 중요: 긴 것부터)
BIZ_WORDS = [
    "테크놀로지스","테크놀로지","테크놀러지","인터내셔널","홀딩스",
    "바이오텍","바이오파마","바이오사이언스","바이오로직스","바이오",
    "파마슈티컬","파마텍","파마","글로벌","코리아","엔터프라이즈",
    "엔터테인먼트","솔루션즈","솔루션","사이언스","이노베이션",
    "커뮤니케이션즈","커뮤니케이션","테라퓨틱스","메디칼","메디컬",
    "일렉트로닉스","일렉트릭","인더스트리","인더스트리얼",
    "시스템즈","시스템","네트웍스","네트워크","소프트",
    "케미칼","머티리얼즈","머티리얼","에너지","파워",
    "디벨롭먼트","프로퍼티","캐피탈","파이낸셜","벤처스",
    "모빌리티","로보틱스","세미콘","디스플레이",
    "제약","건설","중공업","전자","물산","상사","산업",
    "생명과학","생명","제철","화학","정보통신",
]

# 한글 알파벳 발음 → 영문자 매핑 (개별 글자 단위, 긴 것부터)
LETTER_MAP = [
    ("에이치","H"), ("더블유","W"),
    ("에이","A"), ("에스","S"), ("에프","F"),
    ("케이","K"), ("아이","I"), ("제이","J"),
    ("와이","Y"), ("브이","V"),
    ("엘","L"), ("엠","M"), ("엔","N"),
    ("비","B"), ("씨","C"), ("디","D"), ("지","G"),
    ("피","P"), ("티","T"), ("알","R"), ("큐","Q"), ("오","O"),
]
SPECIAL_CHAR = [("앤","&")]

# 한글 브랜드명 → 영문 브랜드명 (개별 글자 변환보다 우선 적용)
BRAND_MAP = [
    ("엘지", "LG"), ("에스케이", "SK"), ("케이티", "KT"),
    ("씨제이", "CJ"), ("지에스", "GS"), ("에이치디", "HD"),
    ("비엔케이", "BNK"), ("디비", "DB"), ("케이비", "KB"),
    ("엔에이치", "NH"), ("아이비케이", "IBK"), ("오씨아이", "OCI"),
    ("에이치엘비", "HLB"), ("이씨에스", "ECS"), ("에스디", "SD"),
]
ENG_KOR = {
    'A':'에이','B':'비','C':'씨','D':'디','E':'이',
    'F':'에프','G':'지','H':'에이치','I':'아이','J':'제이',
    'K':'케이','L':'엘','M':'엠','N':'엔','O':'오',
    'P':'피','Q':'큐','R':'알','S':'에스','T':'티',
    'U':'유','V':'브이','W':'더블유','X':'엑스','Y':'와이','Z':'지',
    '&':'앤',
}

# 외래어 표기 변형 쌍 (같은 영어 원어의 다른 한글 표기)
FOREIGN_VARIANTS = [
    ("내셔널","내셔날"), ("내셔널","내서날"), ("내셔날","내서날"),
    ("인터내셔널","인터내셔날"), ("인터내셔널","인터내서날"), ("인터내셔날","인터내서날"),
    ("테크놀로지","테크놀러지"), ("테크놀로지스","테크놀러지스"),
    ("솔루션","솔루숀"), ("커뮤니케이션","커뮤니케이숀"),
    ("파마슈티컬","파마수티컬"), ("파마슈티칼","파마수티칼"),
    ("매니지먼트","매니지멘트"), ("디벨롭먼트","디벨롭멘트"),
    ("어드바이저리","어드바이져리"), ("어드바이저","어드바이져"),
    ("엔터테인먼트","엔터테인멘트"), ("엔터프라이즈","엔터프라이즈스"),
    ("홀딩스","홀딩즈"), ("시스템즈","시스템스"),
    ("파트너스","파트너즈"), ("벤처스","벤쳐스"),
    ("로지스틱스","로지스틱"),
    ("프로페셔널","프로페셔날"), ("프로페셔널","프로페서날"),
]

# ============================================================
# Azure SQL - 프로젝트명 가져오기
# ============================================================
def get_project_names():
    print("Azure SQL 연결 중...")
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};Encrypt=yes;TrustServerCertificate=no;")
    try:
        df = pd.read_sql("""
            SELECT DISTINCT r.PRJTNM
            FROM BI_STAFFREPORT_RETAIN_V r
            INNER JOIN BI_STAFFREPORT_EMP_V e ON r.EMPNO=e.EMPNO
            WHERE e.ORG_NM IN ('Global CMAAS','IOA','Global IPO','Assurance NGH')
              AND r.YMD >= '2025-01-01'
        """, conn)
    finally:
        conn.close()
    names = df["PRJTNM"].dropna().unique().tolist()
    print(f"  → 2025년 이후 프로젝트 {len(names):,}개")
    return names

# ============================================================
# DART API 함수들
# ============================================================
def load_corp_codes():
    os.makedirs(DART_CACHE_DIR, exist_ok=True)
    cf = os.path.join(DART_CACHE_DIR, "corps.json")
    if os.path.exists(cf) and datetime.fromtimestamp(os.path.getmtime(cf)).date() == date.today():
        with open(cf, "r", encoding="utf-8") as f:
            return json.load(f)
    print("  DART 기업코드 다운로드...")
    r = req_lib.get(f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={DART_API_KEY}", timeout=30)
    if r.status_code != 200:
        print(f"  ⚠ 다운로드 실패: {r.status_code}")
        return {}
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    root = ET.fromstring(zf.read(zf.namelist()[0]))
    corps = {}
    for it in root.findall(".//list"):
        cn = it.findtext("corp_name", "")
        if cn:
            corps[cn] = {"cc": it.findtext("corp_code", ""), "sc": (it.findtext("stock_code", "") or "").strip()}
    with open(cf, "w", encoding="utf-8") as f:
        json.dump(corps, f, ensure_ascii=False)
    print(f"  → {len(corps):,}개 기업 로드")
    return corps

def client_name(pn):
    if not pn: return ""
    n = pn.split("/")[0].strip()
    if "_" in n: n = n.split("_")[0].strip()
    return re.sub(r'\s*\d{2,4}(년|월|분기)?\s*(회계)?감사.*$', '', n).strip()

def _strip_corp(n):
    """법인 유형 접두/접미 모두 제거"""
    return re.sub(r'\(주\)|\(유\)|\(합\)|\(사\)|\(재\)|주식회사|유한회사|유한책임회사|사단법인|재단법인', '', n).strip()

def load_aliases():
    """dart_aliases.json에서 수동 매핑 로드 (없으면 빈 dict)"""
    if os.path.exists(DART_ALIASES):
        with open(DART_ALIASES, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"  → 수동 별칭 {len(data)}개 로드")
        return data
    return {}

def _strip_biz(n):
    """비즈니스 키워드 제거 (킵스바이오파마 → 킵스)"""
    result = n
    for w in BIZ_WORDS:
        result = result.replace(w, "")
    return result.strip()

def _foreign_variants(name):
    """외래어 표기 변형 생성 (내셔널↔내서날 등)"""
    variants = set()
    for a, b in FOREIGN_VARIANTS:
        if a in name: variants.add(name.replace(a, b))
        if b in name: variants.add(name.replace(b, a))
    variants.discard(name)
    return list(variants)

def _transliterate(n):
    """한영 음차 변환 + 외래어 표기 변형 — 모든 변형을 생성"""
    variants = set()
    for fn in [_kor_to_eng_prefix, _kor_to_eng_all, _eng_to_kor_prefix, _eng_to_kor_all]:
        v = fn(n)
        if v: variants.add(v)
    # 외래어 표기 변형
    for v in _foreign_variants(n):
        variants.add(v)
    variants.discard(n)
    return list(variants)

def _kor_to_eng_prefix(name):
    """앞부분 한글 알파벳만 영문 변환 (브랜드명 우선, 2글자+, 비알파벳 만나면 즉시 중단)"""
    # 1차: 브랜드명 매핑 (엘지→LG, 에스케이→SK 등)
    for kor, eng in BRAND_MAP:
        if name.startswith(kor):
            return eng + name[len(kor):]
    # 2차: 개별 글자 변환
    pos = 0; letters = []
    while pos < len(name):
        matched = False
        for kor, eng in SPECIAL_CHAR + LETTER_MAP:
            if name[pos:].startswith(kor):
                letters.append(eng); pos += len(kor); matched = True; break
        if not matched: break
    if len(letters) < 2: return None
    return "".join(letters) + name[pos:]

def _kor_to_eng_all(name):
    """전체 문자열에서 브랜드명 + 2글자+ 연속 구간을 영문 변환"""
    # 브랜드명 먼저 치환
    result_name = name
    for kor, eng in BRAND_MAP:
        result_name = result_name.replace(kor, eng)
    if result_name != name:
        return result_name
    # 개별 글자 변환
    tokens = []; pos = 0
    while pos < len(name):
        matched = False
        for kor, eng in SPECIAL_CHAR + LETTER_MAP:
            if name[pos:].startswith(kor):
                tokens.append((name[pos:pos+len(kor)], eng))
                pos += len(kor); matched = True; break
        if not matched:
            tokens.append((name[pos], None)); pos += 1
    result = []; i = 0
    while i < len(tokens):
        if tokens[i][1] is not None:
            rs = i
            while i < len(tokens) and tokens[i][1] is not None: i += 1
            if (i - rs) >= 2:
                for j in range(rs, i): result.append(tokens[j][1])
            else:
                result.append(tokens[rs][0])
        else:
            result.append(tokens[i][0]); i += 1
    out = "".join(result)
    return out if out != name else None

def _eng_to_kor_prefix(name):
    """앞부분 영문자를 한글 발음 변환 (브랜드명 우선, 2글자+)"""
    # 1차: 브랜드명 역매핑 (LG→엘지, SK→에스케이 등)
    for kor, eng in BRAND_MAP:
        if name.upper().startswith(eng):
            return kor + name[len(eng):]
    # 2차: 개별 글자 변환
    pos = 0; letters = []
    while pos < len(name):
        ch = name[pos].upper()
        if ch in ENG_KOR:
            letters.append(ENG_KOR[ch]); pos += 1
        else: break
    if len(letters) < 2: return None
    return "".join(letters) + name[pos:]

def _eng_to_kor_all(name):
    """전체 문자열에서 2글자+ 영문 연속 구간을 한글 발음 변환"""
    tokens = []
    for ch in name:
        if ch.upper() in ENG_KOR: tokens.append((ch, ENG_KOR[ch.upper()]))
        else: tokens.append((ch, None))
    result = []; i = 0
    while i < len(tokens):
        if tokens[i][1] is not None:
            rs = i
            while i < len(tokens) and tokens[i][1] is not None: i += 1
            if (i - rs) >= 2:
                for j in range(rs, i): result.append(tokens[j][1])
            else:
                result.append(tokens[rs][0])
        else:
            result.append(tokens[i][0]); i += 1
    out = "".join(result)
    return out if out != name else None

def _build_stripped_index(corps):
    """corps dict에서 _strip_corp된 이름 → 원본이름 역인덱스"""
    idx = {}
    for dn in corps:
        sc = _strip_corp(dn)
        if sc:
            idx.setdefault(sc, []).append(dn)
    return idx

def _build_ci_index(corps):
    """대소문자 무시 인덱스: lower(이름) → [원본이름]"""
    idx = {}
    for dn in corps:
        idx.setdefault(dn.lower(), []).append(dn)
        sc = _strip_corp(dn)
        if sc and sc != dn:
            idx.setdefault(sc.lower(), []).append(dn)
    return idx

def _ci_find(name, corps, ci_idx):
    """대소문자 무시 조회 — 유일매칭만 반환"""
    matches = ci_idx.get(name.lower(), [])
    if len(matches) == 1:
        return corps[matches[0]]
    return None

def find_corp(c, corps, stripped_idx=None, aliases=None, ci_idx=None):
    """반환: (corp_info, match_type) 또는 (None, None)
    match_type: 'exact', 'strip', 'alias', 'fuzzy:키워드제거', 'fuzzy:접두사', None
    """
    if not c: return None, None

    # 0단계: 수동 별칭 우선
    if aliases and c in aliases:
        dart_name = aliases[c]
        if dart_name == "":  # 빈 문자열 = 명시적 스킵
            return None, "skip"
        if dart_name in corps:
            return corps[dart_name], "alias"
        found, _ = find_corp(dart_name, corps, stripped_idx, ci_idx=ci_idx)
        if found: return found, "alias"

    # 1단계: 원본 그대로 (+ 대소문자 무시)
    if c in corps: return corps[c], "exact"
    if ci_idx:
        r = _ci_find(c, corps, ci_idx)
        if r: return r, "exact"

    cl = _strip_corp(c)
    if not cl: return None, None

    # 2단계: strip 후 정확 매칭 (+ 대소문자 무시)
    if cl in corps: return corps[cl], "exact"
    if ci_idx:
        r = _ci_find(cl, corps, ci_idx)
        if r: return r, "exact"
    for prefix in ["", "(주)", "(유)", "(합)", "주식회사 ", "유한회사 "]:
        for suffix in ["", "(주)", "(유)"]:
            v = (prefix + cl + suffix).strip()
            if v and v in corps: return corps[v], "exact"
            if ci_idx:
                r = _ci_find(v, corps, ci_idx)
                if r: return r, "exact"

    # 3단계: corps 전체에서 strip 비교
    if stripped_idx:
        matches = stripped_idx.get(cl, [])
        if len(matches) == 1:
            return corps[matches[0]], "strip"
    else:
        for dn, inf in corps.items():
            if _strip_corp(dn) == cl: return inf, "strip"
    # 3단계 CI fallback
    if stripped_idx and ci_idx:
        r = _ci_find(cl, corps, ci_idx)
        if r: return r, "strip"

    # 3.5단계: 한영 음차 변환 후 1-3단계 재시도 (에이치엘만도 ↔ HL만도)
    for variant in _transliterate(cl):
        if variant in corps: return corps[variant], f"translit:{cl}→{variant}"
        if ci_idx:
            r = _ci_find(variant, corps, ci_idx)
            if r: return r, f"translit:{cl}→{variant}"
        for prefix in ["", "(주)", "(유)", "주식회사 "]:
            v = (prefix + variant).strip()
            if v and v in corps: return corps[v], f"translit:{cl}→{variant}"
        if stripped_idx:
            matches = stripped_idx.get(variant, [])
            if len(matches) == 1:
                return corps[matches[0]], f"translit:{cl}→{variant}"

    # 4단계: 비즈니스 키워드 제거 후 매칭
    cl_biz = _strip_biz(cl)
    if cl_biz and cl_biz != cl and len(cl_biz) >= 2:
        if cl_biz in corps: return corps[cl_biz], f"fuzzy:{cl}→{cl_biz}"
        for prefix in ["", "(주)", "(유)", "주식회사 "]:
            v = (prefix + cl_biz).strip()
            if v and v in corps: return corps[v], f"fuzzy:{cl}→{cl_biz}"
        candidates = []
        check_items = stripped_idx.items() if stripped_idx else [(k, [k]) for k in corps]
        for stripped_name, orig_names in check_items:
            corp_biz = _strip_biz(stripped_name)
            if corp_biz and corp_biz == cl_biz:
                candidates.extend(orig_names if isinstance(orig_names, list) else [orig_names])
        if len(candidates) == 1:
            return corps[candidates[0]], f"fuzzy:{cl}→{cl_biz}={candidates[0]}"

    # 5단계: 접두사 매칭 (최소 3글자 접두사 공유 + 유일매칭)
    if len(cl) >= 3:
        prefix_candidates = []
        check_items = stripped_idx.items() if stripped_idx else [(k, [k]) for k in corps]
        for stripped_name, orig_names in check_items:
            if len(stripped_name) < 2: continue
            if (cl.startswith(stripped_name) and len(stripped_name) >= 3) or \
               (stripped_name.startswith(cl) and len(cl) >= 3):
                prefix_candidates.extend(orig_names if isinstance(orig_names, list) else [orig_names])
        if len(prefix_candidates) == 1:
            return corps[prefix_candidates[0]], f"prefix:{cl}⊃{_strip_corp(prefix_candidates[0])}"

    return None, None

KSIC = {
    "01":"농업","01220":"화훼작물 재배업","0321":"해면양식업",
    "10":"식료품 제조업","1012":"도축업","10121":"육류 가공업","10211":"수산물 가공업","1030":"과실·채소 가공업","104":"곡물가공품 제조업","10411":"곡물도정업","10412":"제분업","105":"전분·당류 제조업","1070":"기타 식품 제조업","10701":"전분 제조업","10709":"기타 식품 제조업","108":"동물용 사료 및 조제식품 제조업","10891":"동물용 사료 제조업","10897":"건강기능식품 제조업","10899":"기타 식료품 제조업","109":"사료 및 조제식품 제조업","1090":"동물용 사료 및 조제식품 제조업","10901":"배합사료 제조업","10902":"기타 동물용 사료 제조업",
    "11":"음료 제조업","11112":"맥주 제조업","11122":"소주 제조업","11209":"기타 비알코올음료 제조업","12":"담배 제조업","12000":"담배 제조업",
    "13":"섬유제품 제조업","132":"직물직조 및 직물제품 제조업","1322":"기타 직물 제조업","13992":"부직포 제조업",
    "14":"의복 제조업","141":"봉제의복 제조업","14112":"남자용 겉옷 제조업","14120":"여자용 겉옷 제조업","1419":"기타 봉제의복 제조업","14199":"기타 봉제의복 제조업",
    "15":"가죽·가방·신발 제조업","15121":"핸드백 및 가방 제조업","15190":"기타 가죽제품 제조업","152":"신발 및 신발부분품 제조업",
    "17":"펄프·종이 제조업","171":"펄프·종이 제조업","17223":"골판지 및 골판지상자 제조업","179":"기타 종이 및 판지제품 제조업",
    "18":"인쇄업","1811":"인쇄업","19":"코크스·석유정제품 제조업","192":"석유정제품 제조업","19210":"석유정제품 제조업","19221":"윤활유 제조업",
    "20":"화학물질·화학제품 제조업","201":"기초화학물질 제조업","20111":"석유화학계 기초화학물질 제조업","20119":"기타 기초유기화학물질 제조업","2012":"기초무기화학물질 제조업","20122":"산업용 가스 제조업","20129":"기타 기초무기화학물질 제조업","20132":"합성수지 및 기타 플라스틱물질 제조업","20201":"비료 제조업","20202":"농약 제조업","2032":"접착제 및 젤라틴 제조업","204":"화장품·비누·세제 제조업","20411":"비누 및 세제 제조업","20412":"치약·치솔 및 기타 구강위생용품 제조업","20422":"치약 및 구강청정제 제조업","20423":"화장품 제조업","2049":"기타 화학제품 제조업","20494":"사진화학제품 제조업","20499":"기타 화학제품 제조업","2050":"화학섬유 제조업",
    "21":"의료용 물질·의약품 제조업","211":"의약품 제조업","2110":"의약품 제조업","21100":"완제 의약품 제조업","212":"의료용품 및 기타 의약관련제품 제조업","2121":"의료용품 제조업","21211":"의약용 화합물 및 항생물질 제조업","21212":"생물학적 제제 제조업","21230":"의료용 기기 제조업","213":"의료용 기기 제조업","2130":"의료용 기기 제조업","21309":"기타 의료용 기기 제조업",
    "22":"고무·플라스틱 제조업","2219":"기타 고무제품 제조업","222":"플라스틱 제품 제조업","22211":"플라스틱 창·문 및 관련부품 제조업","22214":"플라스틱 포장재 제조업","2224":"플라스틱 성형품 제조업","22241":"플라스틱 사출성형제품 제조업","2229":"기타 플라스틱 제품 제조업",
    "23":"비금속 광물제품 제조업","231":"유리 및 유리제품 제조업","2312":"판유리 가공업","23121":"판유리 가공 및 기타 유리 제조업","2321":"시멘트 제조업","23222":"콘크리트 제품 제조업","23322":"타일 및 유사 비내화요업제품 제조업","23323":"위생용 도자기 제조업","23993":"기타 비금속 광물제품 제조업",
    "24":"1차 금속 제조업","241":"제철 및 제강업","2411":"제철업","24112":"제강업","24122":"합금철 제조업","24123":"주물 주강품 제조업","2421":"동 제련·정련 및 합금 제조업","24212":"알루미늄 제련·정련 및 합금 제조업","24213":"연 및 아연 제련·정련 및 합금 제조업","24219":"기타 비철금속 제련·정련 및 합금 제조업","24221":"동 압연·압출 및 연신제품 제조업","24222":"알루미늄 압연·압출 및 연신제품 제조업","24229":"기타 비철금속 압연·압출 및 연신 제조업","2429":"기타 비철금속 제련·정련 및 합금 제조업","243":"금속 주조업",
    "25":"금속가공제품 제조업","25111":"구조용 금속제품 제조업","25119":"기타 구조용 금속제품 제조업","25130":"증기발생기 및 원자로 제조업","259":"기타 금속가공제품 제조업","2591":"금속 단조·압형·분말야금제품 제조업","25911":"금속 열처리업","25921":"절삭가공 금속제품 제조업","25923":"금속 파스너 및 스프링 제조업","25929":"기타 금속가공제품 제조업","25932":"금형 제조업","25934":"금속 절삭가공업","25991":"금속 열처리업","25993":"도금업",
    "26":"전자부품·컴퓨터·통신장비 제조업","261":"반도체 제조업","2611":"전자집적회로 제조업","26111":"메모리용 전자집적회로 제조업","26112":"비메모리용 및 기타 전자집적회로 제조업","2612":"다이오드·트랜지스터 및 유사 반도체소자 제조업","26129":"기타 반도체소자 제조업","262":"전자부품 제조업","2621":"액정표시장치 제조업","26211":"액정 표시장치 제조업","2622":"인쇄회로기판 제조업","2629":"기타 전자부품 제조업","26293":"전자감시장치 제조업","26294":"전자카드 제조업","26299":"기타 전자부품 제조업","26321":"텔레비전 제조업","264":"통신장비 제조업","26410":"유선 통신장비 제조업","2642":"방송장비 제조업","26429":"기타 방송장비 제조업","265":"영상 및 음향기기 제조업","26519":"기타 영상기기 제조업",
    "27":"의료·정밀·광학기기 제조업","271":"의료용 기기 제조업","27111":"방사선 의료용 기기 제조업","27112":"전자 의료용 기기 제조업","2719":"기타 의료용 기기 제조업","27195":"정형외과용 기기 제조업","27196":"안과용 기기 제조업","27199":"기타 의료용 기기 제조업","27212":"광학렌즈 및 광학요소 제조업","27213":"광학기기 제조업","27216":"사진기 및 관련장비 제조업","27219":"기타 광학기기 제조업",
    "28":"전기장비 제조업","28111":"전동기 및 발전기 제조업","2812":"변압기 제조업","28121":"변압기 제조업","28123":"배전반 및 전기자동제어반 제조업","28201":"일차전지 제조업","28202":"축전지 제조업","2830":"절연선 및 케이블 제조업","28302":"광섬유 케이블 제조업","28410":"전구 및 조명장치 제조업","2851":"가정용 전기기기 제조업","28511":"가정용 전기 냉방기기 제조업","28519":"기타 가정용 전기기기 제조업","289":"기타 전기장비 제조업","28903":"전기경보 및 신호장치 제조업","28909":"기타 전기장비 제조업",
    "29":"기타 기계·장비 제조업","291":"일반 목적용 기계 제조업","29119":"기타 액체 펌프 제조업","2912":"공기 및 가스 압축기 제조업","29120":"공기 및 가스 압축기 제조업","29131":"탭·밸브 및 유사장치 제조업","29132":"유압기기 제조업","29133":"공유압기기 제조업","29141":"산업용 오븐·노 및 노용 버너 제조업","29150":"산업용 냉장·냉동 및 공기조화장치 제조업","29161":"산업용 냉장·냉동장치 제조업","29162":"공기조화장치 제조업","29172":"포장용 기계 제조업","29176":"이화학기기 제조업","29180":"기타 일반 목적용 기계 제조업","29192":"반도체·디스플레이 제조용 기계 제조업","29199":"기타 특수 목적용 기계 제조업","292":"특수 목적용 기계 제조업","29210":"농업용 기계 제조업","29223":"식품가공기계 제조업","29241":"인쇄 및 제본기계 제조업","29250":"반도체·디스플레이 제조용 기계 제조업","2927":"반도체·디스플레이 제조용 기계 제조업","29271":"반도체 제조용 기계 제조업","29272":"디스플레이 제조용 기계 제조업","2928":"자동조립 및 검사장비 제조업","29280":"산업용 로봇 제조업","2929":"기타 특수 목적용 기계 제조업","29293":"섬유기계 제조업","29299":"기타 특수 목적용 기계 제조업",
    "30":"자동차 및 트레일러 제조업","30121":"자동차 차체부품 제조업","303":"자동차 부품 제조업","30320":"자동차 전기·전자장치 제조업","30331":"자동차 엔진부품 제조업","30392":"자동차 동력전달장치 제조업","30399":"기타 자동차 부품 제조업",
    "31":"기타 운송장비 제조업","3111":"강선 건조업","31114":"선박 구성부분품 제조업","3131":"항공기 부품 제조업","31321":"항공기용 엔진 및 부품 제조업",
    "32":"가구 제조업","32021":"주방용 가구 제조업","32029":"기타 가구 제조업","33":"기타 제품 제조업","331":"귀금속 및 장신구 제조업","339":"기타 제품 제조업","33999":"기타 제품 제조업",
    "35":"전기·가스·증기 공급업","351":"전기업","3511":"발전업","35113":"태양력 발전업","35114":"풍력 발전업","35115":"기타 발전업","3520":"가스업","35200":"가스업","353":"증기·냉온수 및 공기조절 공급업","35300":"증기·냉온수 및 공기조절 공급업",
    "38":"폐기물 처리업","382":"폐기물 처리업","39":"환경 복원업","39009":"환경정화 및 복원업",
    "41":"종합 건설업","4111":"주거용 건물 건설업","41112":"아파트 건설업","41119":"기타 주거용 건물 건설업","4112":"비주거용 건물 건설업","412":"토목건설업","41221":"토목시설물 건설업","42":"전문직별 공사업","421":"기반조성 및 시설물 축조관련 전문공사업","423":"건물설비 설치 공사업","42322":"전기공사업","424":"실내건축 및 건축마무리 공사업",
    "45":"자동차 판매업","451":"자동차 판매업","45110":"자동차 신품 판매업","45120":"자동차 중고품 판매업","4521":"자동차 부품 판매업","45213":"자동차 부품 및 내장품 판매업",
    "46":"도매업","461":"도매 중개업","4610":"도매 중개업","46107":"화학제품 중개업","46109":"기타 도매 중개업","463":"음식료품 도매업","4631":"곡물·잡곡 도매업","46311":"곡물 도매업","46313":"육류 도매업","4632":"과실·채소 도매업","46331":"수산물 도매업","46333":"김치류 도매업","46413":"직물 도매업","46420":"의복 도매업","46431":"신발 도매업","46432":"가방 도매업","4644":"화장품·비누 및 세제 도매업","46441":"화장품 도매업","46443":"비누·세제 도매업","46451":"의약품 도매업","46452":"의료용품 도매업","46461":"농약 도매업","46463":"화학비료 도매업","46464":"기타 화학물질 도매업","46492":"판유리 도매업","46493":"철강재 도매업","465":"기계장비 도매업","46510":"컴퓨터 및 주변장치 도매업","4652":"전자·통신장비 도매업","46521":"전자부품 도매업","46522":"통신장비 도매업","4653":"전기용품 도매업","46532":"전기용 기기 도매업","46539":"기타 전기용품 도매업","4659":"기타 기계장비 도매업","46591":"의료용 기기 도매업","46592":"정밀기기 도매업","46593":"사무용 기기 도매업","46595":"건설기계 도매업","46599":"기타 기계장비 도매업","467":"기타 전문 도매업","46711":"연료용 가스 도매업","46712":"액체연료 도매업","46731":"금속광물 도매업","46733":"재생재료 도매업","46739":"기타 산업용 농축산물 도매업","46791":"기타 재생재료 도매업","468":"상품 종합 도매업","46800":"상품 종합 도매업",
    "47":"소매업","471":"종합 소매업","47111":"백화점","47312":"의약품 및 의료용품 소매업","4741":"통신장비 소매업","47430":"컴퓨터 및 주변장치 소매업","47592":"화장품 및 방향제 소매업","4771":"연료 소매업","47711":"주유소 운영업","47841":"안경 소매업","47859":"기타 의료용품 소매업","4791":"통신판매업","47911":"전자상거래 소매업","47912":"기타 통신 판매업","47919":"기타 무점포 소매업","47991":"기타 무점포 소매업",
    "49":"육상운송업","49220":"시내버스 운송업","4930":"화물 자동차 운송업","49301":"일반 화물 자동차 운송업","49309":"기타 화물 자동차 운송업","49401":"송유관 운송업","49500":"화물취급업",
    "50":"수상운송업","50112":"외항 화물 운송업","50130":"내항 화물 운송업","51":"항공운송업","51100":"항공 여객 운송업",
    "52":"창고·운송관련 서비스업","521":"보관 및 창고업","52102":"냉장·냉동 창고업","52104":"위험물품 보관업","529":"기타 운송관련 서비스업","52912":"화물 운송 중개업","52919":"기타 화물 운송관련 서비스업","52921":"항만 하역업","5294":"화물 포장 및 검수업","52941":"화물 포장업","52942":"화물 검수업","5299":"기타 운송관련 서비스업","52992":"통관대리 및 관련 서비스업","52999":"기타 운송관련 서비스업",
    "55":"숙박업","55101":"호텔업","55103":"휴양콘도 운영업","56":"음식점·주점업","5622":"주점업","56221":"일반 주점업",
    "58":"출판업","5811":"서적·잡지 출판업","58112":"잡지 및 정기간행물 출판업","58113":"신문 발행업","582":"소프트웨어 개발 및 공급업","5821":"시스템·응용 소프트웨어 개발 및 공급업","58211":"게임 소프트웨어 개발 및 공급업","58212":"시스템 소프트웨어 개발 및 공급업","58221":"응용 소프트웨어 개발 및 공급업","58222":"시스템 소프트웨어 개발 및 공급업",
    "59":"영상·오디오 제작 및 배급업","5911":"영화·비디오물 제작업","59111":"경기장 운영업","59112":"영화·비디오물 제작업","59113":"영화·비디오물 배급업","59114":"영화관 운영업","59120":"오디오물 출판 및 원판 녹음업","5913":"영화·비디오 후반 작업업","59130":"오디오물 출판 및 원판 녹음업","592":"음악 녹음·출판업",
    "60":"방송업","60210":"지상파 방송업","6022":"프로그램 공급업","60221":"텔레비전 프로그램 제작업","60229":"기타 방송 프로그램 제작업",
    "61":"통신업","612":"전기 통신업","61220":"기타 전기 통신업","61299":"기타 전기 통신업",
    "62":"컴퓨터 프로그래밍·시스템 통합 및 관리업","620":"컴퓨터 프로그래밍·시스템 통합 및 관리업","62010":"컴퓨터 프로그래밍 서비스업","6202":"컴퓨터 시스템 통합 자문 및 구축 서비스업","62021":"컴퓨터 시스템 통합 자문 및 구축 서비스업","62022":"컴퓨터 시설 관리업",
    "63":"정보서비스업","631":"자료처리·호스팅 서비스업","63111":"자료처리업","63112":"호스팅 및 관련 서비스업","63120":"포털 및 기타 인터넷 정보매개 서비스업","63991":"데이터베이스 및 온라인정보 제공업","63999":"기타 정보 서비스업",
    "64":"금융업","64201":"투자신탁 운용업","64209":"기타 투자기관","649":"기타 금융업","64911":"신탁업","64913":"벤처캐피탈 투자업","64919":"기타 투자기관","6499":"기타 금융업","64992":"지주회사",
    "65":"보험·연금업","65110":"생명보험업","65121":"손해보험업","65122":"재보험업",
    "66":"금융·보험 관련 서비스업","66110":"금융시장 관리업","66121":"증권 중개업","66192":"손해사정업","66199":"기타 금융지원 서비스업","66202":"보험 관련 서비스업",
    "68":"부동산업","6811":"부동산 임대업","68111":"주거용 건물 임대업","68112":"비주거용 건물 임대업","6812":"부동산 개발업","68121":"주거용 부동산 개발업","68122":"비주거용 부동산 개발업","68129":"기타 부동산 개발업","68212":"부동산 중개업","68222":"비주거용 부동산 관리업",
    "70":"건축기술·엔지니어링 서비스업","701":"건축기술·엔지니어링 서비스업","70111":"건축설계 서비스업","70113":"엔지니어링 서비스업","70121":"도시계획 및 조경설계 서비스업","70129":"기타 기술 서비스업","70130":"지질조사 및 탐사업","70201":"기술시험·검사 및 분석업",
    "71":"전문 서비스업","7131":"자연과학 연구개발업","71310":"자연과학 연구개발업","715":"전문디자인업","71511":"인테리어 디자인업","71531":"광고 대행업","71600":"사진촬영 및 처리업",
    "72":"사업시설 관리·지원 서비스업","721":"사업시설 관리 및 조경 서비스업","72121":"건물관리업","72122":"산업설비 청소업","72129":"기타 산업설비 청소업","7291":"경비업","72911":"경비업","72919":"기타 보안 서비스업",
    "73":"사업 지원 서비스업","739":"기타 사업 지원 서비스업","7390":"기타 사업 지원 서비스업","73901":"인력 공급업","73909":"기타 사업 지원 서비스업",
    "74":"연구개발업","741":"자연과학 연구개발업","74100":"자연과학 및 공학 연구개발업","74211":"인문사회과학 연구개발업","74220":"인문·사회과학 연구개발업",
    "75":"임대업","751":"산업용 기계장비 임대업","75122":"건설 및 광업용 기계장비 임대업","75210":"운송장비 임대업","7599":"기타 임대업","75993":"기타 산업용 기계장비 임대업","75999":"기타 임대업","76110":"자동차 임대업","76320":"비디오 및 디스크 임대업","76390":"기타 개인용품 임대업","76400":"지식재산권 임대업",
    "84":"공공행정","85":"교육 서비스업","85120":"기술 및 직업훈련학원","8550":"기타 교육기관","85501":"경영 교육기관","85631":"외국어학원","85650":"기타 교육기관","8570":"교육지원 서비스업",
    "86":"보건업","87":"사회복지 서비스업",
    "90":"예술·스포츠·여가 서비스업","90199":"기타 창작 및 예술관련 서비스업","90231":"유원지 및 테마파크 운영업","91111":"경기장 운영업","91191":"기타 스포츠 서비스업",
    "94":"협회·단체","95":"수리업","95212":"통신장비 수리업","96":"기타 개인 서비스업","96991":"기타 개인 서비스업",
}

def induty_name(code):
    if not code: return ""
    c = str(code).strip()
    for length in range(len(c), 1, -1):
        key = c[:length]
        if key in KSIC: return KSIC[key]
    return ""

def dart_co(cc):
    try:
        r = req_lib.get(f"https://opendart.fss.or.kr/api/company.json?crtfc_key={DART_API_KEY}&corp_code={cc}", timeout=10).json()
        if r.get("status") == "000":
            cm = {"Y": "유가증권", "K": "코스닥", "N": "코넥스"}
            ic = r.get("induty_code", "")
            return {"ceo": r.get("ceo_nm", ""), "adres": r.get("adres", ""), "hm_url": r.get("hm_url", ""),
                    "est_dt": r.get("est_dt", ""), "market": cm.get(r.get("corp_cls", ""), "비상장"),
                    "induty": ic, "induty_nm": induty_name(ic),
                    "acc_mt": r.get("acc_mt", ""),
                    "dart_name": (r.get("stock_name") or "").strip()}
    except Exception as e:
        print(f"    ⚠ 기업정보 API 오류 ({cc}): {e}")
    return None

def dart_fin(cc):
    # 계정ID 기반 매핑 (가장 정확)
    ACCT_ID_MAP = {
        "ifrs-full_Assets": "자산총계",
        "ifrs-full_Equity": "자본총계",
        "ifrs-full_Liabilities": "부채총계",
        "ifrs-full_Revenue": "매출액",
        "dart_OperatingIncomeLoss": "영업손익",
        "ifrs-full_ProfitLoss": "당기순손익",
        "dart_TotalSellingGeneralAdministrativeExpenses": None,
    }
    # 계정명 기반 매핑 (폴백)
    NAME_MAP = [
        ("자산총계", "자산총계"),
        ("매출액", "매출액"), ("매출", "매출액"), ("수익(매출액)", "매출액"), ("영업수익", "매출액"),
        ("영업이익", "영업손익"), ("영업이익(손실)", "영업손익"), ("영업손익", "영업손익"),
        ("당기순이익", "당기순손익"), ("당기순이익(손실)", "당기순손익"),
        ("당기순손익", "당기순손익"), ("분기순이익", "당기순손익"),
        ("부채총계", "부채총계"), ("자본총계", "자본총계"),
    ]
    def _parse(r):
        if r.get("status") != "000" or not r.get("list"): return None
        res = {}
        for it in r["list"]:
            acct_id = it.get("account_id", "")
            acct_nm = it.get("account_nm", "").strip()
            val = it.get("thstrm_amount", "")
            if not val or val == "-": continue
            mapped = ACCT_ID_MAP.get(acct_id)
            if mapped and mapped not in res:
                res[mapped] = val; continue
            for pattern, target in NAME_MAP:
                if acct_nm == pattern and target not in res:
                    res[target] = val; break
        return res if res else None

    # 기말 사업보고서(11011)만 사용, 연결(CFS) 우선 → 별도(OFS)
    base_yr = date.today().year
    for yr in [base_yr - 1, base_yr - 2]:
        for fs in ["CFS", "OFS"]:
            try:
                r = req_lib.get(f"https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json?crtfc_key={DART_API_KEY}&corp_code={cc}&bsns_year={yr}&reprt_code=11011&fs_div={fs}", timeout=10).json()
                res = _parse(r)
                if res:
                    res["year"] = str(yr)
                    res["fs_div"] = "연결" if fs == "CFS" else "별도"
                    return res
            except Exception as e:
                print(f"    ⚠ 재무정보 API 오류 ({cc}, {yr}/{fs}): {e}")
            time.sleep(0.05)
    return None

def dart_disc(cc):
    try:
        today = date.today()
        try:
            bgn = today.replace(year=today.year - 3)
        except ValueError:
            bgn = today.replace(year=today.year - 3, day=28)
        r = req_lib.get(f"https://opendart.fss.or.kr/api/list.json?crtfc_key={DART_API_KEY}&corp_code={cc}&bgn_de={bgn.strftime('%Y%m%d')}&end_de={today.strftime('%Y%m%d')}&page_count=5&sort=date&sort_mth=desc", timeout=10).json()
        if r.get("status") == "000" and r.get("list"):
            return [{"date": d.get("rcept_dt", ""), "title": d.get("report_nm", ""),
                     "url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={d.get('rcept_no', '')}"}
                    for d in r["list"]]
    except Exception as e:
        print(f"    ⚠ 공시목록 API 오류 ({cc}): {e}")
    return None

def dart_audit(cc):
    """감사의견 조회 — 최근 2년 사업보고서에서 감사인명/감사의견/핵심감사사항"""
    base_yr = date.today().year
    for yr in [base_yr - 1, base_yr - 2]:
        try:
            r = req_lib.get(f"https://opendart.fss.or.kr/api/accnutAdtorNmNdAdtOpinion.json?crtfc_key={DART_API_KEY}&corp_code={cc}&bsns_year={yr}&reprt_code=11011", timeout=10).json()
            if r.get("status") == "000" and r.get("list"):
                # 당기 행만 추출 (bsns_year에 "당기" 포함)
                items = []
                seen = set()
                for it in r["list"]:
                    by = it.get("bsns_year", "")
                    if "당기" not in by: continue
                    opinion = (it.get("adt_opinion") or "").strip()
                    auditor = (it.get("adtor") or "").strip()
                    kam = (it.get("core_adt_matter") or "").strip()
                    if not opinion or opinion == "-": continue
                    # 같은 감사인+의견 중복 방지 (별도/연결 동일한 경우)
                    key = f"{auditor}|{opinion}"
                    if key in seen: continue
                    seen.add(key)
                    item = {"opinion": opinion, "auditor": auditor}
                    if kam and kam != "-": item["kam"] = kam
                    items.append(item)
                if items:
                    return {"year": str(yr), "items": items}
        except Exception as e:
            print(f"    ⚠ 감사의견 API 오류 ({cc}, {yr}): {e}")
        time.sleep(0.05)
    return None

def dart_audit_fee(cc):
    """감사보수/시간 조회 — 감사용역 체결현황 (당기 첫번째 행)"""
    def _parse_num(s):
        """'7,800' / '945 백만원' / '1,980/년' / '8,375 시간' → 숫자 문자열"""
        if not s or s.strip() == "-": return ""
        n = re.sub(r'[^\d]', '', s)  # 숫자만 추출
        return n if n else ""

    base_yr = date.today().year
    for yr in [base_yr - 1, base_yr - 2]:
        try:
            r = req_lib.get(f"https://opendart.fss.or.kr/api/adtServcCnclsSttus.json?crtfc_key={DART_API_KEY}&corp_code={cc}&bsns_year={yr}&reprt_code=11011", timeout=10).json()
            if r.get("status") == "000" and r.get("list"):
                # 당기 행 우선, 없으면 첫번째 행
                row = None
                for it in r["list"]:
                    by = it.get("bsns_year", "")
                    if "당기" in by:
                        row = it; break
                if not row: row = r["list"][0]

                fee = _parse_num(row.get("adt_cntrct_dtls_mendng", ""))
                hours = _parse_num(row.get("real_exc_dtls_time", "") or row.get("adt_cntrct_dtls_time", ""))
                if fee or hours:
                    result = {"year": str(yr)}
                    if fee: result["fee"] = fee
                    if hours: result["hours"] = hours
                    return result
        except Exception as e:
            print(f"    ⚠ 감사보수 API 오류 ({cc}, {yr}): {e}")
        time.sleep(0.05)
    return None

# ============================================================
# 메인 수집
# ============================================================
def main():
    start = datetime.now()
    print("=" * 60)
    print("  DART 공시정보 업데이트")
    print(f"  {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    if not DART_API_KEY:
        print("\n❌ DART API 키를 .env에 설정하세요.")
        if not IS_CI: input("Enter...")
        return

    # 프로젝트명 가져오기
    proj_names = get_project_names()

    # 기업코드 로드
    print("\nDART 기업코드 로드...")
    corps = load_corp_codes()
    if not corps:
        print("❌ 기업코드 로드 실패")
        if not IS_CI: input("Enter...")
        return

    # 수동 별칭 로드
    aliases = load_aliases()

    # strip 인덱스 빌드 (성능 최적화)
    stripped_idx = _build_stripped_index(corps)
    ci_idx = _build_ci_index(corps)

    # 고객사 추출 & 매핑
    clients = {}
    for pn in proj_names:
        cn = client_name(pn)
        if cn and cn not in ["기타Admin(교육 등)", "New Staff", "Refresh Off", ""]:
            if cn not in clients: clients[cn] = []
            clients[cn].append(pn)
    print(f"\n고유 고객사: {len(clients)}개")

    # 기존 캐시 로드 (있으면)
    cache_file = os.path.join(DART_CACHE_DIR, "dart_details.json")
    old_cache = {}
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            old_cache = json.load(f)

    result = {"projMap": {}, "companies": {}, "updated": _kst_now().strftime("%Y.%m.%d %H:%M:%S")}
    matched = 0
    new_fetched = 0
    unmatched = []
    fuzzy_matches = []

    for cn in sorted(clients):
        # projMap 업데이트
        for pn in clients[cn]:
            result["projMap"][pn] = cn

        corp, match_type = find_corp(cn, corps, stripped_idx, aliases, ci_idx)
        if not corp:
            if match_type != "skip":  # skip은 명시적 제외
                unmatched.append(cn)
            continue
        matched += 1
        cc = corp["cc"]

        # 퍼지/접두사/음차 매칭 로그
        if match_type and ("fuzzy" in match_type or "prefix" in match_type or "translit" in match_type):
            fuzzy_matches.append((cn, match_type))

        # DART API 호출 → 항상 최신으로 갱신
        info = {"stock_code": corp.get("sc", "")}
        ci = dart_co(cc); time.sleep(0.12)
        if ci: info["info"] = ci
        fi = dart_fin(cc); time.sleep(0.12)
        if fi: info["fin"] = fi
        di = dart_disc(cc); time.sleep(0.12)
        if di: info["disc"] = di
        ai = dart_audit(cc); time.sleep(0.12)
        if ai: info["audit"] = ai
        af = dart_audit_fee(cc); time.sleep(0.12)
        if af:
            if "audit" not in info: info["audit"] = {"year": af["year"], "items": []}
            if af.get("fee"): info["audit"]["fee"] = af["fee"]
            if af.get("hours"): info["audit"]["hours"] = af["hours"]
        result["companies"][cn] = info
        new_fetched += 1

        if new_fetched % 20 == 0:
            print(f"  {new_fetched}개 수집 완료...")

    # 캐시 저장
    os.makedirs(DART_CACHE_DIR, exist_ok=True)
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False)

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n{'=' * 60}")
    print(f"  DART 매칭: {matched}/{len(clients)}개 기업")
    print(f"  API 조회: {new_fetched}개")
    if fuzzy_matches:
        print(f"\n  🔍 퍼지 매칭 결과 ({len(fuzzy_matches)}건) — 아래 매칭이 정확한지 확인하세요:")
        for cn, mt in fuzzy_matches:
            print(f"     {cn} → [{mt}]")
        print(f"     ⚠ 잘못된 매칭이 있으면 dart_aliases.json에 올바른 이름을 등록하거나,")
        print(f"       매칭을 무시하려면 \"고객사명\": \"\" 으로 설정하세요.")
    if unmatched:
        print(f"  ⚠ 미매칭: {len(unmatched)}개")
        for u in unmatched:
            print(f"     - {u}")
    if fuzzy_matches or unmatched:
        # 리포트 파일 저장
        um_file = os.path.join(DART_CACHE_DIR, "match_report.txt")
        with open(um_file, "w", encoding="utf-8") as f:
            f.write(f"DART 매칭 리포트 ({_kst_now().strftime('%Y-%m-%d %H:%M')})\n")
            f.write(f"{'=' * 50}\n\n")
            if fuzzy_matches:
                f.write(f"[퍼지 매칭 결과] ({len(fuzzy_matches)}건)\n")
                f.write("아래 매칭이 맞는지 확인하세요. 틀리면 dart_aliases.json에서 수정하세요.\n\n")
                for cn, mt in fuzzy_matches:
                    f.write(f"  {cn} → [{mt}]\n")
                f.write("\n")
            if unmatched:
                f.write(f"[미매칭 고객사] ({len(unmatched)}건)\n")
                f.write("수동 매핑이 필요하면 dart_aliases.json에 추가하세요.\n\n")
                f.write("dart_aliases.json 예시:\n")
                f.write('{\n')
                for i, u in enumerate(unmatched):
                    comma = "," if i < len(unmatched) - 1 else ""
                    f.write(f'    "{u}": "DART에 등록된 정확한 회사명"{comma}\n')
                f.write('}\n\n')
                for u in sorted(unmatched):
                    f.write(f"  - {u}\n")
        print(f"  → 리포트: {um_file}")
        # dart_aliases.json 없으면 빈 템플릿 생성
        if not os.path.exists(DART_ALIASES):
            with open(DART_ALIASES, "w", encoding="utf-8") as f:
                json.dump({}, f, ensure_ascii=False, indent=2)
            print(f"  → 별칭 파일 생성: {DART_ALIASES}")
    else:
        print(f"  ✅ 모든 고객사 매칭 완료!")
    print(f"  캐시 저장: {cache_file}")
    print(f"  소요시간: {elapsed:.1f}초")
    print(f"{'=' * 60}")
    print("\n완료! retain_all.py 실행 시 이 캐시가 자동 적용됩니다.")
    if not IS_CI: input("Enter...")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ {e}")
        import traceback; traceback.print_exc()
        if not IS_CI: input("Enter...")
        sys.exit(1)
