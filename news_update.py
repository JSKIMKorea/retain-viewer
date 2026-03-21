"""
업계 관련 뉴스 수집 + 요약
==============================
1. 네이버 뉴스 검색 API로 카테고리별 기사 수집
2. 기사 본문 크롤링
3. Cerebras API로 요약 생성 (실패 시 제목+URL만)
4. news_cache/daily_news.json 저장

사용법: python news_update.py
"""

import requests as req_lib
import json, os, re, time, urllib3, sys
from datetime import datetime, date, timedelta
from html import unescape

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

# SSL 경고 억제 (회사 네트워크 프록시 대응)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================
# 설정
# ============================================================
NAVER_CLIENT_ID     = os.getenv("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "")
CEREBRAS_API_KEY    = os.getenv("CEREBRAS_API_KEY", "")
NEWS_CACHE_DIR      = os.path.join(_SCRIPT_DIR, "news_cache")
CUSTOM_NEWS_FILE    = os.path.join(NEWS_CACHE_DIR, "custom_news.txt")
MAX_ARTICLES_PER_CAT = 10  # 카테고리당 최대 기사 수 (실제로는 쿼리 수만큼)
MAX_DAYS = 7               # 최근 N일 이내 기사만
AI_MODEL = "llama3.1-8b"
AI_API_URL = "https://api.cerebras.ai/v1/chat/completions"

# ============================================================
# 카테고리별 검색 키워드
# ============================================================
CATEGORIES = [
    {
        "title": "업계동향",
        "icon": "📊",
        "queries": [
            '회계법인 과로 근로감독',
            '공인회계사회 동향',
            '감사보수 외부감사',
            'AI 전문직 대체 일자리',
            '삼일회계법인',
            '삼정회계법인',
            '안진회계법인',
            '한영회계법인',
            'IFRS 18호',
            'ESG 공시 의무화',
        ]
    },
    {
        "title": "회계",
        "icon": "📋",
        "queries": [
            '분식회계 과징금',
            '금감원 회계감리',
            '감사의견 한정 의견거절',
            'K-IFRS 개정',
            '내부회계관리제도',
            'AI 회계 대체 영향',
        ]
    },
    {
        "title": "세무",
        "icon": "🏛️",
        "queries": [
            '국세청 세무조사',
            '세법개정 법인세',
            '양도세 상속세 개정',
            '부가가치세 면세 과세',
            '국제조세 이전가격',
            'AI 세무 대체 영향',
        ]
    },
    {
        "title": "상법개정",
        "icon": "⚖️",
        "queries": [
            '상법개정',
            '상법 개정안',
            '상법 시행령 개정',
        ]
    },
]

# ============================================================
# 수동 기사 주입 (custom_news.txt)
# ============================================================
def load_custom_news():
    """custom_news.txt에서 수동 기사 목록 로드
    형식: 카테고리 | 순서 | URL
    예: 업계동향 | 1 | https://news.example.com/article/12345
    """
    if not os.path.exists(CUSTOM_NEWS_FILE):
        return []
    entries = []
    with open(CUSTOM_NEWS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split("|")]
            if len(parts) != 3:
                print(f"  ⚠ custom_news.txt 형식 오류 (무시): {line}")
                continue
            category, order, url = parts
            try:
                order = int(order)
            except ValueError:
                print(f"  ⚠ 순서가 숫자가 아님 (무시): {line}")
                continue
            entries.append({"category": category, "order": order, "url": url})
    if entries:
        print(f"\n📌 custom_news.txt: {len(entries)}건 수동 기사 발견")
    return entries

def inject_custom_news(result, custom_entries, global_seen_urls):
    """수동 기사를 크롤링+AI요약 후 해당 카테고리의 지정 순서에 삽입"""
    if not custom_entries:
        return 0, 0
    injected = 0
    summarized = 0
    # 카테고리별로 그룹핑
    by_cat = {}
    for entry in custom_entries:
        cat = entry["category"]
        if cat not in by_cat:
            by_cat[cat] = []
        by_cat[cat].append(entry)

    for cat_name, entries in by_cat.items():
        # 결과에서 해당 카테고리 섹션 찾기
        target_section = None
        for sec in result["sections"]:
            if sec["title"] == cat_name:
                target_section = sec
                break
        if not target_section:
            print(f"  ⚠ 카테고리 '{cat_name}'를 찾을 수 없음 → 무시")
            continue

        # 삽입 전 기존 기사 수 기록 (총 개수 유지용)
        original_count = len(target_section["items"])

        # 순서 역순으로 처리 (삽입 시 인덱스 밀림 방지)
        entries.sort(key=lambda x: x["order"], reverse=True)
        cat_injected = 0
        for entry in entries:
            url = entry["url"]
            order = entry["order"]
            if url in global_seen_urls:
                print(f"  ⚠ 중복 URL (무시): {url[:60]}")
                continue
            global_seen_urls.add(url)
            print(f"  📌 [{cat_name} #{order}] {url[:60]}...")

            # 본문 크롤링
            body = fetch_article_body(url)
            time.sleep(0.3)
            if not body or len(body) < 50:
                print(f"       ⚠ 본문 추출 실패 → 무시")
                continue

            # 제목 추출 (og:title 또는 <title>)
            title = ""
            try:
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
                r = req_lib.get(url, headers=headers, timeout=8, verify=False)
                r.encoding = r.apparent_encoding or 'utf-8'
                m = re.search(r'<meta[^>]*property="og:title"[^>]*content="([^"]*)"', r.text, re.IGNORECASE)
                if not m:
                    m = re.search(r'<title[^>]*>([^<]+)</title>', r.text, re.IGNORECASE)
                if m:
                    title = unescape(m.group(1)).strip()
                    title = re.sub(r'\s+', ' ', title)
            except:
                pass
            if not title:
                title = url.split("/")[-1][:50]

            source = extract_source(title, url)

            # AI 요약 (관련성 판단 스킵 — 수동 지정이므로 무조건 요약)
            clean_body = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', body)
            clean_body = re.sub(r'[\u200b\u200c\u200d\ufeff\u00a0]', ' ', clean_body)[:1500]
            gem = ai_summarize(title, clean_body, cat_name)

            article = {
                "title": title,
                "url": url,
                "source": source,
                "summary": "",
                "impact": ""
            }

            if gem and gem.get("summary"):
                article["summary"] = gem["summary"]
                article["impact"] = gem.get("impact", "")
                summarized += 1
                print(f"       ✅ 요약 완료: {title[:40]}...")
            elif gem and gem.get("skip"):
                # 수동 기사는 관련성 낮아도 강제 삽입 (요약 없이)
                print(f"       ⚠ AI가 관련성 낮음 판단했으나 수동 지정이므로 삽입")
            else:
                print(f"       ⚠ AI요약 실패 → 제목+URL만 삽입")
            time.sleep(2)

            # 지정 순서에 삽입 (1-based → 0-based)
            insert_idx = min(order - 1, len(target_section["items"]))
            insert_idx = max(0, insert_idx)
            target_section["items"].insert(insert_idx, article)
            injected += 1
            cat_injected += 1
            print(f"       → '{cat_name}' {order}번째에 삽입 완료")

        # 총 개수 유지: 수동 기사 삽입 후 뒤에서 AI 기사 제거
        if cat_injected > 0 and len(target_section["items"]) > original_count:
            excess = len(target_section["items"]) - original_count
            target_section["items"] = target_section["items"][:original_count]
            print(f"  → '{cat_name}' 총 {original_count}건 유지 (뒤에서 {excess}건 제거)")

    return injected, summarized

# ============================================================
# KICPA 웹진 기사 수집 (우선 소스)
# ============================================================
def fetch_cpanews(max_items=10):
    """한국공인회계사회 기사(webzine.kicpa.or.kr) 최근 기사 수집"""
    articles = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

    # 여러 카테고리 페이지에서 기사 수집
    category_urls = [
        "https://webzine.kicpa.or.kr/category/issue",
        "https://webzine.kicpa.or.kr/category/audit",
        "https://webzine.kicpa.or.kr/category/tax",
        "https://webzine.kicpa.or.kr/category/opinion",
        "https://webzine.kicpa.or.kr/category/sustainability",
        "https://webzine.kicpa.or.kr/",
    ]

    seen = set()
    for cat_url in category_urls:
        try:
            r = req_lib.get(cat_url, headers=headers, timeout=10, verify=False)
            r.encoding = r.apparent_encoding or 'utf-8'
            if r.status_code != 200 or len(r.text) < 500:
                continue
            html = r.text

            # 기사 링크+제목+날짜 추출
            # 패턴: <a href="...">제목</a> 근처에 날짜 (MM-DD HH:MM 또는 YYYY-MM-DD)
            # 다양한 패턴 시도
            link_patterns = [
                r'<a[^>]*href="((?:https?://webzine\.kicpa\.or\.kr)?/[^"]*?(?:article|post|view|volumes)[^"]*)"[^>]*>\s*(.*?)\s*</a>',
                r'<a[^>]*href="(/[^"]{10,})"[^>]*class="[^"]*(?:title|subject|heading)[^"]*"[^>]*>\s*(.*?)\s*</a>',
                r'<h[2-4][^>]*>\s*<a[^>]*href="((?:https?://webzine\.kicpa\.or\.kr)?/[^"]+)"[^>]*>\s*(.*?)\s*</a>',
                r'<a[^>]*href="((?:https?://webzine\.kicpa\.or\.kr)?/[^"]{15,})"[^>]*>((?:(?!<img)[^<])+)</a>',
            ]

            for pat in link_patterns:
                links = re.findall(pat, html, re.DOTALL | re.IGNORECASE)
                for href, raw_title in links:
                    title = re.sub(r'<[^>]+>', '', raw_title).strip()
                    title = re.sub(r'\s+', ' ', title)
                    if not title or len(title) > 200:
                        continue
                    if len(title) < 15 and '[청년회계사' not in title:
                        continue
                        continue
                    if title in ['홈', '목차', '다음', '이전', '로그인', '회원가입', '검색', '더보기']:
                        continue
                    url = href if href.startswith("http") else "https://webzine.kicpa.or.kr" + href
                    # 목록 페이지/카테고리 페이지 제외 (개별 기사만)
                    if "articleList" in url or "category" in url.split("?")[0]:
                        continue
                    if url in seen:
                        continue
                    seen.add(url)

                    # 근처 텍스트에서 날짜 추출 시도
                    pub_date = ""
                    date_ctx = html[max(0, html.find(raw_title[:20])-200):html.find(raw_title[:20])+len(raw_title)+200] if raw_title[:20] in html else ""
                    dm = re.search(r'(\d{2,4}[-./]\d{1,2}[-./]\d{1,2})', date_ctx)
                    if dm:
                        pub_date = dm.group(1)

                    articles.append({
                        "title": title,
                        "url": url,
                        "source": "한국공인회계사회 기사",
                        "desc": "",
                        "pubDate": pub_date,
                    })

            time.sleep(0.3)
        except Exception as e:
            print(f"    ⚠ 한국공인회계사회 기사 {cat_url} 오류: {e}")

    # 중복 제거 후 반환
    unique = []
    titles_seen = set()
    for a in articles:
        tk = re.sub(r'[\s\.\,]+', '', a["title"])[:30]
        if tk not in titles_seen:
            titles_seen.add(tk)
            unique.append(a)
    print(f"  → 한국공인회계사회 기사: {len(unique)}건 수집")
    return unique[:max_items]

def load_cpanews_cache():
    """기존 한국공인회계사회 기사 캐시 로드 (새 기사가 없으면 기존 유지용)"""
    cache_file = os.path.join(NEWS_CACHE_DIR, "구버전", "daily_news.json")
    if not os.path.exists(cache_file):
        return []
    try:
        with open(cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        for sec in data.get("sections", []):
            if sec.get("title") == "한국공인회계사회 기사":
                return sec.get("items", [])
    except:
        pass
    return []

# ============================================================
# 네이버 뉴스 검색
# ============================================================
def search_naver_news(query, display=10):
    """네이버 뉴스 검색 API 호출"""
    url = "https://openapi.naver.com/v1/search/news.json"
    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }
    params = {
        "query": query,
        "display": display,
        "sort": "sim",  # 관련도순 (제목에 키워드 있는 기사 우선)
    }
    try:
        r = req_lib.get(url, headers=headers, params=params, timeout=10)
        if r.status_code == 200:
            items = r.json().get("items", [])
            return items
        else:
            print(f"    ⚠ 네이버 API {r.status_code}: {r.text[:80]}")
    except Exception as e:
        print(f"    ⚠ 네이버 검색 오류: {e}")
    return []

def clean_html(text):
    """HTML 태그 및 엔티티 제거"""
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = re.sub(r'&[a-zA-Z]+;', '', text)
    return text.strip()

def is_recent(pub_date_str):
    """네이버 API pubDate가 최근 MAX_DAYS일 이내인지 확인"""
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(pub_date_str)
        article_date = dt.date()
        cutoff = date.today() - timedelta(days=MAX_DAYS)
        return article_date >= cutoff
    except:
        return True  # 파싱 실패 시 포함 (안전하게)

def collect_category(cat):
    """카테고리 내 쿼리별 기사 수집 → 쿼리 우선순위대로 1건씩 (라운드로빈)"""
    per_query = []  # 쿼리별 기사 리스트 (우선순위 순)
    seen_urls = set()

    for q in cat["queries"]:
        items = search_naver_news(q, display=20)
        print(f"    쿼리 [{q[:25]}...] → 원본 {len(items)}건", end="")
        # 쿼리에서 핵심 키워드 추출 (2글자 이상)
        q_keywords = [w for w in q.split() if len(w) >= 2]
        query_items = []
        for it in items:
            link = it.get("link", "")
            originallink = it.get("originallink", "")
            pub = it.get("pubDate", "")

            # 네이버 호스팅 URL 우선 (n.news.naver.com — 본문 추출 안정적)
            if "news.naver.com" in link:
                url = link
                alt_url = originallink
            elif "news.naver.com" in originallink:
                url = originallink
                alt_url = link
            else:
                url = link
                alt_url = originallink if originallink != link else ""

            if not is_recent(pub):
                continue
            if url in seen_urls:
                continue
            seen_urls.add(url)
            if alt_url:
                seen_urls.add(alt_url)

            title = clean_html(it.get("title", ""))

            # 제목 블랙리스트 (수집 단계에서 제외)
            if any(bl in title for bl in ['[Who Is', '[인사]', '[부고]', '[포토]', '[영상]', '[사진]']):
                continue

            desc = clean_html(it.get("description", ""))
            source = extract_source(title, url)

            # 제목에 쿼리 키워드가 몇 개 포함되는지 점수
            # 띄어쓰기 변형도 매칭 (상법개정 ↔ 상법 개정)
            title_nospace = title.replace(" ", "")
            title_score = 0
            for kw in q_keywords:
                kw_nospace = kw.replace(" ", "")
                if kw in title or kw_nospace in title_nospace:
                    title_score += 1

            query_items.append({
                "title": title, "url": url, "alt_url": alt_url,
                "source": source, "desc": desc, "pubDate": pub,
                "_score": title_score,
            })
        # 제목에 키워드 많이 포함된 기사 우선
        query_items.sort(key=lambda x: x.get("_score", 0), reverse=True)
        per_query.append(query_items)
        print(f" → 필터 후 {len(query_items)}건 (제목매칭 {sum(1 for x in query_items if x['_score']>0)}건)")
        time.sleep(0.1)

    # 쿼리별 후보 리스트를 그대로 반환 (메인 루프에서 본문 성공할 때까지 시도)
    return per_query

def extract_source(title, url):
    """URL에서 언론사명 추출"""
    domain_map = {
        "sejungilbo": "세정일보", "tfmedia": "조세금융신문", "etoday": "이투데이",
        "fnnews": "파이낸셜뉴스", "hankyung": "한국경제", "mk.co.kr": "매일경제",
        "chosun": "조선일보", "donga": "동아일보", "joongang": "중앙일보",
        "hani": "한겨레", "yonhapnews": "연합뉴스", "yna.co.kr": "연합뉴스",
        "sedaily": "서울경제", "mt.co.kr": "머니투데이", "edaily": "이데일리",
        "newsis": "뉴시스", "news1": "뉴스1", "intn.co.kr": "일간NTN",
        "joseilbo": "조세일보", "taxtimes": "세정타임스", "nts.go.kr": "국세청",
        "etnews": "전자신문", "zdnet": "ZDNet", "bloter": "블로터",
        "kacta": "세무사신문",
        "kicpa": "한국공인회계사회 기사", "webzine.kicpa": "한국공인회계사회 기사",
    }
    for key, name in domain_map.items():
        if key in url:
            return name
    # URL에서 도메인 추출
    m = re.search(r'https?://(?:www\.)?([^/]+)', url)
    return m.group(1) if m else ""

# ============================================================
# 기사 본문 크롤링
# ============================================================
def fetch_article_body(url, max_chars=2000):
    """기사 URL에서 본문 텍스트 추출 (최대 max_chars)"""
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
        r = req_lib.get(url, headers=headers, timeout=8, verify=False)
        r.encoding = r.apparent_encoding or 'utf-8'
        html = r.text

        if r.status_code != 200:
            print(f"       [DEBUG] HTTP {r.status_code}: {url[:60]}")
            return ""

        # 불필요 영역 제거 (스크립트, 스타일, 네비게이션 등)
        clean = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL|re.IGNORECASE)
        clean = re.sub(r'<style[^>]*>.*?</style>', '', clean, flags=re.DOTALL|re.IGNORECASE)
        clean = re.sub(r'<nav[^>]*>.*?</nav>', '', clean, flags=re.DOTALL|re.IGNORECASE)
        clean = re.sub(r'<header[^>]*>.*?</header>', '', clean, flags=re.DOTALL|re.IGNORECASE)
        clean = re.sub(r'<footer[^>]*>.*?</footer>', '', clean, flags=re.DOTALL|re.IGNORECASE)
        clean = re.sub(r'<!--.*?-->', '', clean, flags=re.DOTALL)
        # 관련기사/추천기사/사이드바 영역 제거 (본문 오염 방지)
        clean = re.sub(r'<(?:div|section|aside|ul)[^>]*(?:class|id)="[^"]*(?:related|recommend|aside|sidebar|more_news|news_more|hotissue|popular|rank|most_read|also_read|other_news|bottom_article|article_relate|article_bottom|news_list|article_list|associated|tag_area|copyright|reporter_info|byline_info)[^"]*"[^>]*>.*?</(?:div|section|aside|ul)>', '', clean, flags=re.DOTALL|re.IGNORECASE)
        # "관련기사", "관련 뉴스", "추천기사" 헤더 이후 내용 제거
        clean = re.sub(r'<(?:div|h[2-5]|strong|p)[^>]*>[^<]*(?:관련\s*기사|관련\s*뉴스|추천\s*기사|인기\s*기사|많이\s*본|다른\s*기사|포토\s*뉴스|이\s*시각)[^<]*</(?:div|h[2-5]|strong|p)>.*', '', clean, flags=re.DOTALL|re.IGNORECASE)

        body = ""

        # 1단계: 본문 영역 id/class로 큰 블록 추출 (중첩 div 대응: </div> 여러 개까지)
        block_patterns = [
            r'id="article-view-content-div"[^>]*>(.*)',
            r'class="article-view-content"[^>]*>(.*)',
            r'id="dic_area"[^>]*>(.*)',
            r'id="newsct_article"[^>]*>(.*)',
            r'id="articeBody"[^>]*>(.*)',
            r'class="article[_-]?[Bb]ody[^"]*"[^>]*>(.*)',
            r'class="news[_-]?[Cc]ontent[^"]*"[^>]*>(.*)',
            r'class="view[_-]?[Cc]ont[^"]*"[^>]*>(.*)',
            r'class="view[_-]?[Aa]rticle[^"]*"[^>]*>(.*)',
            r'class="cnt_view[^"]*"[^>]*>(.*)',
            r'class="article_txt[^"]*"[^>]*>(.*)',
            r'class="news_text[^"]*"[^>]*>(.*)',
            r'class="detail[_-]?view[^"]*"[^>]*>(.*)',
            r'class="editor[_-]?area[^"]*"[^>]*>(.*)',
            r'class="view-content[^"]*"[^>]*>(.*)',
            r'itemprop="articleBody"[^>]*>(.*)',
        ]
        for pat in block_patterns:
            m = re.search(pat, clean, re.DOTALL | re.IGNORECASE)
            if m:
                block = m.group(1)[:10000]  # 안전하게 10K까지만
                # HTML 태그 제거
                text = re.sub(r'<[^>]+>', ' ', block)
                text = unescape(text)
                text = re.sub(r'\s+', ' ', text).strip()
                if len(text) > 100:
                    body = text
                    break

        # 2단계: <article> 태그 전체
        if not body or len(body) < 100:
            m = re.search(r'<article[^>]*>(.*?)</article>', clean, re.DOTALL|re.IGNORECASE)
            if m:
                text = re.sub(r'<[^>]+>', ' ', m.group(1))
                text = unescape(text)
                text = re.sub(r'\s+', ' ', text).strip()
                if len(text) > 100:
                    body = text

        # 3단계: <p> 태그 모아서 (가장 범용적)
        if not body or len(body) < 100:
            ps = re.findall(r'<p[^>]*>(.*?)</p>', clean, re.DOTALL)
            combined = ' '.join(ps)
            text = re.sub(r'<[^>]+>', ' ', combined)
            text = unescape(text)
            text = re.sub(r'\s+', ' ', text).strip()
            if len(text) > 100:
                body = text

        # 4단계: og:description (최후 수단)
        if not body or len(body) < 100:
            m = re.search(r'<meta[^>]*property="og:description"[^>]*content="([^"]*)"', html, re.IGNORECASE)
            if not m:
                m = re.search(r'<meta[^>]*name="description"[^>]*content="([^"]*)"', html, re.IGNORECASE)
            if m:
                body = unescape(m.group(1))

        if body:
            body = re.sub(r'\s+', ' ', body).strip()
            # 본문 텍스트에서 관련기사/추천기사 이후 내용 제거
            for marker in ['관련기사', '관련 기사', '관련뉴스', '관련 뉴스', '추천기사', '추천 기사',
                           '인기기사', '많이 본 뉴스', '많이 본 기사', '다른 기사', '포토뉴스',
                           '저작권자', 'Copyrights', 'ⓒ', '무단전재 재배포', '기자 = ']:
                idx = body.find(marker)
                if idx > 200:  # 본문 앞부분이 아닌 경우만
                    body = body[:idx].strip()
            # 날씨/헤더 데이터 필터
            if '미세먼지' in body and '°C' in body and len(body) < 300:
                return ""
            if not body or len(body) < 50:
                print(f"       [DEBUG] 본문 너무 짧음 ({len(body)}자): {url[:60]}")
                return ""
            return body[:max_chars]

        print(f"       [DEBUG] 본문 추출 실패: {url[:60]}")
        return ""
    except Exception as e:
        print(f"       [DEBUG] 크롤링 에러 ({e}): {url[:60]}")
        return ""

# ============================================================
# 말투 후처리 (존댓말 → 서술체)
# ============================================================
def fix_tone(text):
    """AI 출력에서 존댓말을 서술체로 변환"""
    if not text:
        return text
    replacements = [
        ('있습니다.', '있음.'), ('됩니다.', '됨.'), ('합니다.', '함.'),
        ('입니다.', '임.'), ('했습니다.', '함.'), ('됐습니다.', '됨.'),
        ('봅니다.', '보임.'), ('습니다.', '음.'), ('겠습니다.', '것으로 보임.'),
        ('있습니다', '있음'), ('됩니다', '됨'), ('합니다', '함'),
        ('입니다', '임'), ('했습니다', '함'), ('됐습니다', '됨'),
        ('봅니다', '보임'), ('습니다', '음'), ('겠습니다', '것으로 보임'),
    ]
    for old, new in replacements:
        text = text.replace(old, new)
    return text

# ============================================================
# 할루시네이션 필터 — 본문에 없는 고유명사가 요약에 나오면 해당 문장 제거
# ============================================================
# 자주 등장하는 기업/기관명 패턴 (2글자 이상 고유명사)
_ENTITY_PATTERN = re.compile(
    r'(?:볼빅|이킴|고려아연|셀트리온|삼성|현대|SK|LG|한화|롯데|포스코|카카오|네이버|'
    r'쿠팡|두산|효성|한진|대한항공|아시아나|CJ|GS|OCI|HLB|영풍|MBK|KT|NH|KB|'
    r'신한|하나|우리|IBK|DB|메리츠|동양생명|한국금융지주|미래에셋|삼일|삼정|안진|한영|딜로이트|'
    r'금감원|금융위|국세청|대법원|조세심판원|감리위)'
)

def _remove_hallucinated_sentences(text, body):
    """요약 텍스트에서 본문에 없는 고유명사를 포함한 문장을 제거"""
    if not text or not body:
        return text
    # 문장 분리 (마침표 기준)
    sentences = re.split(r'(?<=\.)\s+', text)
    if len(sentences) <= 1:
        return text
    filtered = []
    for sent in sentences:
        entities_in_sent = _ENTITY_PATTERN.findall(sent)
        if not entities_in_sent:
            filtered.append(sent)
            continue
        # 이 문장의 고유명사가 본문에 있는지 확인
        all_in_body = all(ent in body for ent in entities_in_sent)
        if all_in_body:
            filtered.append(sent)
        else:
            missing = [e for e in entities_in_sent if e not in body]
            # 디버그용 (필요시 활성화)
            # print(f"       🔍 할루시네이션 감지: {missing} → 문장 제거")
            pass
    result = ' '.join(filtered).strip()
    return result if result else text  # 전부 제거되면 원본 유지

# ============================================================
# AI 요약 (Cerebras)
# ============================================================
def ai_summarize(title, body, category):
    """Cerebras API로 기사 요약 + 업계영향 + 관련성 판단"""
    if not CEREBRAS_API_KEY:
        return None

    # 본문 정리 (특수문자/제어문자 제거 — API 500 에러 방지)
    clean_body = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', body)
    clean_body = re.sub(r'[\u200b\u200c\u200d\ufeff\u00a0]', ' ', clean_body)  # zero-width/NBSP
    clean_body = re.sub(r'\s+', ' ', clean_body).strip()[:1500]
    clean_title = re.sub(r'[\x00-\x1f\x7f-\x9f\u200b\ufeff]', '', title)

    prompt = f"""당신은 Big4 회계법인 10년차 공인회계사임.

아래 기사를 분석해주세요.

절대 금지사항:
- 본문에 없는 수치, 통계, 순위를 만들어내지 말 것
- 본문의 수치를 다른 대상에 붙이지 말 것 (예: A가 74.5%인데 B가 74.5%라고 쓰면 안 됨)
- 기사 원문에 있는 사실만 요약할 것. 추측이나 변형 금지

[제목] {clean_title}
[본문] {clean_body}

1) 이 기사가 회계사 업무에 관련되는지 판단하세요.
   - 관련 높음 (아래 중 하나라도 해당하면 높음):
     · 회계기준(K-IFRS, 일반기업회계기준) 개정/적용/해석
     · 분식회계, 회계부정, 과징금, 감리지적
     · 감사의견(적정/한정/의견거절/계속기업 불확실성)
     · 내부회계관리제도, 외부감사제도
     · 금감원/금융위 회계감리, 조치, 제재
     · ESG공시, 지속가능성보고 의무화
     · 세법개정, 조세판례, 예규, 경정청구
     · 회계법인 동향, 감사보수, 회계사 인력/노동 이슈
     · 회계사 근무환경, 노동시간, 청년회계사 이슈
     · 재무 데이터분석, 빅데이터, 회계 디지털전환
     · AI·인공지능 활용(감사, 세무신고, 회계처리, 내부통제 자동화 등)
     · 기업지배구조(감사위원회, 사외이사, 주주제안)
     · 상법 개정(이사 충실의무, 감사위원 분리선임, 소수주주권, 집중투표제)
     · 지방자치단체/공공기관 외부 회계감사 도입
   - 관련 낮음: 단순 기업 실적발표, 주가 변동, 부동산 시세, 지역 축제, 연예/스포츠

2) 관련 높음이면 아래 형식으로 답변하세요.
   [요약 작성 규칙]
   - 기사 본문에 실제로 적힌 내용만 요약. 수치/순위는 본문 그대로 인용, 변형 금지
   - 2~3문장, 구체적 수치/제도명/법령명 포함
   - 문체: "~임.", "~됨.", "~함.", "~있음.", "~것으로 보임."
   - 금지: "~합니다", "~입니다", "~됩니다"
   [영향 작성 규칙]
   - 핵심만 1문장(30자 이내). "~예상됨", "~참고 필요" 등으로 끝낼 것
   - "회계사들은", "회계법인의" 서두 금지

반드시 아래 형식으로만 답변하세요:
관련성: 높음 또는 낮음
핵심요약: (여기에 요약)
업계영향: (여기에 영향)"""

    for attempt in range(3):
        try:
            r = req_lib.post(
                AI_API_URL,
                headers={
                    "Authorization": f"Bearer {CEREBRAS_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": AI_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 400,
                    "temperature": 0.3
                },
                timeout=15,
                verify=False
            )
            if r.status_code == 200:
                try:
                    data = r.json()
                except:
                    preview = r.text[:200].strip()
                    if "<html" in preview.lower():
                        print(f"       ⚠ 프록시 차단 감지")
                        return None
                    print(f"       ⏳ JSON 파싱 실패 — {(attempt+1)*3}초 대기...")
                    time.sleep((attempt + 1) * 3)
                    continue
                text = data["choices"][0]["message"]["content"]
                relevance = ""
                summary = ""
                impact = ""
                for line in text.strip().split("\n"):
                    line = line.strip()
                    # 유연한 파싱: 앞의 마크다운/번호/기호 제거 후 매칭
                    clean_line = re.sub(r'^[\s\-\*\#\d\.\)]+', '', line).strip()
                    clean_line = re.sub(r'\*+', '', clean_line).strip()
                    if re.match(r'관\s*련\s*성\s*[:：]', clean_line):
                        relevance = re.sub(r'관\s*련\s*성\s*[:：]\s*', '', clean_line).strip()
                    elif re.match(r'핵\s*심\s*요\s*약\s*[:：]', clean_line):
                        summary = re.sub(r'핵\s*심\s*요\s*약\s*[:：]\s*', '', clean_line).strip()
                    elif re.match(r'업\s*계\s*영\s*향\s*[:：]', clean_line):
                        impact = re.sub(r'업\s*계\s*영\s*향\s*[:：]\s*', '', clean_line).strip()
                # 파싱 실패 시 — 전체 텍스트에서 패턴 추출 시도
                if not summary:
                    m = re.search(r'핵\s*심\s*요\s*약\s*[:：]\s*(.+?)(?=업\s*계\s*영\s*향|$)', text, re.DOTALL)
                    if m:
                        summary = m.group(1).strip().split('\n')[0].strip()
                if not impact:
                    m = re.search(r'업\s*계\s*영\s*향\s*[:：]\s*(.+?)$', text, re.DOTALL)
                    if m:
                        impact = m.group(1).strip().split('\n')[0].strip()
                if not relevance:
                    m = re.search(r'관\s*련\s*성\s*[:：]\s*(.+?)(?=핵\s*심|$)', text, re.DOTALL)
                    if m:
                        relevance = m.group(1).strip().split('\n')[0].strip()
                if "낮" in relevance:
                    return {"skip": True}
                # 후처리: 존댓말 → 서술체 강제 변환
                summary = fix_tone(summary)
                impact = fix_tone(impact)
                # 지시문이 요약에 섞인 경우 필터
                instruction_markers = ['본문에 실제로', '수치/순위', '변형하지 말', '작성할 것', '인용하고', '오류로 간주']
                if summary and any(m in summary for m in instruction_markers):
                    summary = ""
                # 할루시네이션 필터: 본문에 없는 고유명사가 요약에 있으면 해당 문장 제거
                if summary:
                    summary = _remove_hallucinated_sentences(summary, clean_body)
                    impact = _remove_hallucinated_sentences(impact, clean_body)
                if summary:
                    return {"summary": summary, "impact": impact}
                return None
            elif r.status_code == 429 or r.status_code == 500:
                wait = (attempt + 1) * 3
                print(f"       ⏳ API {r.status_code} — {wait}초 대기 후 재시도...")
                time.sleep(wait)
                continue
            else:
                print(f"    ⚠ AI API {r.status_code}: {r.text[:100]}")
                return None
        except Exception as e:
            print(f"    ⚠ AI 오류: {e}")
            return None
    print(f"    ⚠ AI 재시도 실패")
    return None
def clean_desc_fallback(desc):
    """description에서 마지막 완전한 문장까지만 추출"""
    if not desc:
        return ""
    # 문장 종결 패턴으로 분할
    sentences = re.split(r'(?<=[다요함됨음임죠])[.\s]|(?<=[.!?])\s', desc)
    result = ""
    for s in sentences:
        s = s.strip()
        if not s:
            continue
        candidate = (result + " " + s).strip() if result else s
        if len(candidate) > 120:
            break
        result = candidate
    return result if len(result) > 20 else desc[:100]

# ============================================================
# 메인
# ============================================================
def main():
    start = datetime.now()
    print("=" * 60)
    print("  업계 관련 뉴스 수집")
    print(f"  {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    result = {
        "date": date.today().isoformat(),
        "updated": datetime.now().strftime("%Y.%m.%d %H:%M"),
        "sections": []
    }

    total_articles = 0
    total_summarized = 0
    total_skipped = 0
    global_seen_urls = set()  # 카테고리 간 중복 제거

    # ── 한국공인회계사회 기사 (첫 번째 섹션: 스크래핑 기반) ──
    print("\n📰 한국공인회계사회 기사 수집...")
    cpanews_raw = fetch_cpanews(max_items=15)
    cpanews_old = load_cpanews_cache()

    cpanews_section = {
        "title": "한국공인회계사회 기사",
        "icon": "📰",
        "items": []
    }

    # 새 기사 처리 (AI 요약 시도)
    cpa_count = 0
    for item in cpanews_raw:
        if cpa_count >= 5:
            break
        url = item["url"]
        if url in global_seen_urls:
            continue
        global_seen_urls.add(url)

        title = item["title"]
        source = item["source"]
        print(f"  [{cpa_count+1}] {title[:45]}...")

        # 목록 페이지 URL은 건너뜀 (개별 기사가 아님)
        if "articleList.html" in url:
            continue

        body = fetch_article_body(url)
        time.sleep(0.3)

        article = {"title": title, "url": url, "source": source, "summary": "", "impact": ""}

        # KICPA 기사 중 무조건 포함 키워드
        KICPA_ALWAYS_INCLUDE = ['[청년회계사 포커스]', '[재무빅데이터분석사']
        force_include = any(kw in title for kw in KICPA_ALWAYS_INCLUDE)

        ai_text = body if body and len(body) > 100 else ""
        if not ai_text and item.get("desc") and len(item["desc"]) > 50:
            ai_text = item["desc"]

        if ai_text:
            gem = ai_summarize(title, ai_text, "한국공인회계사회 기사")
            if gem and gem.get("skip") and not force_include:
                print(f"       ⏭️ 관련성 낮음 → 건너뜀")
                total_skipped += 1
                continue
            elif gem and gem.get("skip") and force_include:
                if item.get("desc") and len(item["desc"]) > 30:
                    article["summary"] = clean_desc_fallback(item["desc"])
                elif body:
                    article["summary"] = body[:120].strip()
                print(f"       📌 키워드 강제 포함")
            elif gem and gem.get("summary"):
                article["summary"] = gem["summary"]
                article["impact"] = gem.get("impact", "")
                total_summarized += 1
                print(f"       ✅ 요약 완료")
            else:
                # AI 실패 → 1회 재시도
                print(f"       ↻ AI 재시도...")
                time.sleep(3)
                gem = ai_summarize(title, ai_text, "한국공인회계사회 기사")
                if gem and gem.get("summary"):
                    article["summary"] = gem["summary"]
                    article["impact"] = gem.get("impact", "")
                    total_summarized += 1
                    print(f"       ✅ 요약 완료 (재시도)")
                else:
                    # 재시도도 실패 → 원문 넣지 않고 건너뜀
                    print(f"       ⏭️ AI 요약 실패 → 건너뜀")
                    continue
            time.sleep(2)
        else:
            print(f"       ⚠ 본문/description 모두 부족")
            continue

        cpanews_section["items"].append(article)
        cpa_count += 1
        total_articles += 1

    # 새 기사가 5개 미만이면 기존 캐시에서 채움
    if len(cpanews_section["items"]) < MAX_ARTICLES_PER_CAT and cpanews_old:
        existing_urls = {a["url"] for a in cpanews_section["items"]}
        for old_item in cpanews_old:
            if len(cpanews_section["items"]) >= MAX_ARTICLES_PER_CAT:
                break
            if old_item["url"] not in existing_urls:
                # 캐시 기사에 요약이 없으면 AI 요약 시도
                if not old_item.get("summary"):
                    print(f"  [캐시+AI] {old_item['title'][:40]}...")
                    body = fetch_article_body(old_item["url"])
                    if body and len(body) > 100:
                        clean_b = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', body)
                        clean_b = re.sub(r'[\u200b\u200c\u200d\ufeff\u00a0]', ' ', clean_b)[:1500]
                        gem = ai_summarize(old_item["title"], clean_b, "한국공인회계사회 기사")
                        if gem and gem.get("summary"):
                            old_item["summary"] = gem["summary"]
                            old_item["impact"] = gem.get("impact", "")
                            total_summarized += 1
                            print(f"       ✅ 요약 완료")
                            time.sleep(2)
                        else:
                            print(f"       ⚠ AI요약 실패")
                else:
                    print(f"  [캐시] {old_item['title'][:40]}...")
                cpanews_section["items"].append(old_item)
                global_seen_urls.add(old_item["url"])

    result["sections"].append(cpanews_section)
    print(f"  → 한국공인회계사회 기사 최종 {len(cpanews_section['items'])}건")

    # ── 나머지 카테고리 (네이버 검색 기반) ──
    for cat in CATEGORIES:
        print(f"\n{cat['icon']} {cat['title']} 수집 중...")
        per_query = collect_category(cat)
        total_candidates = sum(len(q) for q in per_query)
        print(f"  → 후보 {total_candidates}건 ({len(per_query)}개 쿼리)")

        section = {
            "title": cat["title"],
            "icon": cat["icon"],
            "items": []
        }

        TITLE_BLACKLIST = ['[Who Is', '[인사]', '[부고]', '[포토]', '[영상]', '[사진]']

        # 쿼리별로 1건씩 성공할 때까지 시도
        for qi_idx, qi in enumerate(per_query):
            found = False
            # 1차: 제목에 키워드가 포함된 기사(score>0)만 시도
            # 2차: score=0도 시도
            candidates = [item for item in qi if item.get("_score", 0) > 0]
            for item in candidates:
                url = item["url"]
                if url in global_seen_urls:
                    continue

                title = item["title"]
                if any(bl in title for bl in TITLE_BLACKLIST):
                    continue

                global_seen_urls.add(url)
                source = item["source"]
                print(f"  [{qi_idx+1}] {title[:50]}...")

                # 본문 크롤링 (1차: 기본 URL)
                body = fetch_article_body(url)
                # 1차 실패 시 대체 URL로 재시도
                if (not body or len(body) < 100) and item.get("alt_url"):
                    alt = item["alt_url"]
                    print(f"       ↻ 대체 URL 시도: {alt[:50]}...")
                    body = fetch_article_body(alt)
                    if body and len(body) >= 100:
                        url = alt

                # 2차 실패 시 — 핵심 키워드로 네이버 재검색해서 다른 언론사 기사 찾기
                if not body or len(body) < 100:
                    # 제목에서 핵심 키워드만 추출 (짧게!)
                    title_words = re.sub(r'[^가-힣a-zA-Z0-9\s]', ' ', title).split()
                    title_keywords = ' '.join([w for w in title_words if len(w) >= 2][:3])  # 핵심 2글자+ 단어 3개만
                    if title_keywords:
                        print(f"       ↻ 제목 재검색: [{title_keywords}]...")
                        retry_items = search_naver_news(title_keywords, display=10)
                        # 네이버 호스팅 URL을 먼저 시도
                        naver_first = []
                        others = []
                        for ri in retry_items:
                            ri_link = ri.get("link", "")
                            ri_orig = ri.get("originallink", "")
                            if "news.naver.com" in ri_link:
                                naver_first.append({"url": ri_link})
                            elif "news.naver.com" in ri_orig:
                                naver_first.append({"url": ri_orig})
                            else:
                                others.append({"url": ri_link})
                        for ri in naver_first + others:
                            ri_url = ri["url"]
                            if ri_url in global_seen_urls or ri_url == url:
                                continue
                            body = fetch_article_body(ri_url)
                            if body and len(body) >= 100:
                                url = ri_url
                                global_seen_urls.add(ri_url)
                                print(f"       ✓ 대체 기사 발견: {ri_url[:60]}...")
                                break

                time.sleep(0.3)

                if not body or len(body) < 100:
                    print(f"       ⏭️ 본문 추출 실패 → 같은 주제 다음 기사 시도")
                    continue

                # AI 요약
                gem = ai_summarize(title, body, cat["title"])
                if gem and gem.get("skip"):
                    print(f"       ⏭️ 관련성 낮음 → 같은 주제 다음 기사 시도")
                    total_skipped += 1
                    continue
                elif gem and gem.get("summary"):
                    article = {
                        "title": title, "url": url, "source": source,
                        "summary": gem["summary"], "impact": gem.get("impact", "")
                    }
                    total_summarized += 1
                    total_articles += 1
                    section["items"].append(article)
                    print(f"       ✅ 요약 완료")
                    found = True
                    time.sleep(2)
                    break  # 이 쿼리 성공 → 다음 쿼리로
                else:
                    print(f"       ⏭️ AI요약 실패 → 같은 주제 다음 기사 시도")
                    time.sleep(2)
                    continue

            if not found:
                print(f"  [{qi_idx+1}] ⚠ 이 주제에서 적합한 기사 없음")

        # 5건 미달 시 모든 쿼리에서 제목매칭 기사 추가 채움
        MIN_ARTICLES = 5
        if len(section["items"]) < MIN_ARTICLES and per_query:
            for qi in per_query:
                if len(section["items"]) >= MIN_ARTICLES:
                    break
                fill_candidates = [item for item in qi if item.get("_score", 0) > 0]
                for item in fill_candidates:
                    if len(section["items"]) >= MIN_ARTICLES:
                        break
                    url = item["url"]
                    if url in global_seen_urls:
                        continue
                    title = item["title"]
                    if any(bl in title for bl in TITLE_BLACKLIST):
                        continue
                    global_seen_urls.add(url)
                    print(f"  [추가] {title[:50]}...")
                    body = fetch_article_body(url)
                    time.sleep(0.3)
                    if not body or len(body) < 100:
                        continue
                    gem = ai_summarize(title, body, cat["title"])
                    if gem and gem.get("skip"):
                        total_skipped += 1
                        continue
                    elif gem and gem.get("summary"):
                        article = {
                            "title": title, "url": url, "source": item["source"],
                            "summary": gem["summary"], "impact": gem.get("impact", "")
                        }
                        total_summarized += 1
                        total_articles += 1
                        section["items"].append(article)
                        print(f"       ✅ 요약 완료")
                        time.sleep(2)

        result["sections"].append(section)

    # ── 수동 기사 주입 (custom_news.txt) ──
    custom_entries = load_custom_news()
    if custom_entries:
        custom_injected, custom_summarized = inject_custom_news(result, custom_entries, global_seen_urls)
        total_articles += custom_injected
        total_summarized += custom_summarized

    # 캐시 저장
    os.makedirs(NEWS_CACHE_DIR, exist_ok=True)
    cache_file = os.path.join(NEWS_CACHE_DIR, "daily_news.json")
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n{'=' * 60}")
    print(f"  수집 완료: {total_articles}건 (AI요약 {total_summarized}건, 관련성낮음 {total_skipped}건 제외)")
    print(f"  캐시: {cache_file}")
    print(f"  소요시간: {elapsed:.1f}초")
    print(f"{'=' * 60}")
    if not IS_CI: input("\nEnter...")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ {e}")
        import traceback; traceback.print_exc()
        if not IS_CI: input("Enter...")
