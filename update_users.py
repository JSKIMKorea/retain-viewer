"""
update_users.py — 사용자 목록 갱신 (Azure SQL → 2.사용자관리/users.xlsx)

기업정보포털과 동일한 로그인 시스템을 Global Retain Viewer에 적용하기 위한 스크립트.
Azure SQL의 BI_STAFFREPORT_EMP_V를 조회하여 [azure_auto] 시트만 갱신합니다.
[manual_add] 시트는 수기 관리용으로, 이 스크립트가 절대 변경하지 않습니다.

retain_all.py 실행 시 두 시트를 자동 병합 → bcrypt 해시 → index.html 임베드.
이메일 중복 시 manual_add 우선 (수기 보정값 보존).

매핑:
  PWC_ID    → 이메일 (로그인 ID)
  EMPNO     → 사번 (로그인 PW, bcrypt 해시로 변환되어 저장)
  EMPNM     → 이름
  ORG_NM    → 부서 (CM_NM은 상위 분류 — 너무 광범위하여 ORG_NM 사용)
  EMP_STAT  → 활성 (재직/휴직 = Y, 그 외 = N)

실행:
  python update_users.py
"""
from __future__ import annotations
import os
import sys
from pathlib import Path

import pyodbc
import pandas as pd
from openpyxl.styles import Font

# ────────────────────────────────────────────────────────────
# 환경 감지: GitHub Actions vs 로컬 PC
# ────────────────────────────────────────────────────────────
IS_CI = os.getenv("GITHUB_ACTIONS") == "true"
SCRIPT_DIR = Path(__file__).parent

if not IS_CI:
    from dotenv import load_dotenv
    _ENV_DIR = SCRIPT_DIR / "000.Azure&API"
    for _name in (".env", ".env.txt", "env.txt", "env"):
        _p = _ENV_DIR / _name
        if _p.exists():
            load_dotenv(_p)
            break

USERS_FILE = SCRIPT_DIR / "2.사용자관리" / "users.xlsx"

DB_CONFIG = {
    "server":   os.getenv("AZURE_SQL_SERVER"),
    "database": os.getenv("AZURE_SQL_DATABASE"),
    "username": os.getenv("AZURE_SQL_USERNAME"),
    "password": os.getenv("AZURE_SQL_PASSWORD"),
}

SHEET_AUTO = "azure_auto"
SHEET_MANUAL = "manual_add"
HEADER = ["이메일", "사번", "이름", "부서", "활성"]


def fetch_from_azure() -> pd.DataFrame:
    """BI_STAFFREPORT_EMP_V에서 직원 정보 조회 → 표준 5컬럼 DataFrame"""
    if not DB_CONFIG["server"] or not DB_CONFIG["password"]:
        raise RuntimeError(
            "Azure SQL 접속 정보가 없습니다. "
            "000.Azure&API/.env 파일에 AZURE_SQL_SERVER/USERNAME/PASSWORD 설정 필요."
        )

    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={DB_CONFIG['server']};DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};PWD={DB_CONFIG['password']};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    print("Azure SQL 연결 중...")
    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql("""
            SELECT PWC_ID, EMPNO, EMPNM, CM_NM, ORG_NM, EMP_STAT
            FROM BI_STAFFREPORT_EMP_V
        """, conn).fillna("")
    print(f"  → {len(df):,}명 원본 조회")

    def _dept(row):
        org = str(row.get("ORG_NM", "")).strip()
        cm = str(row.get("CM_NM", "")).strip()
        return org if org else cm

    out = pd.DataFrame({
        "이메일": df["PWC_ID"].astype(str).str.strip().str.lower(),
        "사번":   df["EMPNO"].astype(str).str.strip(),
        "이름":   df["EMPNM"].astype(str).str.strip(),
        "부서":   df.apply(_dept, axis=1),
        "활성":   df["EMP_STAT"].apply(
            lambda x: "Y" if str(x).strip() in ("재직", "휴직") else "N"
        ),
    })

    before = len(out)
    out = out[(out["이메일"] != "") & (out["사번"] != "")]
    if before != len(out):
        print(f"  → 이메일/사번 누락 {before - len(out)}명 제외")

    return out.sort_values(["부서", "이름"]).reset_index(drop=True)


def read_manual_sheet() -> pd.DataFrame:
    """기존 users.xlsx에서 manual_add 시트 읽기. 없으면 빈 DataFrame."""
    if not USERS_FILE.exists():
        return pd.DataFrame(columns=HEADER)
    try:
        df = pd.read_excel(USERS_FILE, sheet_name=SHEET_MANUAL, dtype=str).fillna("")
    except (ValueError, KeyError):
        return pd.DataFrame(columns=HEADER)

    for c in HEADER:
        if c not in df.columns:
            df[c] = ""
    df = df[HEADER]
    df = df[(df["이메일"].str.strip() != "") | (df["사번"].str.strip() != "")]
    return df.reset_index(drop=True)


def write_users_xlsx(auto_df: pd.DataFrame, manual_df: pd.DataFrame) -> None:
    """두 시트를 단일 파일로 저장 (헤더 굵게 + 폭/고정행 적용)"""
    USERS_FILE.parent.mkdir(parents=True, exist_ok=True)

    if manual_df.empty:
        manual_df = pd.DataFrame([
            {"이메일": "example@samil.com", "사번": "999999",
             "이름": "수기추가예시 (활성=Y로 바꾸면 로그인 가능)",
             "부서": "외부협력", "활성": "N"},
        ], columns=HEADER)

    with pd.ExcelWriter(USERS_FILE, engine="openpyxl") as writer:
        auto_df.to_excel(writer, sheet_name=SHEET_AUTO, index=False)
        manual_df.to_excel(writer, sheet_name=SHEET_MANUAL, index=False)

        for sheet_name in (SHEET_AUTO, SHEET_MANUAL):
            ws = writer.sheets[sheet_name]
            for cell in ws[1]:
                cell.font = Font(bold=True)
            ws.column_dimensions["A"].width = 32
            ws.column_dimensions["B"].width = 10
            ws.column_dimensions["C"].width = 14
            ws.column_dimensions["D"].width = 30
            ws.column_dimensions["E"].width = 8
            ws.freeze_panes = "A2"


def run() -> int:
    """retain_all.py에서 호출하기 위한 엔트리. 활성 인원 수 반환 (실패 시 -1)."""
    print("=" * 60)
    print("  사용자 목록 갱신 (Azure SQL → users.xlsx)")
    print("=" * 60)

    try:
        manual_df = read_manual_sheet()
        print(f"\n[manual_add] 기존 보존: {len(manual_df)}명")

        print()
        auto_df = fetch_from_azure()
        n_active = int((auto_df["활성"] == "Y").sum())
        print(f"[azure_auto] 갱신 대상: {len(auto_df)}명 (활성 Y: {n_active}명)")

        write_users_xlsx(auto_df, manual_df)

        print(f"\n저장 완료: {USERS_FILE}")
        print(f"  [azure_auto]  {len(auto_df)}명  (자동 갱신)")
        print(f"  [manual_add]  {len(manual_df)}명  (수기 관리 — 항상 보존)")
        return n_active
    except Exception as e:
        print(f"\n[오류] {e}")
        import traceback; traceback.print_exc()
        return -1


if __name__ == "__main__":
    rc = run()
    if rc < 0:
        sys.exit(1)
    if not IS_CI:
        input("\nEnter...")
