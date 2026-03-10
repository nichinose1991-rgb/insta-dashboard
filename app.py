#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
インスタ活動分析ダッシュボード

Google スプレッドシートの2タブ（フォロー活動・約束一覧）を読み込み、
リスト別成約率・たねまきをしている人 別パフォーマンス・異常値を可視化する。
"""

import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────────
#  ページ設定
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="インスタ活動分析",
    page_icon="📊",
    layout="wide",
)

# ─────────────────────────────────────────────
#  定数
# ─────────────────────────────────────────────
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
DAILY_START_COL = 10   # K列（0インデックス）
COLS_PER_DAY = 4       # 対象 / Fw / DM / 約束
EXCLUDE_LIST_NAMES = {"対象アカ"}


# ─────────────────────────────────────────────
#  メール送信
# ─────────────────────────────────────────────
def build_anomaly_email(low_dm: pd.DataFrame, low_appt: pd.DataFrame,
                         avg_follow_dm: float, avg_dm_appt: float) -> str:
    import datetime
    today = datetime.date.today().strftime("%Y年%m月%d日")

    def df_to_html_by_dept(df: pd.DataFrame, cols: list) -> str:
        if df.empty:
            return "<p>該当者なし</p>"
        html = ""
        for dept, group in df[cols].groupby("本部"):
            html += f"<h4>{dept}</h4>"
            html += group.drop(columns=["本部"]).to_html(index=False, border=1)
        return html

    low_dm_html = df_to_html_by_dept(
        low_dm, ["本部", "担当者名", "アカウント名", "フォロー累計", "DM累計", "フォロー→DM率(%)"]
    )
    low_appt_html = df_to_html_by_dept(
        low_appt, ["本部", "担当者名", "アカウント名", "DM累計", "約束累計", "DM→約束率(%)"]
    )

    return f"""
<html><body>
<h2>インスタ活動 異常値レポート</h2>
<p>集計日: {today}</p>
<p>
  全体平均 &nbsp; フォロー→DM率: <b>{avg_follow_dm:.1f}%</b> &nbsp;／&nbsp;
  DM→約束率: <b>{avg_dm_appt:.1f}%</b>
</p>
<hr>
<h3>フォローしているがDMが少ない人（平均を下回る）</h3>
{low_dm_html}
<hr>
<h3>DMしているが約束が少ない人（平均を下回る）</h3>
{low_appt_html}
</body></html>
"""


def send_anomaly_email(html_body: str) -> str:
    cfg = st.secrets.get("email", {})
    smtp_host  = cfg.get("smtp_host", "")
    smtp_port  = int(cfg.get("smtp_port", 587))
    sender     = cfg.get("sender", "")
    password   = cfg.get("password", "")
    recipients = cfg.get("recipients", [])

    if not all([smtp_host, sender, password, recipients]):
        return "メール設定が不完全です（secrets.toml の [email] を確認してください）"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "インスタ活動 異常値レポート"
    msg["From"]    = sender
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, recipients, msg.as_string())
        return ""
    except Exception as e:
        return str(e)


# ─────────────────────────────────────────────
#  Google Sheets 接続
# ─────────────────────────────────────────────
@st.cache_resource
def get_client():
    """読み取り用クライアント（キャッシュあり）"""
    creds = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    return gspread.Client(auth=creds)


def get_write_client():
    """書き込み用クライアント（キャッシュなし・毎回新規作成）"""
    creds = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.Client(auth=creds)


@st.cache_data(ttl=300)
def load_sheet(spreadsheet_id: str, sheet_name: str) -> list[list]:
    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    titles = {w.title.strip(): w for w in sh.worksheets()}
    ws = titles.get(sheet_name.strip())
    if ws is None:
        raise gspread.exceptions.WorksheetNotFound(sheet_name)
    return ws.get_all_values()


# ─────────────────────────────────────────────
#  パーサー
# ─────────────────────────────────────────────
def _safe_int(v) -> int:
    try:
        return int(str(v).replace(",", "").strip())
    except Exception:
        return 0


def extract_instagram_id(url: str) -> str:
    """Instagram URL からアカウントID（小文字）を抽出"""
    url = url.strip().rstrip("/")
    m = re.search(r"instagram\.com/([^/?#]+)", url)
    return m.group(1).lower() if m else ""


def _alnum_prefix(s: str) -> str:
    """先頭の連続する半角英数字部分を返す"""
    m = re.match(r'^[a-z0-9]+', s.lower())
    return m.group(0) if m else ""


def lookup_list_name(url: str, account_map: dict) -> str:
    """
    Instagram URL をアカウントリストと照合してリスト名を返す。
    完全一致を優先し、なければ先頭英数字3文字以上の前方一致で検索。
    """
    url_id = extract_instagram_id(url)
    if not url_id:
        return ""
    # 完全一致
    if url_id in account_map:
        return account_map[url_id]
    # 前方一致（先頭英数字3文字以上）
    url_prefix = _alnum_prefix(url_id)
    if len(url_prefix) < 3:
        return ""
    for map_id, list_name in account_map.items():
        map_prefix = _alnum_prefix(map_id)
        if len(map_prefix) < 3:
            continue
        n = min(len(url_prefix), len(map_prefix))
        if url_prefix[:n] == map_prefix[:n]:
            return list_name
    return ""


def _rows_to_account_map(rows: list) -> dict:
    """行リスト（A列=アカウントID、F列=リスト名）をアカウントマップに変換"""
    result = {}
    for row in rows:
        if len(row) >= 6:
            account_id = row[0].strip().lower()
            list_name  = row[5].strip()
            if (account_id and list_name
                    and list_name not in EXCLUDE_LIST_NAMES
                    and re.match(r'^[a-zA-Z0-9\s\-_.]+$', list_name)):
                result[account_id] = list_name
    return result


@st.cache_data(ttl=300)
def load_account_map(sheet_id: str) -> dict:
    """スプレッドシート2の1枚目シートを読み込み A列→F列 の辞書を返す"""
    gc = get_client()
    sh = gc.open_by_key(sheet_id)
    rows = sh.sheet1.get_all_values()
    return _rows_to_account_map(rows)


def load_account_map_from_csv(uploaded_file) -> dict:
    """アップロードされたCSVファイルから A列→F列 のアカウントマップを作成"""
    df = pd.read_csv(uploaded_file, header=None, dtype=str).fillna("")
    rows = df.values.tolist()
    return _rows_to_account_map(rows)


def fill_empty_ad_column(spreadsheet_id: str, yakusoku_sheet_name: str,
                          yakusoku_rows: list, account_map: dict) -> tuple[int, int]:
    """AD列が空欄の行のみリスト名を書き込む。(書き込み数, スキップ数)を返す"""
    gc = get_write_client()
    sh = gc.open_by_key(spreadsheet_id)
    titles = {w.title.strip(): w for w in sh.worksheets()}
    ws = titles.get(yakusoku_sheet_name.strip())
    if ws is None:
        raise ValueError(f"タブ '{yakusoku_sheet_name}' が見つかりません。")

    updates = []
    written = 0
    skipped = 0
    for i, row in enumerate(yakusoku_rows[1:], start=2):
        current_ad = row[29].strip() if len(row) > 29 else ""
        if current_ad:          # すでに値があればスキップ
            skipped += 1
            continue
        url = row[10].strip() if len(row) > 10 else ""
        if not url:
            continue
        list_name = lookup_list_name(url, account_map)
        if list_name:
            updates.append({"range": f"AD{i}", "values": [[list_name]]})
            written += 1

    if updates:
        ws.batch_update(updates)
    return written, skipped


def parse_follow_sheet(rows: list[list]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if len(rows) < 3:
        return pd.DataFrame(), pd.DataFrame()

    date_row = rows[0]
    days: list[tuple[int, int]] = []
    col = DAILY_START_COL
    while col < len(date_row):
        cell = date_row[col] if col < len(date_row) else ""
        m = re.search(r"(\d+)日", cell)
        if m:
            days.append((int(m.group(1)), col))
        col += COLS_PER_DAY

    emp_records = []
    daily_records = []
    last_name = ""

    for row in rows[2:]:
        if not row or len(row) < 2:
            continue
        name = row[1].strip() if len(row) > 1 else ""
        if name and name != "名前":
            if re.search(r'[0-9]', name):
                continue
            last_name = name
        elif last_name:
            name = last_name
        else:
            continue

        dept         = row[2].strip() if len(row) > 2 else ""
        account      = row[3].strip() if len(row) > 3 else ""
        follow_total = _safe_int(row[4]) if len(row) > 4 else 0
        dm_total     = _safe_int(row[6]) if len(row) > 6 else 0
        appt_total   = _safe_int(row[7]) if len(row) > 7 else 0

        if follow_total == 0 and dm_total == 0 and appt_total == 0:
            continue

        emp_records.append({
            "担当者名": name,
            "アカウント名": account,
            "本部": dept,
            "フォロー累計": follow_total,
            "DM累計": dm_total,
            "約束累計": appt_total,
        })

        for day_num, cs in days:
            target = row[cs].strip()       if cs     < len(row) else ""
            follow = _safe_int(row[cs + 1]) if cs + 1 < len(row) else 0
            dm     = _safe_int(row[cs + 2]) if cs + 2 < len(row) else 0
            appt   = _safe_int(row[cs + 3]) if cs + 3 < len(row) else 0
            daily_records.append({
                "担当者名": name, "本部": dept, "日": day_num,
                "対象": target, "フォロー数": follow, "DM数": dm, "約束数": appt,
            })

    return pd.DataFrame(emp_records), pd.DataFrame(daily_records)


def parse_yakusoku_sheet(rows: list[list]) -> pd.DataFrame:
    """約束タブをパース。K列のInstagram URLを保持する。"""
    records = []
    for row in rows[1:]:
        if len(row) < 8:
            continue
        employee = row[7].strip() if len(row) > 7 else ""
        if not employee:
            continue

        step_up   = bool(row[14].strip()) if len(row) > 14 else False
        contract  = bool(row[26].strip()) if len(row) > 26 else False
        insta_url = row[10].strip()       if len(row) > 10 else ""

        records.append({
            "月": _safe_int(row[1]),
            "日": _safe_int(row[2]),
            "担当者名": employee,
            "Instagram URL": insta_url,
            "ステップアップ": step_up,
            "成約": contract,
        })

    return pd.DataFrame(records)


def build_list_analysis(yakusoku_df: pd.DataFrame, account_map: dict) -> pd.DataFrame:
    """K列URLとアカウントリストを照合してリスト名を付与（シートへの書き込みなし）"""
    if yakusoku_df.empty or account_map is None:
        return pd.DataFrame()

    df = yakusoku_df.copy()
    df["対象リスト"] = df["Instagram URL"].apply(
        lambda url: lookup_list_name(url, account_map) if url else ""
    )
    df = df[df["対象リスト"] != ""]
    if df.empty:
        return pd.DataFrame()
    return df[["担当者名", "月", "日", "対象リスト", "ステップアップ", "成約"]].copy()


# ─────────────────────────────────────────────
#  サイドバー: 設定
# ─────────────────────────────────────────────
with st.sidebar:
    st.header("設定")

    default_id = st.secrets.get("sheets", {}).get("spreadsheet_id", "")
    spreadsheet_id = st.text_input("スプレッドシートID（約束・フォロー）", value=default_id)

    default_follow = st.secrets.get("sheets", {}).get("follow_sheet", "インスタフォロー(R8/1)")
    default_appt   = st.secrets.get("sheets", {}).get("yakusoku_sheet", "インスタ約束(R8)")
    follow_sheet   = st.text_input("フォロータブ名", value=default_follow)
    yakusoku_sheet = st.text_input("約束タブ名", value=default_appt)

    st.markdown("---")
    st.subheader("アカウントリスト照合")
    acct_source = st.radio("データソース", ["CSVファイル", "スプレッドシートID"], horizontal=True)
    if acct_source == "CSVファイル":
        uploaded_csv = st.file_uploader("CSVをアップロード（.csv形式）", type="csv",
                                         help="「ファイルを選択」または画面にドラッグ＆ドロップしてください。")
        account_sheet_id = ""
    else:
        uploaded_csv = None
        default_acct_id = st.secrets.get("sheets", {}).get("account_sheet_id", "")
        account_sheet_id = st.text_input("スプレッドシートID", value=default_acct_id)
        st.caption("※ 1枚目のシートを自動で使用します")

    if st.button("データを再読み込み"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.subheader("スプレッドシートの共有設定")
    st.info(
        "スプレッドシートを読み込むには、以下のサービスアカウントを「編集者」として共有してください。\n\n"
        "📧 `insta-dashboard@gen-lang-client-0366687858.iam.gserviceaccount.com`"
    )

    st.markdown("---")
    st.caption("データは5分間キャッシュされます")

# ─────────────────────────────────────────────
#  メイン
# ─────────────────────────────────────────────
st.title("インスタ活動分析ダッシュボード")

if not spreadsheet_id:
    st.info("サイドバーでスプレッドシートIDを入力してください。")
    st.stop()

with st.spinner("スプレッドシートを読み込み中..."):
    follow_rows   = load_sheet(spreadsheet_id, follow_sheet)
    yakusoku_rows = load_sheet(spreadsheet_id, yakusoku_sheet)

emp_df, daily_df = parse_follow_sheet(follow_rows)
yakusoku_df      = parse_yakusoku_sheet(yakusoku_rows)

# アカウントリスト照合（読み取り専用・メモリ上で処理）
account_map = {}
if uploaded_csv is not None:
    try:
        account_map = load_account_map_from_csv(uploaded_csv)
        st.sidebar.success(f"CSV読み込み完了: {len(account_map)} 件")
    except Exception as e:
        st.warning(f"CSVの読み込みに失敗しました: {e}")
elif account_sheet_id:
    try:
        account_map = load_account_map(account_sheet_id)
    except Exception as e:
        st.warning(f"アカウントリストの読み込みに失敗しました: {e}")

list_df = build_list_analysis(yakusoku_df, account_map)

if emp_df.empty:
    st.error("フォロータブのデータを読み込めませんでした。シート名・IDを確認してください。")
    st.stop()

# ── KPI サマリ ──
col1, col2, col3, col4 = st.columns(4)
col1.metric("たねまきをしている人数", f"{len(emp_df)} 名")
col2.metric("総フォロー数", f"{emp_df['フォロー累計'].sum():,}")
col3.metric("総DM数",      f"{emp_df['DM累計'].sum():,}")
col4.metric("総約束数",     f"{emp_df['約束累計'].sum():,}")

st.markdown("---")

# ── タブ ──
tab1, tab2, tab3, tab4 = st.tabs([
    "リスト別成約分析",
    "たねまきをしている人 別パフォーマンス",
    "異常値",
    "生データ確認",
])

# ======================================
# Tab1: リスト別成約分析
# ======================================
with tab1:
    st.subheader("リスト別 成果一覧")

    # ── AD列 空欄のみ更新 ──────────────────────────────
    if account_map:
        with st.expander("AD列（空欄のみ）を更新する"):
            st.caption("AD列が空欄の行のみリスト名を書き込みます。既存の値は上書きしません。")
            if st.button("空欄のみAD列に書き込む"):
                try:
                    with st.spinner("書き込み中..."):
                        written, skipped = fill_empty_ad_column(
                            spreadsheet_id, yakusoku_sheet, yakusoku_rows, account_map
                        )
                    st.success(f"完了: {written} 件書き込み、{skipped} 件スキップ（既存値あり）")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"エラー: {e}")

    st.markdown("---")

    if list_df.empty:
        st.info("約束データがないか、アカウントリストとの照合結果が0件です。")
    else:
        list_stats = (
            list_df.groupby("対象リスト")
            .agg(
                約束数=("対象リスト", "count"),
                ステップアップ数=("ステップアップ", "sum"),
                成約数=("成約", "sum"),
            )
            .reset_index()
        )
        list_stats["約束→OBA率(%)"] = (
            list_stats["成約数"] / list_stats["約束数"].replace(0, float("nan")) * 100
        ).round(1)
        list_stats["約束→中期法施率(%)"] = (
            list_stats["ステップアップ数"] / list_stats["約束数"].replace(0, float("nan")) * 100
        ).round(1)
        display = list_stats.rename(columns={"ステップアップ数": "中期法施数", "成約数": "OBA数"})
        display_cols = ["対象リスト", "約束数", "中期法施数", "約束→中期法施率(%)", "OBA数", "約束→OBA率(%)"]

        sc1, sc2 = st.columns([2, 1])
        sort_col1  = sc1.selectbox("並び替え", display_cols, index=display_cols.index("OBA数"), key="tab1_sort_col")
        sort_order1 = sc2.radio("順序", ["降順", "昇順"], horizontal=True, key="tab1_sort_ord")
        display = display[display_cols].sort_values(sort_col1, ascending=(sort_order1 == "昇順")).reset_index(drop=True)
        st.dataframe(display, use_container_width=True, hide_index=True)


# ======================================
# Tab2: たねまきをしている人 別パフォーマンス
# ======================================
with tab2:
    st.subheader("たねまきをしている人 別 活動実績")

    stats = emp_df.copy()
    stats["DM→約束率(%)"] = (
        stats["約束累計"] / stats["DM累計"].replace(0, float("nan")) * 100
    ).round(1)

    if not list_df.empty:
        emp_contract = (
            list_df.groupby("担当者名")["成約"]
            .sum()
            .reset_index()
            .rename(columns={"成約": "成約数"})
        )
        stats = stats.merge(emp_contract, on="担当者名", how="left")
        stats["成約数"] = stats["成約数"].fillna(0).astype(int)
        stats["約束→成約率(%)"] = (
            stats["成約数"] / stats["約束累計"].replace(0, float("nan")) * 100
        ).round(1)

    display_cols = ["担当者名", "アカウント名", "本部", "フォロー累計", "DM累計", "約束累計", "DM→約束率(%)"]
    if "成約数" in stats.columns:
        display_cols += ["成約数", "約束→成約率(%)"]

    sc1, sc2 = st.columns([2, 1])
    sort_col2   = sc1.selectbox("並び替え", display_cols, index=display_cols.index("フォロー累計"), key="tab2_sort_col")
    sort_order2 = sc2.radio("順序", ["降順", "昇順"], horizontal=True, key="tab2_sort_ord")
    stats_disp = stats[display_cols].sort_values(sort_col2, ascending=(sort_order2 == "昇順")).reset_index(drop=True)
    st.dataframe(stats_disp, use_container_width=True, hide_index=True)


# ======================================
# Tab3: 異常値
# ======================================
with tab3:
    st.subheader("転換率チェック")

    if emp_df.empty:
        st.info("データがありません。")
    else:
        check = emp_df.copy()
        check["フォロー→DM率(%)"] = (
            check["DM累計"] / check["フォロー累計"].replace(0, float("nan")) * 100
        ).round(1)
        check["DM→約束率(%)"] = (
            check["約束累計"] / check["DM累計"].replace(0, float("nan")) * 100
        ).round(1)

        avg_follow_dm = check["フォロー→DM率(%)"].mean()
        avg_dm_appt   = check["DM→約束率(%)"].mean()

        st.markdown(
            f"全体平均　フォロー→DM率: **{avg_follow_dm:.1f}%**　／　DM→約束率: **{avg_dm_appt:.1f}%**"
        )
        st.markdown("---")

        st.subheader("フォローしているがDMが少ない人")
        low_dm = check[
            (check["フォロー累計"] > 0) &
            (check["フォロー→DM率(%)"] < avg_follow_dm)
        ].sort_values("フォロー→DM率(%)")
        if low_dm.empty:
            st.success("該当者なし")
        else:
            cols = ["担当者名", "アカウント名", "本部", "フォロー累計", "DM累計", "フォロー→DM率(%)"]
            st.dataframe(low_dm[cols], use_container_width=True, hide_index=True)

        st.markdown("---")

        st.subheader("DMしているが約束が少ない人")
        low_appt = check[
            (check["DM累計"] > 0) &
            (check["DM→約束率(%)"] < avg_dm_appt)
        ].sort_values("DM→約束率(%)")
        if low_appt.empty:
            st.success("該当者なし")
        else:
            cols = ["担当者名", "アカウント名", "本部", "DM累計", "約束累計", "DM→約束率(%)"]
            st.dataframe(low_appt[cols], use_container_width=True, hide_index=True)

        st.markdown("---")
        st.subheader("異常値レポートをメールで送信")

        recipients_cfg = st.secrets.get("email", {}).get("recipients", [])
        if recipients_cfg:
            st.caption(f"送信先: {', '.join(recipients_cfg)}")
        else:
            st.warning("secrets.toml の [email] にSMTP設定と recipients を追加してください。")

        if st.button("今すぐ送信"):
            html = build_anomaly_email(low_dm, low_appt, avg_follow_dm, avg_dm_appt)
            err = send_anomaly_email(html)
            if err:
                st.error(f"送信失敗: {err}")
            else:
                st.success("送信しました。")


# ======================================
# Tab4: 生データ確認
# ======================================
with tab4:
    st.subheader("生データ確認（デバッグ用）")

    with st.expander("フォローシート（日次）"):
        st.dataframe(daily_df, use_container_width=True, hide_index=True)

    with st.expander("フォローシート（たねまきをしている人累計）"):
        st.dataframe(emp_df, use_container_width=True, hide_index=True)

    with st.expander("約束シート"):
        st.dataframe(yakusoku_df, use_container_width=True, hide_index=True)

    with st.expander("リスト×成約 結合データ"):
        st.dataframe(list_df, use_container_width=True, hide_index=True)
