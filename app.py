#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
インスタ活動分析ダッシュボード

Google スプレッドシートの2タブ（フォロー活動・約束一覧）を読み込み、
リスト別成約率・たねまきをしている人 別パフォーマンス・日別トレンドを可視化する。
"""

import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import gspread
import pandas as pd
import plotly.express as px
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
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
DAILY_START_COL = 10   # K列（0インデックス）
COLS_PER_DAY = 4       # 対象 / Fw / DM / 約束


# ─────────────────────────────────────────────
#  メール送信
# ─────────────────────────────────────────────
def build_anomaly_email(low_dm: pd.DataFrame, low_appt: pd.DataFrame,
                         avg_follow_dm: float, avg_dm_appt: float) -> str:
    """異常値レポートのHTML本文を生成する"""
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
    """secrets.toml の設定を使ってメールを送信する。成功時は '' を返す。"""
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
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=SCOPES
    )
    return gspread.authorize(creds)


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


# ─────────────────────────────────────────────
#  Instagram URL → アカウントID
# ─────────────────────────────────────────────
def extract_instagram_id(url: str) -> str:
    """https://www.instagram.com/username/ → username（小文字）"""
    url = url.strip().rstrip("/")
    m = re.search(r"instagram\.com/([^/?#]+)", url)
    return m.group(1).lower() if m else ""


# ─────────────────────────────────────────────
#  アカウントリスト読み込み（スプレッドシート2）
# ─────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_account_map(sheet_id: str, sheet_tab: str) -> dict[str, str]:
    """スプレッドシート2のA列(アカウントID) → F列(リスト名) の辞書を返す"""
    gc = get_client()
    sh = gc.open_by_key(sheet_id)
    titles = {w.title.strip(): w for w in sh.worksheets()}
    ws = titles.get(sheet_tab.strip())
    if ws is None:
        available = list(titles.keys())
        raise ValueError(f"タブ '{sheet_tab}' が見つかりません。利用可能なタブ: {available}")
    rows = ws.get_all_values()
    result = {}
    for row in rows:
        if len(row) >= 6:
            account_id = row[0].strip().lower()
            list_name  = row[5].strip()
            if account_id and list_name and re.match(r'^[a-zA-Z0-9\s\-_.]+$', list_name):
                result[account_id] = list_name
    return result


# ─────────────────────────────────────────────
#  AD列書き込み
# ─────────────────────────────────────────────
def update_list_column(spreadsheet_id: str, yakusoku_sheet_name: str,
                        yakusoku_rows: list, account_map: dict) -> tuple[int, int]:
    """約束シートのAD列(index=29)にリスト名を書き込む。(一致数, 不一致数)を返す"""
    gc = get_client()
    sh = gc.open_by_key(spreadsheet_id)
    titles = {w.title.strip(): w for w in sh.worksheets()}
    ws = titles.get(yakusoku_sheet_name.strip())
    if ws is None:
        available = list(titles.keys())
        raise ValueError(f"タブ '{yakusoku_sheet_name}' が見つかりません。利用可能なタブ: {available}")

    updates = []
    matched = 0
    unmatched = 0
    for i, row in enumerate(yakusoku_rows[1:], start=2):  # 2行目から（1行目はヘッダ）
        url = row[10].strip() if len(row) > 10 else ""   # K列
        if not url:
            continue
        account_id = extract_instagram_id(url)
        list_name  = account_map.get(account_id, "")
        updates.append({"range": f"AD{i}", "values": [[list_name]]})
        if list_name:
            matched += 1
        else:
            unmatched += 1

    if updates:
        ws.batch_update(updates)
    return matched, unmatched


def parse_follow_sheet(rows: list[list]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    フォロータブをパースして (たねまきをしている人累計DF, 日次活動DF) を返す。

    行構造:
      rows[0] : 年月・日付ヘッダ（例: 1日(木)）が K列以降に 4列おきに入る
      rows[1] : 列名（名前/本部/…/対象/Fw/DM/約束/…）
      rows[2+]: たねまきをしている人データ
    """
    if len(rows) < 3:
        return pd.DataFrame(), pd.DataFrame()

    date_row = rows[0]

    # 日付列位置を抽出（K列以降、4列おきに日付文字列が入る）
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
    last_name = ""  # 直前の有効な名前を保持（空欄行の引き継ぎ用）

    for row in rows[2:]:
        if not row or len(row) < 2:
            continue
        name = row[1].strip() if len(row) > 1 else ""
        if name and name != "名前":
            if re.search(r'[0-9]', name):
                continue  # 半角数字を含む行はたねまきをしている人ではないためスキップ
            last_name = name  # 有効な名前を更新
        elif last_name:
            name = last_name  # 空欄なら直前の名前を使用
        else:
            continue  # 最初から名前がない行はスキップ

        dept         = row[2].strip() if len(row) > 2 else ""
        account      = row[3].strip() if len(row) > 3 else ""  # D列
        follow_total = _safe_int(row[4]) if len(row) > 4 else 0  # E列
        dm_total     = _safe_int(row[6]) if len(row) > 6 else 0  # G列
        appt_total   = _safe_int(row[7]) if len(row) > 7 else 0  # H列

        # フォロー・DM・約束がすべて0の行は除外
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
                "担当者名": name,
                "本部": dept,
                "日": day_num,
                "対象": target,
                "フォロー数": follow,
                "DM数": dm,
                "約束数": appt,
            })

    return pd.DataFrame(emp_records), pd.DataFrame(daily_records)


def parse_yakusoku_sheet(rows: list[list]) -> pd.DataFrame:
    """
    約束タブをパースして約束一覧DFを返す。

    列: B=月, C=日, H=担当者名, K=InstagramURL, O=済（ステップアップ）, AA=成約, AD=対象リスト
    """
    records = []
    for row in rows[1:]:  # 1行目ヘッダをスキップ
        if len(row) < 8:
            continue
        employee = row[7].strip() if len(row) > 7 else ""
        if not employee:
            continue

        step_up   = bool(row[14].strip()) if len(row) > 14 else False
        contract  = bool(row[26].strip()) if len(row) > 26 else False
        insta_url = row[10].strip()       if len(row) > 10 else ""
        list_name = row[29].strip()       if len(row) > 29 else ""  # AD列

        records.append({
            "月": _safe_int(row[1]),
            "日": _safe_int(row[2]),
            "担当者名": employee,
            "Instagram URL": insta_url,
            "対象リスト": list_name,
            "ステップアップ": step_up,
            "成約": contract,
        })

    return pd.DataFrame(records)


# ─────────────────────────────────────────────
#  分析: AD列の対象リストをそのまま使う
# ─────────────────────────────────────────────
EXCLUDE_LIST_NAMES = {"対象アカ"}

def build_list_analysis(yakusoku_df: pd.DataFrame) -> pd.DataFrame:
    """AD列に書き込まれた対象リスト列を使って集計用DFを返す"""
    if yakusoku_df.empty or "対象リスト" not in yakusoku_df.columns:
        return pd.DataFrame()
    df = yakusoku_df[
        (yakusoku_df["対象リスト"] != "") &
        (~yakusoku_df["対象リスト"].isin(EXCLUDE_LIST_NAMES)) &
        (yakusoku_df["対象リスト"].str.match(r'^[a-zA-Z0-9\s\-_.]+$', na=False))
    ].copy()
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
    default_acct_id  = st.secrets.get("sheets", {}).get("account_sheet_id", "")
    default_acct_tab = st.secrets.get("sheets", {}).get("account_sheet_tab", "シート1")
    account_sheet_id  = st.text_input("アカウントリストのスプレッドシートID", value=default_acct_id)
    account_sheet_tab = st.text_input("アカウントリストのタブ名", value=default_acct_tab)

    if st.button("データを再読み込み"):
        st.cache_data.clear()
        st.rerun()

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
list_df          = build_list_analysis(yakusoku_df)

if emp_df.empty:
    st.error("フォロータブのデータを読み込めませんでした。シート名・IDを確認してください。")
    st.stop()

# ── KPI サマリ ──
col1, col2, col3, col4 = st.columns(4)
col1.metric("たねまきをしている人数",    f"{len(emp_df)} 名")
col2.metric("総フォロー数", f"{emp_df['フォロー累計'].sum():,}")
col3.metric("総DM数",     f"{emp_df['DM累計'].sum():,}")
col4.metric("総約束数",    f"{emp_df['約束累計'].sum():,}")

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

    # ── AD列更新ボタン ──────────────────────────────
    with st.expander("AD列（対象リスト）を更新する"):
        st.caption("約束シートのK列URLをアカウントリストと照合し、AD列にリスト名を書き込みます。")
        if not account_sheet_id:
            st.warning("サイドバーでアカウントリストのスプレッドシートIDを入力してください。")
        elif st.button("照合してAD列に書き込む"):
            try:
                with st.spinner("照合中..."):
                    account_map = load_account_map(account_sheet_id, account_sheet_tab)
                    matched, unmatched = update_list_column(
                        spreadsheet_id, yakusoku_sheet, yakusoku_rows, account_map
                    )
                st.success(f"完了: {matched} 件一致、{unmatched} 件未一致")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"エラー: {e}")

    st.markdown("---")

    if list_df.empty:
        st.info("約束データがないか、紐付けに必要な対象リスト情報が不足しています。")
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
        list_stats = list_stats.sort_values("成約数", ascending=False).reset_index(drop=True)

        display = list_stats.rename(columns={
            "ステップアップ数": "中期法施数",
            "成約数": "OBA数",
        })
        display_cols = ["対象リスト", "約束数", "中期法施数", "約束→中期法施率(%)", "OBA数", "約束→OBA率(%)"]
        st.dataframe(display[display_cols], use_container_width=True, hide_index=True)



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
    st.dataframe(stats[display_cols], use_container_width=True, hide_index=True)


# ======================================
# Tab3: 転換率の低いたねまきをしている人
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

        # 全体平均
        avg_follow_dm = check["フォロー→DM率(%)"].mean()
        avg_dm_appt   = check["DM→約束率(%)"].mean()

        st.markdown(
            f"全体平均　フォロー→DM率: **{avg_follow_dm:.1f}%**　／　DM→約束率: **{avg_dm_appt:.1f}%**"
        )
        st.markdown("---")

        # フォロー累計が多いのにDM累計が少ない人（フォロー→DM率が平均を下回る）
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

        # DM累計はあるが約束累計が少ない人（DM→約束率が平均を下回る）
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

        # ── メール送信 ──────────────────────────────
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
