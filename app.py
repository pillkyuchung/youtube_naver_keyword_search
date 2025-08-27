# app.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
from datetime import datetime, date, timezone
import datetime as dt

import pandas as pd
import streamlit as st
from googleapiclient.discovery import build

# ─────────────────────────────────────────────────────────────
# 공통: ENV/키 로딩
# ─────────────────────────────────────────────────────────────
from _env import Naver_CLient_ID, Client_Secret
NAVER_CLIENT_ID = Naver_CLient_ID
NAVER_CLIENT_SECRET = Client_Secret
NAVER_DATALAB_URL = "https://openapi.naver.com/v1/datalab/search"

import _env as _env_mod
YOUTUBE_API_KEY = _env_mod.API['youtube_api']

st.set_page_config(page_title="데이터 도구 모음 (Naver DataLab / YouTube)", page_icon="📊", layout="wide")
st.title("📊 데이터 도구 모음")

# ─────────────────────────────────────────────────────────────
# 공통: 유틸
# ─────────────────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_youtube_client(api_key: str):
    """googleapiclient Resource는 resource 캐시에 보관"""
    return build("youtube", "v3", developerKey=api_key)

def iso_range_last_months(months: int) -> tuple[str, str]:
    """오늘 기준 최근 N개월의 ISO8601 구간 생성 (UTC Z)"""
    today = date.today()
    start_dt = (pd.Timestamp(today) - pd.DateOffset(months=months)).date()
    after = f"{start_dt.isoformat()}T00:00:00Z"
    before = f"{today.isoformat()}T23:59:59Z"
    return after, before

# ─────────────────────────────────────────────────────────────
# Naver DataLab 구현 (기존 로직 기반)
# ─────────────────────────────────────────────────────────────
def parse_groups(text: str):
    """'그룹명: 키워드1, 키워드2' 형식의 여러 줄 → keywordGroups 구조"""
    groups = []
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        name, kws = line.split(":", 1)
        name = name.strip()
        keywords = [k.strip() for k in kws.split(",") if k.strip()]
        if name and keywords:
            groups.append({"groupName": name, "keywords": keywords})
    return groups

@st.cache_data(show_spinner=False, ttl=600)
def fetch_datalab(start_date: dt.date, end_date: dt.date, time_unit: str, keyword_groups: list[dict]) -> pd.DataFrame:
    """Naver DataLab 검색 트렌드 API 호출 → DataFrame(period, ratio, title)"""
    if not keyword_groups:
        return pd.DataFrame()

    payload = {
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d"),
        "timeUnit": time_unit,
        "keywordGroups": keyword_groups,
    }
    data = json.dumps(payload).encode("utf-8")

    req = st.experimental_singleton.clear  # no-op placeholder to avoid linter complaints
    import urllib.request
    req = urllib.request.Request(NAVER_DATALAB_URL)
    req.add_header("X-Naver-Client-Id", NAVER_CLIENT_ID)
    req.add_header("X-Naver-Client-Secret", NAVER_CLIENT_SECRET)
    req.add_header("Content-Type", "application/json")

    with urllib.request.urlopen(req, data=data, timeout=15) as resp:
        if resp.getcode() != 200:
            raise RuntimeError(f"HTTP {resp.getcode()}")
        body = resp.read().decode("utf-8")
        parsed = json.loads(body)

    frames = []
    for res in parsed.get("results", []):
        title = res.get("title")
        rows = res.get("data", [])
        if not rows:
            continue
        df = pd.DataFrame(rows)  # period, ratio
        df["title"] = title
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out["period"] = pd.to_datetime(out["period"])
    return out

def render_naver_datalab():
    st.header("🔎 Naver DataLab 검색 트렌드")
    with st.sidebar:
        st.subheader("Naver DataLab 설정")
        if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
            st.info("`.streamlit/secrets.toml` 또는 환경변수로 NAVER 키를 설정하세요.")

        today = dt.date.today()
        default_start = today - dt.timedelta(days=60)
        start_date = st.date_input("시작일", value=default_start, key="ndl_start")
        end_date = st.date_input("종료일", value=today, key="ndl_end")
        time_unit = st.selectbox("시간 단위", ["date", "week", "month"], index=0, key="ndl_unit")

        st.markdown("**키워드 그룹 입력법**: 한 줄에 한 그룹, `그룹명: 키워드1, 키워드2`")
        st.caption("예)  의자: 의자, 컴퓨터의자\n      영어: 영어, english")
        groups_raw = st.text_area("키워드 그룹", value="의자: 의자, 컴퓨터의자\n영어: 영어, english", height=120, key="ndl_groups")

        run_ndl = st.button("DataLab 조회", key="ndl_run")

    if run_ndl:
        if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
            st.error("NAVER_CLIENT_ID / NAVER_CLIENT_SECRET이 필요합니다.")
            return
        if start_date > end_date:
            st.error("시작일이 종료일보다 클 수 없습니다.")
            return

        groups = parse_groups(groups_raw)
        if not groups:
            st.warning("올바른 키워드 그룹을 입력하세요. 예) `의자: 의자, 컴퓨터의자`")
            return

        try:
            df = fetch_datalab(start_date, end_date, time_unit, groups)
            if df.empty:
                st.warning("데이터가 비어 있습니다. 기간/키워드를 변경해 보세요.")
            else:
                st.subheader("📊 결과 (데이터프레임)")
                st.dataframe(df.sort_values(["title", "period"]).reset_index(drop=True), use_container_width=True)
                st.line_chart(
                    df.pivot_table(index="period", columns="title", values="ratio", aggfunc="sum").sort_index()
                )
        except Exception as e:
            st.exception(e)
    else:
        st.info("사이드바를 설정하고 **DataLab 조회**를 누르세요.")

# ─────────────────────────────────────────────────────────────
# YouTube 구현 (요청 반영: 단일 키워드, 최근 N개월 전체 수집, 조회수 100 이하 제거)
# ─────────────────────────────────────────────────────────────
def _videos_list_chunked(youtube, ids: list[str]) -> list[dict]:
    out = []
    for i in range(0, len(ids), 50):
        resp = youtube.videos().list(part="snippet,statistics", id=",".join(ids[i:i+50])).execute()
        out.extend(resp.get("items", []))
    return out

@st.cache_data(show_spinner=False, ttl=600)
def fetch_all_youtube(api_key: str, keyword: str, order: str,
                      published_after: str, published_before: str) -> list[dict]:
    """기간 내 모든 결과 페이지네이션 수집 → 상세 조회"""
    youtube = get_youtube_client(api_key)

    all_ids: list[str] = []
    page_token = None
    while True:
        params = {
            "q": keyword,
            "part": "id",
            "type": "video",
            "order": order,
            "maxResults": 50,
            "publishedAfter": published_after,
            "publishedBefore": published_before,
        }
        if page_token:
            params["pageToken"] = page_token

        sr = youtube.search().list(**params).execute()
        ids = [it["id"]["videoId"] for it in sr.get("items", []) if it.get("id", {}).get("videoId")]
        all_ids.extend(ids)

        page_token = sr.get("nextPageToken")
        if not page_token:
            break

    if not all_ids:
        return []

    details = _videos_list_chunked(youtube, all_ids)
    videos = []
    for it in details:
        sn = it.get("snippet", {})
        stt = it.get("statistics", {})
        vid = it.get("id", "")
        videos.append({
            "title": sn.get("title", "") or "",
            "published_at": sn.get("publishedAt", "") or "",
            "url": f"https://www.youtube.com/watch?v={vid}",
            "view_count": int(stt.get("viewCount", 0)) if stt.get("viewCount") else 0,
            "like_count": int(stt.get("likeCount", 0)) if "likeCount" in stt else 0,
            "comment_count": int(stt.get("commentCount", 0)) if "commentCount" in stt else 0,
        })
    return videos

def calc_ratios(row: dict) -> tuple[float, float, datetime | None]:
    view = max(int(row.get("view_count", 0)), 0)
    like = max(int(row.get("like_count", 0)), 0)
    comt = max(int(row.get("comment_count", 0)), 0)
    like_ratio = round(like / view, 6) if view else 0.0
    comment_ratio = round(comt / view, 6) if view else 0.0
    pub_dt = datetime.fromisoformat(row["published_at"].replace("Z", "+00:00")) if row.get("published_at") else None
    return like_ratio, comment_ratio, pub_dt

def yt_to_dataframe(videos: list[dict]) -> pd.DataFrame:
    """표시용 DataFrame: 제목, URL, 게시일, 조회수, 좋아요 비율, 댓글 비율 (조회수 100 이하 제거)"""
    if not videos:
        return pd.DataFrame(columns=["title", "url", "published_at", "view_count", "like_ratio", "comment_ratio"])
    rows = []
    for v in videos:
        if v["view_count"] < 100:
            continue
        like_r, comt_r, pub_dt = calc_ratios(v)
        rows.append({
            "title": v["title"],
            "url": v["url"],
            "published_at": pub_dt,
            "view_count": v["view_count"],
            "like_ratio": like_r,
            "comment_ratio": comt_r,
        })
    return pd.DataFrame(rows)

def render_youtube():
    st.header("📺 YouTube 키워드 분석 (최근 N개월, 조회수 100 초과)")
    with st.sidebar:
        st.subheader("YouTube 설정")
        if not YOUTUBE_API_KEY:
            st.info("`.streamlit/secrets.toml` 또는 환경변수로 YOUTUBE_API_KEY를 설정하세요.")
        keyword = st.text_input("키워드 (1개)", value="허먼밀러 에어론", key="yt_kw").strip()
        months_back = st.number_input("최근 N개월 (최대 24)", min_value=1, max_value=24, value=6, step=1, key="yt_months")
        order = st.selectbox("정렬", ["date", "relevance", "viewCount"], index=0, key="yt_order")
        run_yt = st.button("YouTube 조회", key="yt_run")

    if run_yt:
        if not YOUTUBE_API_KEY:
            st.error("YouTube API Key가 필요합니다.")
            return
        if not keyword:
            st.error("키워드를 입력하세요.")
            return

        after_iso, before_iso = iso_range_last_months(months_back)

        try:
            with st.spinner("YouTube API 호출 중 (페이지네이션)…"):
                videos = fetch_all_youtube(
                    api_key=YOUTUBE_API_KEY,
                    keyword=keyword,
                    order=order,
                    published_after=after_iso,
                    published_before=before_iso,
                )

            df = yt_to_dataframe(videos)

            st.subheader("📦 수집 개요")
            c1, c2, c3 = st.columns(3)
            c1.metric("키워드", keyword)
            c2.metric("수집된 영상 수 (조회수 100 초과)", len(df))
            c3.metric("기간", f"{after_iso[:10]} ~ {before_iso[:10]}")

            st.subheader("📄 영상 목록")
            if df.empty:
                st.info("조건에 맞는 영상이 없습니다.")
            else:
                df_view = df.sort_values("published_at", ascending=False).reset_index(drop=True)
                st.dataframe(df_view, use_container_width=True)

                @st.cache_data
                def to_csv_bytes(xdf: pd.DataFrame) -> bytes:
                    return xdf.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

                st.download_button(
                    "CSV 다운로드",
                    data=to_csv_bytes(df_view),
                    file_name=f"youtube_{keyword}_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
        except Exception as e:
            st.exception(e)
    else:
        st.info("사이드바를 설정하고 **YouTube 조회**를 누르세요.")

# ─────────────────────────────────────────────────────────────
# 메인: 기능 선택 (탭/셀렉트)
# ─────────────────────────────────────────────────────────────
mode = st.radio("기능 선택", ["Naver DataLab", "YouTube 키워드"], horizontal=True)

if mode == "Naver DataLab":
    render_naver_datalab()
else:
    render_youtube()
