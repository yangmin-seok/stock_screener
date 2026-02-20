from __future__ import annotations

from pathlib import Path

import streamlit as st

from stock_screener.pipelines.daily_batch import DailyBatchPipeline
from stock_screener.screener.dsl import apply_filters, preset_conditions
from stock_screener.storage.db import init_db
from stock_screener.storage.repository import Repository

DB_PATH = Path("data/screener.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
init_db(DB_PATH)
repo = Repository(DB_PATH)
pipeline = DailyBatchPipeline(DB_PATH)

st.set_page_config(layout="wide", page_title="KR Fundamental Screener")
st.title("🇰🇷 한국 주식 Fundamental Screener (pykrx + SQLite cache)")
st.caption("최초 실행 시 pykrx 수집으로 시간이 걸리며, 이후에는 DB snapshot을 재사용합니다.")

if "asof" not in st.session_state:
    st.session_state.asof = repo.get_latest_snapshot_date()

c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
with c1:
    refresh_full = st.button("전체 수집 + 스냅샷", type="primary")
with c2:
    refresh_snapshot = st.button("스냅샷만 재계산", help="이미 수집된 DB 데이터로 snapshot만 다시 계산")
with c3:
    refresh_reserve = st.button("유보율만 업데이트", help="네이버 크롤링으로 최신 유보율만 업데이트")
with c4:
    force_date = st.date_input("asof date (optional)", value=None)

target_asof = force_date.strftime("%Y-%m-%d") if force_date else None

if refresh_full:
    with st.spinner("pykrx 전체 수집 + snapshot 생성 중... (초기 1회 느림)"):
        result = pipeline.run(asof_date=target_asof)
    st.session_state.asof = result.asof_date
    st.success(
        f"전체 수집 완료: {result.asof_date} | 티커 {result.tickers}개 | "
        f"prices {result.prices:,}건 | cap {result.cap:,}건 | fundamental {result.fundamental:,}건 | snapshot {result.snapshot:,}건"
    )

if refresh_snapshot:
    try:
        with st.spinner("DB 캐시 기반 snapshot만 재계산 중..."):
            result = pipeline.rebuild_snapshot_only(asof_date=target_asof)
        st.session_state.asof = result.asof_date
        st.success(f"스냅샷 재계산 완료: {result.asof_date} | snapshot {result.snapshot:,}건")
    except ValueError as exc:
        st.error(f"스냅샷만 재계산 실패: {exc}")

if refresh_reserve:
    with st.spinner("네이버 크롤링으로 유보율 업데이트 중..."):
        updated_asof, updated_rows = pipeline.update_reserve_ratio_only(asof_date=target_asof)
        pipeline.rebuild_snapshot_only(asof_date=updated_asof)
    st.session_state.asof = updated_asof
    st.success(f"유보율 업데이트 완료: {updated_asof} | reserve_ratio {updated_rows:,}건")

asof = st.session_state.asof
if not asof:
    st.warning("snapshot이 없습니다. 먼저 '전체 수집 + 스냅샷' 또는 '스냅샷만 재계산' 버튼을 실행하세요.")
    st.stop()

base = repo.load_snapshot(asof)
if base.empty:
    st.warning("선택한 asof_date snapshot이 비어 있습니다. 다시 수집해 주세요.")
    st.stop()

st.subheader(f"Snapshot as of {asof}")
st.write(f"현재 snapshot 종목 수: **{len(base):,}개**")

preset = st.selectbox(
    "프리셋",
    ["none", "deep_value", "rerating", "dividend_lowvol", "momentum", "eps_growth_breakout"],
)

st.markdown("### 조건 선택")
st.caption("원하는 조건만 체크해서 적용하세요. 체크하지 않은 조건은 필터에 사용되지 않습니다. 프리셋 `none`은 프리셋 조건을 적용하지 않는 모드입니다.")

mkt = st.multiselect("시장", sorted(base["market"].dropna().unique().tolist()), default=[])

apply_mcap_min = st.checkbox("최소 시총(원) 적용", value=False)
mcap_min = st.number_input("최소 시총(원)", min_value=0.0, value=0.0, step=100_000_000.0, disabled=not apply_mcap_min)

apply_value_min = st.checkbox("최소 20D 평균 거래대금(원) 적용", value=False)
value_min = st.number_input(
    "최소 20D 평균 거래대금(원)", min_value=0.0, value=0.0, step=100_000_000.0, disabled=not apply_value_min
)

apply_pbr_max = st.checkbox("최대 PBR 적용", value=False)
pbr_max = st.number_input("최대 PBR", min_value=0.0, value=1.0, step=0.1, disabled=not apply_pbr_max)

apply_reserve_ratio_min = st.checkbox("최소 유보율(%) 적용", value=False)
reserve_ratio_min = st.number_input("최소 유보율(%)", value=500.0, step=50.0, disabled=not apply_reserve_ratio_min)

apply_roe_min = st.checkbox("최소 ROE proxy 적용", value=False)
roe_min = st.number_input("최소 ROE proxy", value=0.1, step=0.01, disabled=not apply_roe_min)

apply_eps_positive = st.checkbox("EPS 흑자 기업만(적자 제외)", value=False)

above_200ma = st.checkbox("200일선 위 조건 적용", value=False)

st.markdown("### Growth 조건 선택")
apply_eps_cagr_5y = st.checkbox("최근 5년 EPS CAGR 조건 적용", value=False)
eps_cagr_5y_min = st.number_input(
    "최근 5년 EPS CAGR 최소", value=0.15, step=0.01, format="%.2f", disabled=not apply_eps_cagr_5y
)

apply_eps_yoy_q = st.checkbox("최근 분기 EPS YoY 조건 적용", value=False)
eps_yoy_q_min = st.number_input(
    "최근 분기 EPS YoY 최소", value=0.25, step=0.01, format="%.2f", disabled=not apply_eps_yoy_q
)

apply_near_high = st.checkbox("현재가 / 52주 신고가 조건 적용", value=False)
near_high_min = st.number_input(
    "현재가 / 52주 신고가 최소", value=0.90, step=0.01, format="%.2f", disabled=not apply_near_high
)


active_filter_count = sum(
    [
        int(preset != "none"),
        int(bool(mkt)),
        int(apply_mcap_min),
        int(apply_value_min),
        int(apply_pbr_max),
        int(apply_reserve_ratio_min),
        int(apply_roe_min),
        int(apply_eps_positive),
        int(above_200ma),
        int(apply_eps_cagr_5y),
        int(apply_eps_yoy_q),
        int(apply_near_high),
    ]
)
st.caption(f"적용 중인 조건 수: {active_filter_count}개")

filtered = base.copy()
if preset != "none":
    filtered = apply_filters(filtered, preset_conditions(preset))

if mkt:
    filtered = filtered[filtered["market"].isin(mkt)]
if apply_mcap_min:
    filtered = filtered[filtered["mcap"] >= mcap_min]
if apply_value_min:
    filtered = filtered[filtered["avg_value_20d"] >= value_min]
if apply_pbr_max:
    filtered = filtered[(filtered["pbr"].notna()) & (filtered["pbr"] <= pbr_max)]
if apply_reserve_ratio_min:
    filtered = filtered[(filtered["reserve_ratio"].notna()) & (filtered["reserve_ratio"] >= reserve_ratio_min)]
if apply_roe_min:
    filtered = filtered[(filtered["roe_proxy"].notna()) & (filtered["roe_proxy"] >= roe_min)]
if apply_eps_positive:
    filtered = filtered[filtered["eps_positive"] == 1]
if above_200ma:
    filtered = filtered[filtered["dist_sma200"] >= 0]
if apply_eps_cagr_5y:
    filtered = filtered[(filtered["eps_cagr_5y"].notna()) & (filtered["eps_cagr_5y"] >= eps_cagr_5y_min)]
if apply_eps_yoy_q:
    filtered = filtered[(filtered["eps_yoy_q"].notna()) & (filtered["eps_yoy_q"] >= eps_yoy_q_min)]
if apply_near_high:
    filtered = filtered[(filtered["near_52w_high_ratio"].notna()) & (filtered["near_52w_high_ratio"] >= near_high_min)]

sort_col = st.selectbox(
    "정렬 컬럼",
    ["mcap", "pbr", "reserve_ratio", "roe_proxy", "ret_3m", "div", "avg_value_20d", "eps_cagr_5y", "eps_yoy_q", "near_52w_high_ratio"],
    index=0,
)
ascending = st.checkbox("오름차순", value=False)
limit = st.slider("출력 개수", min_value=10, max_value=500, value=100, step=10)

filtered = filtered.sort_values(sort_col, ascending=ascending).head(limit)

if filtered.empty:
    st.warning("조건을 만족하는 종목이 없습니다. Growth 조건(EPS CAGR/EPS YoY) 임계값을 낮추거나 체크를 해제해 보세요.")

show_cols = [
    "ticker", "name", "market", "close", "mcap", "avg_value_20d", "pbr", "reserve_ratio", "per", "div", "dps",
    "eps", "bps", "roe_proxy", "eps_positive", "ret_3m", "ret_1y", "dist_sma200", "pos_52w",
    "near_52w_high_ratio", "eps_cagr_5y", "eps_yoy_q",
]
st.dataframe(filtered[show_cols], width="stretch", hide_index=True)

csv = filtered[show_cols].to_csv(index=False).encode("utf-8-sig")
st.download_button("CSV 다운로드", data=csv, file_name=f"screener_{asof}.csv", mime="text/csv")
