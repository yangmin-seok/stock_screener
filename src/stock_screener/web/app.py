from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import streamlit as st

from stock_screener.pipelines.daily_batch import DailyBatchPipeline
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
st.caption("기본 asof = 최신 거래일(가격 데이터 기준), 해당 거래일 snapshot이 없으면 재계산이 필요합니다.")


@dataclass(frozen=True)
class FilterSpec:
    name: str
    ftype: str
    default: Any


FILTER_SPECS: list[FilterSpec] = [
    FilterSpec("ticker_input", "str", ""),
    FilterSpec("mkt", "list", []),
    FilterSpec("mcap_mode", "str", "any"),
    FilterSpec("mcap_bucket", "str", "any"),
    FilterSpec("mcap_min", "float", 0.0),
    FilterSpec("mcap_max", "float", 0.0),
    FilterSpec("apply_value_min", "bool", False),
    FilterSpec("value_min", "float", 0.0),
    FilterSpec("apply_pbr_max", "bool", False),
    FilterSpec("pbr_max", "float", 1.0),
    FilterSpec("apply_roe_min", "bool", False),
    FilterSpec("roe_min", "float", 0.1),
    FilterSpec("apply_eps_positive", "bool", False),
    FilterSpec("apply_reserve_ratio_min", "bool", False),
    FilterSpec("reserve_ratio_min", "float", 500.0),
    FilterSpec("apply_eps_cagr_5y", "bool", False),
    FilterSpec("eps_cagr_5y_min", "float", 0.15),
    FilterSpec("apply_eps_yoy_q", "bool", False),
    FilterSpec("eps_yoy_q_min", "float", 0.25),
    FilterSpec("above_200ma", "bool", False),
    FilterSpec("apply_near_high", "bool", False),
    FilterSpec("near_high_min", "float", 0.9),
    FilterSpec("sort_col", "str", "mcap"),
    FilterSpec("ascending", "bool", False),
    FilterSpec("limit", "int", 100),
]

MCAP_MODES: dict[str, str] = {
    "any": "Any",
    "bucket": "구간선택",
    "custom": "Custom",
}

MCAP_BUCKETS: list[dict[str, Any]] = [
    {"key": "any", "label": "전체", "min_mcap": None, "max_mcap": None},
    {"key": "mega", "label": "초대형주 (10조 이상)", "min_mcap": 10_000_000_000_000.0, "max_mcap": None},
    {"key": "large", "label": "대형주 (2조~10조)", "min_mcap": 2_000_000_000_000.0, "max_mcap": 10_000_000_000_000.0},
    {"key": "mid", "label": "중형주 (3천억~2조)", "min_mcap": 300_000_000_000.0, "max_mcap": 2_000_000_000_000.0},
    {"key": "small", "label": "소형주 (5백억~3천억)", "min_mcap": 50_000_000_000.0, "max_mcap": 300_000_000_000.0},
    {"key": "micro", "label": "초소형주 (5백억 미만)", "min_mcap": None, "max_mcap": 50_000_000_000.0},
]

MCAP_BUCKET_MAP = {bucket["key"]: (bucket["min_mcap"], bucket["max_mcap"]) for bucket in MCAP_BUCKETS}
MCAP_BUCKET_LABEL_MAP = {bucket["key"]: bucket["label"] for bucket in MCAP_BUCKETS}


def _get_query_params() -> dict[str, Any]:
    if hasattr(st, "query_params"):
        return dict(st.query_params)
    return st.experimental_get_query_params()


def _set_query_params(params: dict[str, Any]) -> None:
    if hasattr(st, "query_params"):
        qp = st.query_params
        qp.clear()
        for key, value in params.items():
            qp[key] = value
        return
    st.experimental_set_query_params(**params)


def _parse_bool(raw: Any, *, default: bool) -> bool:
    if raw is None:
        return default
    value = raw[0] if isinstance(raw, list) and raw else raw
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"invalid bool: {value}")


def _parse_num(raw: Any, cast_type: type[int] | type[float], *, default: int | float) -> int | float:
    if raw is None:
        return default
    value = raw[0] if isinstance(raw, list) and raw else raw
    try:
        return cast_type(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {cast_type.__name__}: {value}") from exc


def _parse_list(raw: Any, *, default: list[str]) -> list[str]:
    if raw is None:
        return default
    values = raw if isinstance(raw, list) else str(raw).split(",")
    return [item.strip() for item in values if str(item).strip()]


def _parse_str(raw: Any, *, default: str) -> str:
    if raw is None:
        return default
    if isinstance(raw, list):
        return str(raw[0]) if raw else default
    return str(raw)


def _parse_query_filter_value(spec: FilterSpec, query_params: dict[str, Any]) -> Any:
    raw = query_params.get(spec.name)
    if spec.ftype == "bool":
        return _parse_bool(raw, default=spec.default)
    if spec.ftype == "int":
        return _parse_num(raw, int, default=spec.default)
    if spec.ftype == "float":
        return _parse_num(raw, float, default=spec.default)
    if spec.ftype == "list":
        return _parse_list(raw, default=list(spec.default))
    return _parse_str(raw, default=spec.default)


def _serialize_query_filter_value(spec: FilterSpec, value: Any) -> str | list[str] | None:
    if value == spec.default or value in (None, ""):
        return None
    if spec.ftype == "bool":
        return "1" if bool(value) else "0"
    if spec.ftype in {"int", "float", "str"}:
        return str(value)
    if spec.ftype == "list":
        values = [str(item).strip() for item in value if str(item).strip()]
        return values if values else None
    return None


def _run_action_with_progress(action_label: str, callback):
    progress_text = st.empty()
    progress_bar = st.progress(0)
    progress_text.info(f"{action_label}: 요청 접수")
    progress_bar.progress(15)
    progress_text.info(f"{action_label}: 작업 실행 중")
    progress_bar.progress(45)
    result = callback()
    progress_text.info(f"{action_label}: 결과 반영 중")
    progress_bar.progress(85)
    progress_bar.progress(100)
    progress_text.success(f"{action_label}: 완료")
    return result


query_params = _get_query_params()
if "query_params_restored" not in st.session_state:
    st.session_state.query_parse_errors = []
    for spec in FILTER_SPECS:
        try:
            st.session_state[spec.name] = _parse_query_filter_value(spec, query_params)
        except ValueError:
            st.session_state[spec.name] = spec.default
            st.session_state.query_parse_errors.append(spec.name)
    if st.session_state.get("mcap_mode") not in MCAP_MODES:
        st.session_state.mcap_mode = "any"
        st.session_state.query_parse_errors.append("mcap_mode")
    if st.session_state.get("mcap_bucket") not in MCAP_BUCKET_MAP:
        st.session_state.mcap_bucket = "any"
        st.session_state.query_parse_errors.append("mcap_bucket")
    st.session_state.query_params_restored = True

if st.session_state.get("query_parse_errors"):
    st.warning(
        "일부 URL 필터값을 복원하지 못해 기본값으로 대체했습니다: "
        + ", ".join(st.session_state.query_parse_errors)
    )

if "asof" not in st.session_state:
    st.session_state.asof = repo.get_latest_price_date() or repo.get_latest_snapshot_date()

latest_price_date = repo.get_latest_price_date()
latest_snapshot_date = repo.get_latest_snapshot_date()
if latest_price_date and latest_price_date != latest_snapshot_date:
    auto_sync_target = latest_price_date
    if st.session_state.get("auto_snapshot_synced_for") != auto_sync_target:
        try:
            with st.spinner(f"최신 거래일({auto_sync_target}) snapshot 자동 재계산 중..."):
                auto_result = _run_action_with_progress(
                    "최신 거래일 snapshot 자동 동기화",
                    lambda: pipeline.rebuild_snapshot_only(asof_date=auto_sync_target),
                )
            st.session_state.asof = auto_result.asof_date
            st.session_state.auto_snapshot_synced_for = auto_sync_target
            st.success(f"최신 거래일 snapshot 자동 동기화 완료: {auto_result.asof_date}")
        except ValueError as exc:
            st.warning(f"최신 거래일 snapshot 자동 동기화 실패: {exc}")

c1, c2, c3 = st.columns([1, 1, 1])
with c1:
    refresh_full = st.button("전체 수집 + 스냅샷", type="primary")
with c2:
    refresh_snapshot = st.button("스냅샷만 재계산", help="이미 수집된 DB 데이터로 snapshot만 다시 계산")
with c3:
    refresh_reserve = st.button("유보율만 업데이트", help="네이버 크롤링으로 최신 유보율만 업데이트")

if refresh_full:
    with st.spinner("pykrx 전체 수집 + snapshot 생성 중... (초기 1회 느림)"):
        result = _run_action_with_progress("전체 수집 + 스냅샷", lambda: pipeline.run(asof_date=None))
    st.session_state.asof = result.asof_date
    st.success(
        f"전체 수집 완료: {result.asof_date} | 티커 {result.tickers}개 | "
        f"prices {result.prices:,}건 | cap {result.cap:,}건 | fundamental {result.fundamental:,}건 | snapshot {result.snapshot:,}건"
    )

if refresh_snapshot:
    try:
        with st.spinner("DB 캐시 기반 snapshot만 재계산 중..."):
            result = _run_action_with_progress(
                "스냅샷 재계산",
                lambda: pipeline.rebuild_snapshot_only(asof_date=repo.get_latest_price_date()),
            )
        st.session_state.asof = result.asof_date
        st.success(f"스냅샷 재계산 완료: {result.asof_date} | snapshot {result.snapshot:,}건")
    except ValueError as exc:
        st.error(f"스냅샷만 재계산 실패: {exc}")

if refresh_reserve:
    with st.spinner("네이버 크롤링으로 유보율 업데이트 중..."):
        update_result = _run_action_with_progress(
            "유보율 업데이트",
            lambda: pipeline.update_reserve_ratio_only(asof_date=repo.get_latest_price_date()),
        )
        updated_asof, updated_rows = update_result
        _run_action_with_progress(
            "유보율 반영 snapshot 재계산",
            lambda: pipeline.rebuild_snapshot_only(asof_date=updated_asof),
        )
    st.session_state.asof = updated_asof
    st.success(f"유보율 업데이트 완료: {updated_asof} | reserve_ratio {updated_rows:,}건")

asof = st.session_state.asof
if not asof:
    st.warning("snapshot이 없습니다. 먼저 '전체 수집 + 스냅샷' 또는 '스냅샷만 재계산' 버튼을 실행하세요.")
    st.stop()

base = repo.load_snapshot(asof)
if base.empty:
    st.warning(
        "해당 거래일 스냅샷이 없습니다. '스냅샷만 재계산' 버튼으로 스냅샷 재계산이 필요합니다."
    )
    st.stop()

st.subheader(f"Snapshot as of {asof}")
st.write(f"현재 snapshot 종목 수: **{len(base):,}개**")

st.markdown("### 조건 선택")
st.caption("원하는 조건만 체크해서 적용하세요. 체크하지 않은 조건은 필터에 사용되지 않습니다.")

descriptive_tab, fundamental_tab, technical_tab = st.tabs(["Descriptive", "Fundamental", "Technical"])

with descriptive_tab:
    ticker_input = st.text_input("티커 직접 입력", help="콤마(,) 또는 공백으로 여러 티커를 입력하세요.", key="ticker_input")

    raw_tickers = [token.strip().upper() for token in re.split(r"[\s,]+", ticker_input or "") if token.strip()]
    ticker_list = list(dict.fromkeys(raw_tickers))

    mkt = st.multiselect("시장", sorted(base["market"].dropna().unique().tolist()), key="mkt")

    mcap_mode = st.radio(
        "시총 필터 모드",
        options=list(MCAP_MODES.keys()),
        format_func=lambda mode: MCAP_MODES[mode],
        horizontal=True,
        key="mcap_mode",
    )

    mcap_bucket = st.selectbox(
        "시총 구간",
        options=[bucket["key"] for bucket in MCAP_BUCKETS],
        format_func=lambda key: MCAP_BUCKET_LABEL_MAP[key],
        disabled=mcap_mode != "bucket",
        key="mcap_bucket",
    )

    custom_mode = mcap_mode == "custom"
    mcap_min = st.number_input(
        "최소 시총(원)",
        min_value=0.0,
        step=100_000_000.0,
        disabled=not custom_mode,
        key="mcap_min",
    )
    mcap_max = st.number_input(
        "최대 시총(원)",
        min_value=0.0,
        step=100_000_000.0,
        disabled=not custom_mode,
        key="mcap_max",
    )

    apply_value_min = st.checkbox("최소 20D 평균 거래대금(원) 적용", key="apply_value_min")
    value_min = st.number_input(
        "최소 20D 평균 거래대금(원)",
        min_value=0.0,

        step=100_000_000.0,
        disabled=not apply_value_min,
        key="value_min",
    )

with fundamental_tab:
    apply_pbr_max = st.checkbox("최대 PBR 적용", key="apply_pbr_max")
    pbr_max = st.number_input("최대 PBR", min_value=0.0, step=0.1, disabled=not apply_pbr_max, key="pbr_max")

    apply_roe_min = st.checkbox("최소 ROE proxy 적용", key="apply_roe_min")
    roe_min = st.number_input("최소 ROE proxy", step=0.01, disabled=not apply_roe_min, key="roe_min")

    apply_eps_positive = st.checkbox("EPS 흑자 기업만(적자 제외)", key="apply_eps_positive")

    apply_reserve_ratio_min = st.checkbox("최소 유보율(%) 적용", key="apply_reserve_ratio_min")
    reserve_ratio_min = st.number_input(
        "최소 유보율(%)", step=50.0, disabled=not apply_reserve_ratio_min, key="reserve_ratio_min"
    )

    apply_eps_cagr_5y = st.checkbox("최근 5년 EPS CAGR 조건 적용", key="apply_eps_cagr_5y")
    eps_cagr_5y_min = st.number_input(
        "최근 5년 EPS CAGR 최소",

        step=0.01,
        format="%.2f",
        disabled=not apply_eps_cagr_5y,
        key="eps_cagr_5y_min",
    )

    apply_eps_yoy_q = st.checkbox("최근 분기 EPS YoY 조건 적용", key="apply_eps_yoy_q")
    eps_yoy_q_min = st.number_input(
        "최근 분기 EPS YoY 최소",

        step=0.01,
        format="%.2f",
        disabled=not apply_eps_yoy_q,
        key="eps_yoy_q_min",
    )

with technical_tab:
    above_200ma = st.checkbox("200일선 위 조건 적용", key="above_200ma")

    apply_near_high = st.checkbox("현재가 / 52주 신고가 조건 적용", key="apply_near_high")
    near_high_min = st.number_input(
        "현재가 / 52주 신고가 최소",

        step=0.01,
        format="%.2f",
        disabled=not apply_near_high,
        key="near_high_min",
    )


active_filter_count = sum(
    [
        int(bool(ticker_list)),
        int(bool(mkt)),
        int(mcap_mode != "any"),
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
missing_tickers: list[str] = []
if ticker_list:
    available_tickers = set(filtered["ticker"].astype(str).str.strip().str.upper())
    missing_tickers = [ticker for ticker in ticker_list if ticker not in available_tickers]
    filtered = filtered[filtered["ticker"].astype(str).str.strip().str.upper().isin(ticker_list)]

if mkt:
    filtered = filtered[filtered["market"].isin(mkt)]
mcap_filter_min: float | None = None
mcap_filter_max: float | None = None
if mcap_mode == "bucket":
    mcap_filter_min, mcap_filter_max = MCAP_BUCKET_MAP[mcap_bucket]
elif mcap_mode == "custom":
    mcap_filter_min = mcap_min if mcap_min > 0 else None
    mcap_filter_max = mcap_max if mcap_max > 0 else None
    if mcap_filter_min is not None and mcap_filter_max is not None and mcap_filter_min >= mcap_filter_max:
        st.warning("Custom 시총 범위가 올바르지 않습니다. 최대 시총은 최소 시총보다 커야 합니다.")

if mcap_filter_min is not None:
    filtered = filtered[filtered["mcap"] >= mcap_filter_min]
if mcap_filter_max is not None:
    filtered = filtered[filtered["mcap"] < mcap_filter_max]
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
    key="sort_col",
)
ascending = st.checkbox("오름차순", key="ascending")
limit = st.slider("출력 개수", min_value=10, max_value=500, step=10, key="limit")

query_filter_state: dict[str, Any] = {}
for spec in FILTER_SPECS:
    serialized = _serialize_query_filter_value(spec, st.session_state.get(spec.name, spec.default))
    if serialized is not None:
        query_filter_state[spec.name] = serialized

if mcap_mode != "custom":
    query_filter_state.pop("mcap_min", None)
    query_filter_state.pop("mcap_max", None)
if mcap_mode != "bucket":
    query_filter_state.pop("mcap_bucket", None)

if mcap_mode == "custom":
    query_filter_state["mcap_min"] = str(mcap_min)
    query_filter_state["mcap_max"] = str(mcap_max)

_set_query_params(query_filter_state)

share_query_string = urlencode(query_filter_state, doseq=True)
share_link = f"?{share_query_string}" if share_query_string else ""
st.caption("필터 상태가 URL에 자동 반영됩니다. 링크를 복사해 동일한 조건을 공유할 수 있습니다.")
st.code(share_link or "(기본 필터 상태: 공유할 추가 파라미터 없음)", language="text")
st.button("공유 링크 복사", disabled=True, help="브라우저 주소창 URL을 복사해 공유하세요.")

filtered = filtered.sort_values(sort_col, ascending=ascending).head(limit)

if ticker_list:
    st.caption(f"티커 직접 입력: {len(ticker_list)}개 중 {len(ticker_list) - len(missing_tickers)}개 매칭")
    if missing_tickers:
        st.warning("snapshot에 없는 티커: " + ", ".join(missing_tickers))

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
