from __future__ import annotations

import re
import multiprocessing as mp
import queue
import time
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


@dataclass(frozen=True)
class FilterSpec:
    name: str
    ftype: str
    default: Any


FILTER_SPECS: list[FilterSpec] = [
    FilterSpec("ticker_input", "str", ""),
    FilterSpec("mkt", "list", []),
    FilterSpec("apply_mcap_min", "bool", False),
    FilterSpec("mcap_min", "float", 0.0),
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


def _safe_rerun() -> None:
    if hasattr(st, "rerun"):
        st.rerun()
        return
    st.experimental_rerun()


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


def _job_worker(db_path: str, job_type: str, asof_date: str | None, result_queue: mp.Queue) -> None:
    worker_pipeline = DailyBatchPipeline(Path(db_path))
    try:
        if job_type == "full_refresh":
            result = worker_pipeline.run(asof_date=None)
            result_queue.put({"status": "success", "job_type": job_type, "result": result.__dict__})
            return

        if job_type in {"snapshot_refresh", "auto_snapshot_sync"}:
            result = worker_pipeline.rebuild_snapshot_only(asof_date=asof_date)
            result_queue.put({"status": "success", "job_type": job_type, "result": result.__dict__})
            return

        if job_type == "reserve_refresh":
            updated_asof, updated_rows = worker_pipeline.update_reserve_ratio_only(asof_date=asof_date)
            snap_result = worker_pipeline.rebuild_snapshot_only(asof_date=updated_asof)
            result_queue.put(
                {
                    "status": "success",
                    "job_type": job_type,
                    "updated_asof": updated_asof,
                    "updated_rows": updated_rows,
                    "snapshot_rows": snap_result.snapshot,
                }
            )
            return

        result_queue.put({"status": "error", "job_type": job_type, "error": f"Unknown job type: {job_type}"})
    except Exception as exc:  # noqa: BLE001
        result_queue.put({"status": "error", "job_type": job_type, "error": str(exc)})


def _start_background_job(job_type: str, label: str, asof_date: str | None) -> None:
    active_job = st.session_state.get("active_job")
    if active_job and active_job["process"].is_alive():
        st.warning("다른 작업이 이미 실행 중입니다. 완료되거나 취소 후 다시 시도하세요.")
        return

    result_queue: mp.Queue = mp.Queue()
    process = mp.Process(target=_job_worker, args=(str(DB_PATH), job_type, asof_date, result_queue), daemon=True)
    process.start()
    st.session_state.active_job = {
        "job_type": job_type,
        "label": label,
        "asof_date": asof_date,
        "process": process,
        "result_queue": result_queue,
        "started_at": time.time(),
    }


def _poll_background_job() -> None:
    active_job = st.session_state.get("active_job")
    if not active_job:
        return

    process = active_job["process"]
    result_queue = active_job["result_queue"]
    if process.is_alive():
        return

    process.join(timeout=0.2)
    message: dict[str, Any]
    try:
        message = result_queue.get_nowait()
    except queue.Empty:
        message = {"status": "error", "job_type": active_job["job_type"], "error": "작업 결과를 읽지 못했습니다."}

    st.session_state.last_job_message = message
    st.session_state.active_job = None

    if message.get("status") == "success":
        if message.get("job_type") in {"full_refresh", "snapshot_refresh", "auto_snapshot_sync"}:
            result = message.get("result", {})
            st.session_state.asof = result.get("asof_date")
            if message.get("job_type") == "auto_snapshot_sync" and result.get("asof_date"):
                st.session_state.auto_snapshot_synced_for = result["asof_date"]
        elif message.get("job_type") == "reserve_refresh":
            st.session_state.asof = message.get("updated_asof")


def _render_active_job_panel() -> None:
    active_job = st.session_state.get("active_job")
    if not active_job:
        return

    elapsed = int(time.time() - active_job["started_at"])
    st.info(f"{active_job['label']} 실행 중... ({elapsed}초 경과)")
    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("진행상태 새로고침", key="refresh_active_job"):
            _safe_rerun()
    with c2:
        if st.button("작업 취소", type="secondary", key="cancel_active_job"):
            process = active_job["process"]
            if process.is_alive():
                process.terminate()
                process.join(timeout=1)
            st.session_state.active_job = None
            st.session_state.last_job_message = {
                "status": "cancelled",
                "job_type": active_job["job_type"],
                "message": f"{active_job['label']} 작업을 취소했습니다.",
            }
            _safe_rerun()


query_params = _get_query_params()
if "query_params_restored" not in st.session_state:
    st.session_state.query_parse_errors = []
    for spec in FILTER_SPECS:
        try:
            st.session_state[spec.name] = _parse_query_filter_value(spec, query_params)
        except ValueError:
            st.session_state[spec.name] = spec.default
            st.session_state.query_parse_errors.append(spec.name)
    st.session_state.query_params_restored = True

if st.session_state.get("query_parse_errors"):
    st.warning(
        "일부 URL 필터값을 복원하지 못해 기본값으로 대체했습니다: "
        + ", ".join(st.session_state.query_parse_errors)
    )

_poll_background_job()

last_job_message = st.session_state.pop("last_job_message", None)
if last_job_message:
    status = last_job_message.get("status")
    job_type = last_job_message.get("job_type")
    if status == "success":
        if job_type == "full_refresh":
            result = last_job_message.get("result", {})
            st.success(
                f"전체 수집 완료: {result.get('asof_date')} | 티커 {result.get('tickers', 0)}개 | "
                f"prices {result.get('prices', 0):,}건 | cap {result.get('cap', 0):,}건 | "
                f"fundamental {result.get('fundamental', 0):,}건 | snapshot {result.get('snapshot', 0):,}건"
            )
        elif job_type in {"snapshot_refresh", "auto_snapshot_sync"}:
            result = last_job_message.get("result", {})
            if job_type == "auto_snapshot_sync":
                st.success(f"최신 거래일 snapshot 자동 동기화 완료: {result.get('asof_date')}")
            else:
                st.success(f"스냅샷 재계산 완료: {result.get('asof_date')} | snapshot {result.get('snapshot', 0):,}건")
        elif job_type == "reserve_refresh":
            st.success(
                f"유보율 업데이트 완료: {last_job_message.get('updated_asof')} | "
                f"reserve_ratio {last_job_message.get('updated_rows', 0):,}건 | "
                f"snapshot {last_job_message.get('snapshot_rows', 0):,}건"
            )
    elif status == "cancelled":
        st.warning(last_job_message.get("message", "작업이 취소되었습니다."))
    else:
        st.error(f"작업 실패({job_type}): {last_job_message.get('error', '알 수 없는 오류')}")

if "asof" not in st.session_state:
    st.session_state.asof = repo.get_latest_price_date() or repo.get_latest_snapshot_date()

latest_price_date = repo.get_latest_price_date()
latest_snapshot_date = repo.get_latest_snapshot_date()
if latest_price_date and latest_price_date != latest_snapshot_date:
    auto_sync_target = latest_price_date
    active_job = st.session_state.get("active_job")
    if st.session_state.get("auto_snapshot_synced_for") != auto_sync_target and not active_job:
        _start_background_job("auto_snapshot_sync", "최신 거래일 snapshot 자동 동기화", auto_sync_target)
        _safe_rerun()

_render_active_job_panel()

c1, c2, c3 = st.columns([1, 1, 1])
with c1:
    refresh_full = st.button("전체 수집 + 스냅샷", type="primary")
with c2:
    refresh_snapshot = st.button("스냅샷만 재계산", help="이미 수집된 DB 데이터로 snapshot만 다시 계산")
with c3:
    refresh_reserve = st.button("유보율만 업데이트", help="네이버 크롤링으로 최신 유보율만 업데이트")

if refresh_full:
    _start_background_job("full_refresh", "전체 수집 + 스냅샷", None)
    _safe_rerun()

if refresh_snapshot:
    _start_background_job("snapshot_refresh", "스냅샷 재계산", repo.get_latest_price_date())
    _safe_rerun()

if refresh_reserve:
    _start_background_job("reserve_refresh", "유보율 업데이트 + 스냅샷 재계산", repo.get_latest_price_date())
    _safe_rerun()

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

st.markdown("### 조건 선택")
st.caption("원하는 조건만 체크해서 적용하세요. 체크하지 않은 조건은 필터에 사용되지 않습니다.")

descriptive_tab, fundamental_tab, technical_tab = st.tabs(["Descriptive", "Fundamental", "Technical"])

with descriptive_tab:
    ticker_input = st.text_input("티커 직접 입력", help="콤마(,) 또는 공백으로 여러 티커를 입력하세요.", key="ticker_input")

    raw_tickers = [token.strip().upper() for token in re.split(r"[\s,]+", ticker_input or "") if token.strip()]
    ticker_list = list(dict.fromkeys(raw_tickers))

    mkt = st.multiselect("시장", sorted(base["market"].dropna().unique().tolist()), key="mkt")

    apply_mcap_min = st.checkbox("최소 시총(원) 적용", key="apply_mcap_min")
    mcap_min = st.number_input(
        "최소 시총(원)",
        min_value=0.0,

        step=100_000_000.0,
        disabled=not apply_mcap_min,
        key="mcap_min",
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
missing_tickers: list[str] = []
if ticker_list:
    available_tickers = set(filtered["ticker"].astype(str).str.strip().str.upper())
    missing_tickers = [ticker for ticker in ticker_list if ticker not in available_tickers]
    filtered = filtered[filtered["ticker"].astype(str).str.strip().str.upper().isin(ticker_list)]

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
    key="sort_col",
)
ascending = st.checkbox("오름차순", key="ascending")
limit = st.slider("출력 개수", min_value=10, max_value=500, step=10, key="limit")

query_filter_state: dict[str, Any] = {}
for spec in FILTER_SPECS:
    serialized = _serialize_query_filter_value(spec, st.session_state.get(spec.name, spec.default))
    if serialized is not None:
        query_filter_state[spec.name] = serialized
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
