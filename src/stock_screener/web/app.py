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
st.caption("기본 asof = 최신 거래일(가격 데이터 기준), 해당 거래일 snapshot이 없으면 재계산이 필요합니다.")


@dataclass(frozen=True)
class FilterSpec:
    name: str
    ftype: str
    default: Any


FILTER_SPECS: list[FilterSpec] = [
    FilterSpec("ticker_input", "str", ""),
    FilterSpec("mkt", "list", []),
    FilterSpec("mcap_filter_mode", "str", "Any"),
    FilterSpec("mcap_bucket", "str", "전체"),
    FilterSpec("mcap_min_custom", "float", 0.0),
    FilterSpec("mcap_max_custom", "float", 0.0),
    FilterSpec("price_filter_mode", "str", "Any"),
    FilterSpec("price_bucket", "str", "전체"),
    FilterSpec("price_min_custom", "float", 0.0),
    FilterSpec("price_max_custom", "float", 0.0),
    FilterSpec("div_filter_mode", "str", "Any"),
    FilterSpec("div_bucket", "str", "전체"),
    FilterSpec("div_min_custom", "float", 0.0),
    FilterSpec("div_max_custom", "float", 0.0),
    FilterSpec("value_filter_mode", "str", "Any"),
    FilterSpec("value_bucket", "str", "전체"),
    FilterSpec("value_min_custom", "float", 0.0),
    FilterSpec("value_max_custom", "float", 0.0),
    FilterSpec("relvol_filter_mode", "str", "Any"),
    FilterSpec("relvol_bucket", "str", "전체"),
    FilterSpec("relvol_min_custom", "float", 0.0),
    FilterSpec("relvol_max_custom", "float", 0.0),
    FilterSpec("momentum_metric", "str", "ret_3m"),
    FilterSpec("momentum_filter_mode", "str", "Any"),
    FilterSpec("momentum_bucket", "str", "전체"),
    FilterSpec("momentum_min_custom", "float", 0.0),
    FilterSpec("momentum_max_custom", "float", 0.0),
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

MCAP_BUCKETS: dict[str, tuple[float | None, float | None]] = {
    "전체": (None, None),
    "Nano (<500억)": (None, 50_000_000_000),
    "Micro (500억~3,000억)": (50_000_000_000, 300_000_000_000),
    "Small (3,000억~2조)": (300_000_000_000, 2_000_000_000_000),
    "Mid (2조~10조)": (2_000_000_000_000, 10_000_000_000_000),
    "Large (10조~50조)": (10_000_000_000_000, 50_000_000_000_000),
    "Mega (50조 이상)": (50_000_000_000_000, None),
    "+Large (10조 이상)": (10_000_000_000_000, None),
    "+Mid (2조 이상)": (2_000_000_000_000, None),
    "-Small (2조 미만)": (None, 2_000_000_000_000),
}

PRICE_BUCKETS: dict[str, tuple[float | None, float | None]] = {
    "전체": (None, None),
    "1,000원 미만": (None, 1_000),
    "1,000원~5,000원": (1_000, 5_000),
    "5,000원~10,000원": (5_000, 10_000),
    "10,000원~50,000원": (10_000, 50_000),
    "50,000원~100,000원": (50_000, 100_000),
    "100,000원 이상": (100_000, None),
}

DIV_BUCKETS: dict[str, tuple[float | None, float | None]] = {
    "전체": (None, None),
    "무배당(0%)": (0.0, 0.0),
    "배당주(0% 초과)": (0.000001, None),
    "1% 이상": (1.0, None),
    "2% 이상": (2.0, None),
    "3% 이상": (3.0, None),
    "5% 이상": (5.0, None),
}

VALUE_BUCKETS: dict[str, tuple[float | None, float | None]] = {
    "전체": (None, None),
    "1억 미만": (None, 100_000_000),
    "1억~5억": (100_000_000, 500_000_000),
    "5억~20억": (500_000_000, 2_000_000_000),
    "20억 이상": (2_000_000_000, None),
}

RELVOL_BUCKETS: dict[str, tuple[float | None, float | None]] = {
    "전체": (None, None),
    "0.5x 이하": (None, 0.5),
    "1x 이상": (1.0, None),
    "2x 이상": (2.0, None),
    "3x 이상": (3.0, None),
}

# Keep explicit mode constants to avoid key drift and to support legacy state migration.
MCAP_MODES = ("Any", "구간 선택", "직접 입력")
PRICE_MODES = ("Any", "구간 선택", "직접 입력")
DIV_MODES = ("Any", "구간 선택", "직접 입력")
VALUE_MODES = ("Any", "구간 선택", "직접 입력")
RELVOL_MODES = ("Any", "구간 선택", "직접 입력")
MOMENTUM_MODES = ("Any", "구간 선택", "직접 입력")
MOMENTUM_METRICS: dict[str, str] = {
    "ret_3m": "3개월 수익률(ret_3m)",
    "ret_6m": "6개월 수익률(ret_6m)",
    "ret_1y": "1년 수익률(ret_1y)",
    "near_52w_high_ratio": "52주 신고가 근접도(near_52w_high_ratio)",
}
MOMENTUM_BUCKETS: dict[str, tuple[float | None, float | None]] = {
    "전체": (None, None),
    "-20% 이하": (None, -0.2),
    "-10% 이하": (None, -0.1),
    "0% 이상": (0.0, None),
    "+10% 이상": (0.1, None),
    "+20% 이상": (0.2, None),
    "+50% 이상": (0.5, None),
}


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


def _reset_all_filters() -> None:
    for spec in FILTER_SPECS:
        st.session_state[spec.name] = spec.default


def _format_range_summary(mode: str, bucket: str, min_custom: float, max_custom: float) -> str:
    if mode == "구간 선택":
        return bucket
    if mode == "직접 입력":
        min_text = f"{min_custom:g}" if min_custom else "-"
        max_text = f"{max_custom:g}" if max_custom else "-"
        return f"{min_text}~{max_text}"
    return "Any"


query_params = _get_query_params()
if "query_params_restored" not in st.session_state:
    st.session_state.query_parse_errors = []

    # Backward compatibility: migrate legacy query keys before restoring filter specs.
    legacy_query_key_map = {
        "mcap_mode": "mcap_filter_mode",
        "price_mode": "price_filter_mode",
        "div_mode": "div_filter_mode",
        "momentum_mode": "momentum_filter_mode",
        "momentum_col": "momentum_metric",
    }
    for legacy_key, new_key in legacy_query_key_map.items():
        if new_key not in query_params and legacy_key in query_params:
            query_params[new_key] = query_params[legacy_key]

    if query_params.get("apply_value_min") == "1":
        if "value_filter_mode" not in query_params:
            query_params["value_filter_mode"] = "직접 입력"
        if "value_min_custom" not in query_params and "value_min" in query_params:
            query_params["value_min_custom"] = query_params["value_min"]

    for spec in FILTER_SPECS:
        try:
            st.session_state[spec.name] = _parse_query_filter_value(spec, query_params)
        except ValueError:
            st.session_state[spec.name] = spec.default
            st.session_state.query_parse_errors.append(spec.name)

    if st.session_state.get("mcap_filter_mode") not in MCAP_MODES:
        st.session_state.mcap_filter_mode = "Any"
        st.session_state.query_parse_errors.append("mcap_filter_mode")
    if st.session_state.get("price_filter_mode") not in PRICE_MODES:
        st.session_state.price_filter_mode = "Any"
        st.session_state.query_parse_errors.append("price_filter_mode")
    if st.session_state.get("div_filter_mode") not in DIV_MODES:
        st.session_state.div_filter_mode = "Any"
        st.session_state.query_parse_errors.append("div_filter_mode")
    if st.session_state.get("value_filter_mode") not in VALUE_MODES:
        st.session_state.value_filter_mode = "Any"
        st.session_state.query_parse_errors.append("value_filter_mode")
    if st.session_state.get("relvol_filter_mode") not in RELVOL_MODES:
        st.session_state.relvol_filter_mode = "Any"
        st.session_state.query_parse_errors.append("relvol_filter_mode")
    if st.session_state.get("momentum_filter_mode") not in MOMENTUM_MODES:
        st.session_state.momentum_filter_mode = "Any"
        st.session_state.query_parse_errors.append("momentum_filter_mode")

    if st.session_state.get("mcap_bucket") not in MCAP_BUCKETS:
        st.session_state.mcap_bucket = "전체"
        st.session_state.query_parse_errors.append("mcap_bucket")
    if st.session_state.get("price_bucket") not in PRICE_BUCKETS:
        st.session_state.price_bucket = "전체"
        st.session_state.query_parse_errors.append("price_bucket")
    if st.session_state.get("div_bucket") not in DIV_BUCKETS:
        st.session_state.div_bucket = "전체"
        st.session_state.query_parse_errors.append("div_bucket")
    if st.session_state.get("value_bucket") not in VALUE_BUCKETS:
        st.session_state.value_bucket = "전체"
        st.session_state.query_parse_errors.append("value_bucket")
    if st.session_state.get("relvol_bucket") not in RELVOL_BUCKETS:
        st.session_state.relvol_bucket = "전체"
        st.session_state.query_parse_errors.append("relvol_bucket")
    if st.session_state.get("momentum_bucket") not in MOMENTUM_BUCKETS:
        st.session_state.momentum_bucket = "전체"
        st.session_state.query_parse_errors.append("momentum_bucket")
    if st.session_state.get("momentum_metric") not in MOMENTUM_METRICS:
        st.session_state.momentum_metric = "ret_3m"
        st.session_state.query_parse_errors.append("momentum_metric")

    st.session_state.query_params_restored = True

if st.session_state.get("query_parse_errors"):
    st.warning(
        "일부 URL 필터값을 복원하지 못해 기본값으로 대체했습니다: "
        + ", ".join(st.session_state.query_parse_errors)
    )

if st.session_state.get("mcap_filter_mode") not in MCAP_MODES:
    st.session_state.mcap_filter_mode = "Any"
if st.session_state.get("price_filter_mode") not in PRICE_MODES:
    st.session_state.price_filter_mode = "Any"
if st.session_state.get("div_filter_mode") not in DIV_MODES:
    st.session_state.div_filter_mode = "Any"
if st.session_state.get("value_filter_mode") not in VALUE_MODES:
    st.session_state.value_filter_mode = "Any"
if st.session_state.get("relvol_filter_mode") not in RELVOL_MODES:
    st.session_state.relvol_filter_mode = "Any"
if st.session_state.get("momentum_filter_mode") not in MOMENTUM_MODES:
    st.session_state.momentum_filter_mode = "Any"

if st.session_state.get("mcap_bucket") not in MCAP_BUCKETS:
    st.session_state.mcap_bucket = "전체"
if st.session_state.get("price_bucket") not in PRICE_BUCKETS:
    st.session_state.price_bucket = "전체"
if st.session_state.get("div_bucket") not in DIV_BUCKETS:
    st.session_state.div_bucket = "전체"
if st.session_state.get("value_bucket") not in VALUE_BUCKETS:
    st.session_state.value_bucket = "전체"
if st.session_state.get("relvol_bucket") not in RELVOL_BUCKETS:
    st.session_state.relvol_bucket = "전체"
if st.session_state.get("momentum_bucket") not in MOMENTUM_BUCKETS:
    st.session_state.momentum_bucket = "전체"
if st.session_state.get("momentum_metric") not in MOMENTUM_METRICS:
    st.session_state.momentum_metric = "ret_3m"

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
    st.warning(
        "해당 거래일 스냅샷이 없습니다. '스냅샷만 재계산' 버튼으로 스냅샷 재계산이 필요합니다."
    )
    st.stop()

st.subheader(f"Snapshot as of {asof}")
st.write(f"현재 snapshot 종목 수: **{len(base):,}개**")

st.markdown("### 조건 선택")
st.caption("조건은 Any + 임계치 방식으로 설정되며, 계산 불가한 항목은 자동 비활성화됩니다.")

avg_value_available = "avg_value_20d" in base.columns and base["avg_value_20d"].notna().any()
relative_value_available = "relative_value" in base.columns and base["relative_value"].notna().any()
available_momentum_metrics = [metric for metric in MOMENTUM_METRICS if metric in base.columns and base[metric].notna().any()]
momentum_available = bool(available_momentum_metrics)

def _active_filter_count_from_state() -> int:
    return sum(
        [
            int(bool([token.strip() for token in re.split(r"[\s,]+", st.session_state.get("ticker_input", "") or "") if token.strip()])),
            int(bool(st.session_state.get("mkt", []))),
            int(st.session_state.get("mcap_filter_mode", "Any") != "Any"),
            int(st.session_state.get("price_filter_mode", "Any") != "Any"),
            int(st.session_state.get("div_filter_mode", "Any") != "Any"),
            int(relative_value_available and st.session_state.get("relvol_filter_mode", "Any") != "Any"),
            int(momentum_available and st.session_state.get("momentum_filter_mode", "Any") != "Any"),
            int(avg_value_available and st.session_state.get("value_filter_mode", "Any") != "Any"),
            int(bool(st.session_state.get("apply_pbr_max", False))),
            int(bool(st.session_state.get("apply_reserve_ratio_min", False))),
            int(bool(st.session_state.get("apply_roe_min", False))),
            int(bool(st.session_state.get("apply_eps_positive", False))),
            int(bool(st.session_state.get("above_200ma", False))),
            int(bool(st.session_state.get("apply_eps_cagr_5y", False))),
            int(bool(st.session_state.get("apply_eps_yoy_q", False))),
            int(bool(st.session_state.get("apply_near_high", False))),
        ]
    )

header_cols = st.columns([4, 1])
with header_cols[0]:
    st.caption(f"Active filters: {_active_filter_count_from_state()}개")
with header_cols[1]:
    if st.button("초기화", key="reset_all_filters"):
        _reset_all_filters()
        _safe_rerun()

descriptive_tab, fundamental_tab, technical_tab = st.tabs(["Descriptive", "Fundamental", "Technical"])

with descriptive_tab:
    st.markdown("#### 시장")
    mkt_cols = st.columns(4)
    with mkt_cols[0]:
        ticker_input = st.text_input("티커", help="콤마(,) 또는 공백으로 여러 티커를 입력하세요.", key="ticker_input")
    with mkt_cols[1]:
        mkt = st.multiselect("시장", sorted(base["market"].dropna().unique().tolist()), key="mkt")

    raw_tickers = [token.strip().upper() for token in re.split(r"[\s,]+", ticker_input or "") if token.strip()]
    ticker_list = list(dict.fromkeys(raw_tickers))

    st.markdown("#### 시가총액")
    mcap_cols = st.columns([1, 1, 1, 1, 1])
    with mcap_cols[0]:
        mcap_filter_mode = st.selectbox("시총", list(MCAP_MODES), key="mcap_filter_mode")
    with mcap_cols[1]:
        mcap_bucket = st.selectbox(
            "시총 구간",
            list(MCAP_BUCKETS.keys()),
            key="mcap_bucket",
            disabled=mcap_filter_mode != "구간 선택",
        )
    with mcap_cols[2]:
        mcap_min_custom = st.number_input(
            "최소",
            min_value=0.0,
            step=100_000_000.0,
            key="mcap_min_custom",
            disabled=mcap_filter_mode != "직접 입력",
            help="단위: 원",
        )
    with mcap_cols[3]:
        mcap_max_custom = st.number_input(
            "최대",
            min_value=0.0,
            step=100_000_000.0,
            key="mcap_max_custom",
            disabled=mcap_filter_mode != "직접 입력",
            help="단위: 원",
        )

    st.markdown("#### 가격")
    price_cols = st.columns([1, 1, 1, 1, 1])
    with price_cols[0]:
        price_filter_mode = st.selectbox("가격", list(PRICE_MODES), key="price_filter_mode")
    with price_cols[1]:
        price_bucket = st.selectbox(
            "가격 구간",
            list(PRICE_BUCKETS.keys()),
            key="price_bucket",
            disabled=price_filter_mode != "구간 선택",
        )
    with price_cols[2]:
        price_min_custom = st.number_input(
            "최소",
            min_value=0.0,
            step=100.0,
            key="price_min_custom",
            disabled=price_filter_mode != "직접 입력",
            help="단위: 원",
        )
    with price_cols[3]:
        price_max_custom = st.number_input(
            "최대",
            min_value=0.0,
            step=100.0,
            key="price_max_custom",
            disabled=price_filter_mode != "직접 입력",
            help="단위: 원",
        )

    st.markdown("#### 배당")
    div_cols = st.columns([1, 1, 1, 1, 1])
    with div_cols[0]:
        div_filter_mode = st.selectbox("배당", list(DIV_MODES), key="div_filter_mode")
    with div_cols[1]:
        div_bucket = st.selectbox(
            "배당 구간",
            list(DIV_BUCKETS.keys()),
            key="div_bucket",
            disabled=div_filter_mode != "구간 선택",
        )
    with div_cols[2]:
        div_min_custom = st.number_input(
            "최소",
            min_value=0.0,
            step=0.1,
            key="div_min_custom",
            disabled=div_filter_mode != "직접 입력",
            help="단위: %",
        )
    with div_cols[3]:
        div_max_custom = st.number_input(
            "최대",
            min_value=0.0,
            step=0.1,
            key="div_max_custom",
            disabled=div_filter_mode != "직접 입력",
            help="단위: %",
        )

    st.markdown("#### 거래대금")
    value_cols = st.columns([1, 1, 1, 1, 1])
    with value_cols[0]:
        value_filter_mode = st.selectbox(
            "20D 거래대금",
            list(VALUE_MODES),
            key="value_filter_mode",
            disabled=not avg_value_available,
        )
    with value_cols[1]:
        value_bucket = st.selectbox(
            "거래대금 구간",
            list(VALUE_BUCKETS.keys()),
            key="value_bucket",
            disabled=(not avg_value_available) or (value_filter_mode != "구간 선택"),
        )
    with value_cols[2]:
        value_min_custom = st.number_input(
            "최소",
            min_value=0.0,
            step=100_000_000.0,
            key="value_min_custom",
            disabled=(not avg_value_available) or (value_filter_mode != "직접 입력"),
            help="단위: 원",
        )
    with value_cols[3]:
        value_max_custom = st.number_input(
            "최대",
            min_value=0.0,
            step=100_000_000.0,
            key="value_max_custom",
            disabled=(not avg_value_available) or (value_filter_mode != "직접 입력"),
            help="단위: 원",
        )
    if not avg_value_available:
        st.session_state.value_filter_mode = "Any"
        value_filter_mode = "Any"
        st.info("평균 거래대금 데이터가 없어 해당 필터를 비활성화했습니다.")

    st.markdown("#### 상대강도")
    rel_cols = st.columns([1, 1, 1, 1, 1])
    with rel_cols[0]:
        relvol_filter_mode = st.selectbox(
            "상대거래대금",
            list(RELVOL_MODES),
            key="relvol_filter_mode",
            disabled=not relative_value_available,
            help="relative_value 배수",
        )
    with rel_cols[1]:
        relvol_bucket = st.selectbox(
            "거래강도 구간",
            list(RELVOL_BUCKETS.keys()),
            key="relvol_bucket",
            disabled=(not relative_value_available) or (relvol_filter_mode != "구간 선택"),
        )
    with rel_cols[2]:
        relvol_min_custom = st.number_input(
            "최소",
            min_value=0.0,
            step=0.1,
            key="relvol_min_custom",
            disabled=(not relative_value_available) or (relvol_filter_mode != "직접 입력"),
            help="단위: x",
        )
    with rel_cols[3]:
        relvol_max_custom = st.number_input(
            "최대",
            min_value=0.0,
            step=0.1,
            key="relvol_max_custom",
            disabled=(not relative_value_available) or (relvol_filter_mode != "직접 입력"),
            help="단위: x",
        )
    if not relative_value_available:
        st.session_state.relvol_filter_mode = "Any"
        relvol_filter_mode = "Any"
        st.info("relative_value 데이터가 없어 해당 필터를 비활성화했습니다.")

    momentum_cols = st.columns([1, 1, 1, 1, 1])
    with momentum_cols[0]:
        momentum_metric = st.selectbox(
            "강도기준",
            list(MOMENTUM_METRICS.keys()),
            key="momentum_metric",
            format_func=lambda key: MOMENTUM_METRICS.get(key, key),
            disabled=not momentum_available,
        )
    with momentum_cols[1]:
        momentum_filter_mode = st.selectbox(
            "강도필터",
            list(MOMENTUM_MODES),
            key="momentum_filter_mode",
            disabled=not momentum_available,
        )
    with momentum_cols[2]:
        momentum_bucket = st.selectbox(
            "강도구간",
            list(MOMENTUM_BUCKETS.keys()),
            key="momentum_bucket",
            disabled=(not momentum_available) or (momentum_filter_mode != "구간 선택"),
        )
    with momentum_cols[3]:
        momentum_min_custom = st.number_input(
            "최소",
            step=0.01,
            format="%.2f",
            key="momentum_min_custom",
            disabled=(not momentum_available) or (momentum_filter_mode != "직접 입력"),
            help="수익률/비율 값",
        )
    with momentum_cols[4]:
        momentum_max_custom = st.number_input(
            "최대",
            step=0.01,
            format="%.2f",
            key="momentum_max_custom",
            disabled=(not momentum_available) or (momentum_filter_mode != "직접 입력"),
            help="수익률/비율 값",
        )
    if momentum_metric not in available_momentum_metrics and available_momentum_metrics:
        st.session_state.momentum_metric = available_momentum_metrics[0]
        momentum_metric = st.session_state.momentum_metric
    if not momentum_available:
        st.session_state.momentum_filter_mode = "Any"
        st.info("모멘텀 데이터가 없어 해당 필터를 비활성화했습니다.")

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


filtered = base.copy()
missing_tickers: list[str] = []
if ticker_list:
    available_tickers = set(filtered["ticker"].astype(str).str.strip().str.upper())
    missing_tickers = [ticker for ticker in ticker_list if ticker not in available_tickers]
    filtered = filtered[filtered["ticker"].astype(str).str.strip().str.upper().isin(ticker_list)]

if mkt:
    filtered = filtered[filtered["market"].isin(mkt)]
if mcap_filter_mode == "구간 선택":
    mcap_min, mcap_max = MCAP_BUCKETS.get(mcap_bucket, (None, None))
    if mcap_min is not None:
        filtered = filtered[filtered["mcap"] >= mcap_min]
    if mcap_max is not None:
        filtered = filtered[filtered["mcap"] < mcap_max]
elif mcap_filter_mode == "직접 입력":
    if mcap_min_custom > 0:
        filtered = filtered[filtered["mcap"] >= mcap_min_custom]
    if mcap_max_custom > 0:
        filtered = filtered[filtered["mcap"] <= mcap_max_custom]

if price_filter_mode == "구간 선택":
    price_min, price_max = PRICE_BUCKETS.get(price_bucket, (None, None))
    if price_min is not None:
        filtered = filtered[filtered["close"] >= price_min]
    if price_max is not None:
        filtered = filtered[filtered["close"] < price_max]
elif price_filter_mode == "직접 입력":
    if price_min_custom > 0:
        filtered = filtered[filtered["close"] >= price_min_custom]
    if price_max_custom > 0:
        filtered = filtered[filtered["close"] <= price_max_custom]

if div_filter_mode == "구간 선택":
    div_min, div_max = DIV_BUCKETS.get(div_bucket, (None, None))
    if div_bucket == "무배당(0%)":
        filtered = filtered[filtered["div"].fillna(0.0) == 0.0]
    else:
        filtered = filtered[filtered["div"].notna()]
        if div_min is not None:
            filtered = filtered[filtered["div"] >= div_min]
        if div_max is not None:
            filtered = filtered[filtered["div"] <= div_max]
elif div_filter_mode == "직접 입력":
    filtered = filtered[filtered["div"].notna()]
    if div_min_custom > 0:
        filtered = filtered[filtered["div"] >= div_min_custom]
    if div_max_custom > 0:
        filtered = filtered[filtered["div"] <= div_max_custom]

if relvol_filter_mode == "구간 선택":
    relvol_min, relvol_max = RELVOL_BUCKETS.get(relvol_bucket, (None, None))
    filtered = filtered[filtered["relative_value"].notna()]
    if relvol_min is not None:
        filtered = filtered[filtered["relative_value"] >= relvol_min]
    if relvol_max is not None:
        filtered = filtered[filtered["relative_value"] <= relvol_max]
elif relvol_filter_mode == "직접 입력":
    filtered = filtered[filtered["relative_value"].notna()]
    if relvol_min_custom > 0:
        filtered = filtered[filtered["relative_value"] >= relvol_min_custom]
    if relvol_max_custom > 0:
        filtered = filtered[filtered["relative_value"] <= relvol_max_custom]

if momentum_available and momentum_filter_mode == "구간 선택":
    momentum_min, momentum_max = MOMENTUM_BUCKETS.get(momentum_bucket, (None, None))
    filtered = filtered[filtered[momentum_metric].notna()]
    if momentum_min is not None:
        filtered = filtered[filtered[momentum_metric] >= momentum_min]
    if momentum_max is not None:
        filtered = filtered[filtered[momentum_metric] <= momentum_max]
elif momentum_available and momentum_filter_mode == "직접 입력":
    filtered = filtered[filtered[momentum_metric].notna()]
    if momentum_min_custom != 0:
        filtered = filtered[filtered[momentum_metric] >= momentum_min_custom]
    if momentum_max_custom != 0:
        filtered = filtered[filtered[momentum_metric] <= momentum_max_custom]

if avg_value_available and value_filter_mode == "구간 선택":
    value_min, value_max = VALUE_BUCKETS.get(value_bucket, (None, None))
    filtered = filtered[filtered["avg_value_20d"].notna()]
    if value_min is not None:
        filtered = filtered[filtered["avg_value_20d"] >= value_min]
    if value_max is not None:
        filtered = filtered[filtered["avg_value_20d"] < value_max]
elif avg_value_available and value_filter_mode == "직접 입력":
    filtered = filtered[filtered["avg_value_20d"].notna()]
    if value_min_custom > 0:
        filtered = filtered[filtered["avg_value_20d"] >= value_min_custom]
    if value_max_custom > 0:
        filtered = filtered[filtered["avg_value_20d"] <= value_max_custom]
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
    [
        "mcap", "pbr", "reserve_ratio", "roe_proxy", "ret_3m", "ret_6m", "ret_1y", "near_52w_high_ratio", "div",
        "avg_value_20d", "current_value", "relative_value", "eps_cagr_5y", "eps_yoy_q",
    ],
    key="sort_col",
)
ascending = st.checkbox("오름차순", key="ascending")
limit = st.slider("출력 개수", min_value=10, max_value=500, step=10, key="limit")

query_filter_state: dict[str, Any] = {}
for spec in FILTER_SPECS:
    serialized = _serialize_query_filter_value(spec, st.session_state.get(spec.name, spec.default))
    if serialized is not None:
        query_filter_state[spec.name] = serialized

if mcap_filter_mode != "직접 입력":
    query_filter_state.pop("mcap_min_custom", None)
    query_filter_state.pop("mcap_max_custom", None)
if mcap_filter_mode != "구간 선택":
    query_filter_state.pop("mcap_bucket", None)

if price_filter_mode != "직접 입력":
    query_filter_state.pop("price_min_custom", None)
    query_filter_state.pop("price_max_custom", None)
if price_filter_mode != "구간 선택":
    query_filter_state.pop("price_bucket", None)

if div_filter_mode != "직접 입력":
    query_filter_state.pop("div_min_custom", None)
    query_filter_state.pop("div_max_custom", None)
if div_filter_mode != "구간 선택":
    query_filter_state.pop("div_bucket", None)

if value_filter_mode != "직접 입력":
    query_filter_state.pop("value_min_custom", None)
    query_filter_state.pop("value_max_custom", None)
if value_filter_mode != "구간 선택":
    query_filter_state.pop("value_bucket", None)

if relvol_filter_mode != "직접 입력":
    query_filter_state.pop("relvol_min_custom", None)
    query_filter_state.pop("relvol_max_custom", None)
if relvol_filter_mode != "구간 선택":
    query_filter_state.pop("relvol_bucket", None)

if momentum_filter_mode != "직접 입력":
    query_filter_state.pop("momentum_min_custom", None)
    query_filter_state.pop("momentum_max_custom", None)
if momentum_filter_mode != "구간 선택":
    query_filter_state.pop("momentum_bucket", None)
if momentum_filter_mode == "Any":
    query_filter_state.pop("momentum_metric", None)

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

condition_summaries: list[str] = []
if ticker_list:
    condition_summaries.append(f"티커 {len(ticker_list)}")
if mkt:
    condition_summaries.append(f"시장 {', '.join(mkt)}")
if mcap_filter_mode != "Any":
    condition_summaries.append(f"시총 {_format_range_summary(mcap_filter_mode, mcap_bucket, mcap_min_custom, mcap_max_custom)}")
if price_filter_mode != "Any":
    condition_summaries.append(f"가격 {_format_range_summary(price_filter_mode, price_bucket, price_min_custom, price_max_custom)}")
if div_filter_mode != "Any":
    condition_summaries.append(f"배당 {_format_range_summary(div_filter_mode, div_bucket, div_min_custom, div_max_custom)}")
if avg_value_available and value_filter_mode != "Any":
    condition_summaries.append(
        f"20D 거래대금 {_format_range_summary(value_filter_mode, value_bucket, value_min_custom, value_max_custom)}"
    )
if relative_value_available and relvol_filter_mode != "Any":
    condition_summaries.append(
        f"상대거래대금 {_format_range_summary(relvol_filter_mode, relvol_bucket, relvol_min_custom, relvol_max_custom)}"
    )
if momentum_available and momentum_filter_mode != "Any":
    condition_summaries.append(
        f"{MOMENTUM_METRICS.get(momentum_metric, momentum_metric)} {_format_range_summary(momentum_filter_mode, momentum_bucket, momentum_min_custom, momentum_max_custom)}"
    )

if condition_summaries:
    st.caption("현재 조건: " + " • ".join(condition_summaries))

show_cols = [
    "ticker", "name", "market", "close", "mcap", "avg_value_20d", "current_value", "relative_value", "pbr", "reserve_ratio", "per", "div", "dps",
    "eps", "bps", "roe_proxy", "eps_positive", "ret_3m", "ret_6m", "ret_1y", "dist_sma200", "pos_52w",
    "near_52w_high_ratio", "eps_cagr_5y", "eps_yoy_q",
]
st.dataframe(filtered[show_cols], width="stretch", hide_index=True)

csv = filtered[show_cols].to_csv(index=False).encode("utf-8-sig")
st.download_button("CSV 다운로드", data=csv, file_name=f"screener_{asof}.csv", mime="text/csv")
