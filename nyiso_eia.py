# %%writefile /content/Experiments_for-students/nyiso_eia.py
# nyiso_eia.py
import io
import os
from pathlib import Path

import requests
import numpy as np
import pandas as pd

DEFAULT_URL = "https://www.eia.gov/electricity/wholesalemarkets/csv/nyiso_load_act_hr_2024.csv"


def _detect_header_row(file_content: str, max_lines: int = 20) -> int:
    lines = file_content.splitlines()
    header_row_index = 0
    for i, line in enumerate(lines[:max_lines]):
        if ("Time" in line) or ("Period" in line) or ("Date" in line):
            header_row_index = i
            break
    return header_row_index


def load_eia_csv_with_auto_header(url: str) -> pd.DataFrame:
    resp = requests.get(url)
    resp.raise_for_status()
    file_content = resp.text

    header_row_index = _detect_header_row(file_content)
    df = pd.read_csv(io.StringIO(file_content), header=header_row_index)
    df.columns = df.columns.str.strip()
    return df


def make_power_temp_solar_frames(
    url: str,
    start: str = "2024-07-01",
    end: str = "2024-07-07",
    seed: int | None = 42,
):
    """파일 저장 없이 power/temp/solar DataFrame만 생성해서 반환"""
    if seed is not None:
        np.random.seed(seed)

    df_eia = load_eia_csv_with_auto_header(url)

    target_date_cols = [
        c for c in df_eia.columns
        if ("time" in c.lower()) or ("period" in c.lower()) or ("date" in c.lower())
    ]
    target_load_cols = [
        c for c in df_eia.columns
        if ("load" in c.lower()) or ("value" in c.lower())
    ]

    if not target_date_cols or not target_load_cols:
        raise ValueError(f"날짜 또는 부하 컬럼 감지 실패. 현재 컬럼: {list(df_eia.columns)}")

    date_col = target_date_cols[0]
    load_col = target_load_cols[0]

    df_eia[date_col] = pd.to_datetime(df_eia[date_col])
    df_eia = df_eia.sort_values(date_col)

    mask = (df_eia[date_col] >= start) & (df_eia[date_col] <= end)
    df_sample = df_eia.loc[mask].copy()

    if df_sample.empty:
        raise ValueError(
            f"{start}~{end} 구간 데이터가 비었습니다. 범위: {df_eia[date_col].min()} ~ {df_eia[date_col].max()}"
        )

    # -------------------------
    # power frame (Date/Hour 기반)
    # -------------------------
    df_sample["Date"] = df_sample[date_col].dt.strftime("%Y-%m-%d")
    df_sample["Hour"] = df_sample[date_col].dt.hour

    df_sample["NYC"] = df_sample[load_col] * 0.45
    df_sample["NJ"]  = df_sample[load_col] * 0.30
    df_sample["CT"]  = df_sample[load_col] * 0.25

    power = df_sample[["Date", "Hour", "NYC", "NJ", "CT"]].reset_index(drop=True)

    # -------------------------
    # temp frame
    # -------------------------
    dates = pd.to_datetime(df_sample[date_col].values)
    n = len(dates)
    daily_temp = np.sin((power["Hour"] - 14) * 2 * np.pi / 24) * 5
    temp_data = 28 + daily_temp + np.random.normal(0, 1, n)
    temp = pd.DataFrame({"일시": dates, "기온(°C)": temp_data.round(1)})

    # -------------------------
    # solar frame
    # -------------------------
    solar_gen = np.zeros(n)
    for i in range(n):
        h = int(power.loc[i, "Hour"])
        if 6 <= h <= 19:
            eff = np.exp(-((h - 12.5) ** 2) / 6)
            solar_gen[i] = eff * 90 * np.random.choice([1, 0.6], p=[0.8, 0.2])

    solar = pd.DataFrame({
        "거래일자": power["Date"],
        "거래시간": power["Hour"] + 1,   # 1~24
        "태양광": solar_gen.round(1),
    })

    return power, temp, solar


def build_merged(power: pd.DataFrame, temp: pd.DataFrame, solar: pd.DataFrame) -> pd.DataFrame:
    # -------------------------
    # power: datetime 만들기
    # -------------------------
    power = power.copy()
    power["Date"] = pd.to_datetime(power["Date"])
    power["Hour"] = power["Hour"].astype(int)
    power["datetime"] = power["Date"] + pd.to_timedelta(power["Hour"], unit="h")
    power = power.sort_values("datetime").drop(columns=["Date", "Hour"])

    # -------------------------
    # temp: 일시 datetime
    # -------------------------
    temp = temp.copy()
    temp["일시"] = pd.to_datetime(temp["일시"])
    temp = (
        temp.sort_values("일시")
            .rename(columns={"일시": "datetime", "기온(°C)": "temp_c"})
    )

    # -------------------------
    # solar: datetime 만들기
    # -------------------------
    solar = solar.copy()
    solar["거래일자"] = pd.to_datetime(solar["거래일자"])
    solar["거래시간"] = solar["거래시간"].astype(int)
    solar["Hour"] = solar["거래시간"] - 1  # 1~24 → 0~23
    solar["datetime"] = solar["거래일자"] + pd.to_timedelta(solar["Hour"], unit="h")
    solar = (
        solar.sort_values("datetime")
             .rename(columns={"태양광": "solar"})
             .drop(columns=["거래일자", "거래시간", "Hour"])
    )

    # -------------------------
    # merge
    # -------------------------
    merged = (
        power.merge(solar, on="datetime", how="outer")
             .merge(temp,  on="datetime", how="outer")
             .sort_values("datetime")
             .reset_index(drop=True)
    )
    return merged


# ============================
# import 시 자동 실행 (AUTORUN)
# ============================
def _default_out_dir() -> str:
    if "__file__" in globals():
        return str(Path(__file__).resolve().parent)
    return os.getcwd()


def run_on_import(
    url: str = DEFAULT_URL,
    out_dir: str | None = None,
    start: str = "2024-07-01",
    end: str = "2024-07-07",
    out_filename: str = "integrated_merged.csv",
    out_encoding: str = "utf-8-sig",
    seed: int | None = 42,
):
    if out_dir is None:
        out_dir = _default_out_dir()

    # 1) 메모리에서만 power/temp/solar 생성
    power, temp, solar = make_power_temp_solar_frames(url=url, start=start, end=end, seed=seed)

    # 2) 메모리에서 merge 생성
    merged = build_merged(power, temp, solar)

    # 3) 최종 파일만 저장
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, out_filename)
    merged.to_csv(out_path, index=False, encoding=out_encoding)

    return merged


_AUTORUN = os.environ.get("NYISO_EIA_AUTORUN", "1") == "1"

if _AUTORUN and not globals().get("_NYISO_EIA_ALREADY_RAN", False):
    globals()["_NYISO_EIA_ALREADY_RAN"] = True
    out_dir_used = _default_out_dir()
    try:
        run_on_import(out_dir=out_dir_used)
        print(f"✅ nyiso_eia import 완료: integrated_merged.csv만 생성됨 (저장폴더: {out_dir_used})")
    except Exception as e:
        print(f"⚠️ nyiso_eia import 자동 실행 실패: {e}")
