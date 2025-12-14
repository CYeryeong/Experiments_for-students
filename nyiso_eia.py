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


def make_nyiso_power_temp_solar(
    url: str,
    start: str = "2024-07-01",
    end: str = "2024-07-07",
    out_dir: str = ".",
    encoding: str = "cp949",
    seed: int | None = 42,
):
    if seed is not None:
        np.random.seed(seed)

    df_eia = load_eia_csv_with_auto_header(url)

    target_date_cols = [c for c in df_eia.columns if ("time" in c.lower()) or ("period" in c.lower()) or ("date" in c.lower())]
    target_load_cols = [c for c in df_eia.columns if ("load" in c.lower()) or ("value" in c.lower())]

    if not target_date_cols or not target_load_cols:
        raise ValueError(f"날짜 또는 부하 컬럼 감지 실패. 현재 컬럼: {list(df_eia.columns)}")

    date_col = target_date_cols[0]
    load_col = target_load_cols[0]

    df_eia[date_col] = pd.to_datetime(df_eia[date_col])
    df_eia = df_eia.sort_values(date_col)

    mask = (df_eia[date_col] >= start) & (df_eia[date_col] <= end)
    df_sample = df_eia.loc[mask].copy()

    if df_sample.empty:
        raise ValueError(f"{start}~{end} 구간 데이터가 비었습니다. 범위: {df_eia[date_col].min()} ~ {df_eia[date_col].max()}")

    df_sample["Date"] = df_sample[date_col].dt.strftime("%Y-%m-%d")
    df_sample["Hour"] = df_sample[date_col].dt.hour

    df_sample["NYC"] = df_sample[load_col] * 0.45
    df_sample["NJ"]  = df_sample[load_col] * 0.30
    df_sample["CT"]  = df_sample[load_col] * 0.25

    os.makedirs(out_dir, exist_ok=True)

    final_power = df_sample[["Date", "Hour", "NYC", "NJ", "CT"]].reset_index(drop=True)
    final_power.to_csv(f"{out_dir}/nyiso_power.csv", index=False, encoding=encoding)

    dates = pd.to_datetime(df_sample[date_col].values)
    n = len(dates)
    daily_temp = np.sin((final_power["Hour"] - 14) * 2 * np.pi / 24) * 5
    temp_data = 28 + daily_temp + np.random.normal(0, 1, n)
    df_temp = pd.DataFrame({"일시": dates, "기온(°C)": temp_data.round(1)})
    df_temp.to_csv(f"{out_dir}/temp.csv", index=False, encoding=encoding)

    solar_gen = np.zeros(n)
    for i in range(n):
        h = int(final_power.loc[i, "Hour"])
        if 6 <= h <= 19:
            eff = np.exp(-((h - 12.5) ** 2) / 6)
            solar_gen[i] = eff * 90 * np.random.choice([1, 0.6], p=[0.8, 0.2])

    df_solar = pd.DataFrame({
        "거래일자": final_power["Date"],
        "거래시간": final_power["Hour"] + 1,
        "태양광": solar_gen.round(1),
    })
    df_solar.to_csv(f"{out_dir}/solar.csv", index=False, encoding=encoding)

    return final_power, df_temp, df_solar


def plot_power(final_power, n_hours: int = 48):
    import matplotlib.pyplot as plt
    plt.figure(figsize=(12, 6))
    plt.plot(final_power.index[:n_hours], final_power["NYC"][:n_hours], label="NYC", marker="o")
    plt.title("2024년 7월 전력 수요 패턴 (EIA 실제 데이터)")
    plt.legend()
    plt.grid(True)
    plt.show()


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
    encoding: str = "cp949",
    seed: int | None = 42,
):
    if out_dir is None:
        out_dir = _default_out_dir()
    return make_nyiso_power_temp_solar(
        url=url, start=start, end=end, out_dir=out_dir, encoding=encoding, seed=seed
    )


_AUTORUN = os.environ.get("NYISO_EIA_AUTORUN", "1") == "1"

if _AUTORUN and not globals().get("_NYISO_EIA_ALREADY_RAN", False):
    globals()["_NYISO_EIA_ALREADY_RAN"] = True
    out_dir_used = _default_out_dir()
    try:
        run_on_import(out_dir=out_dir_used)
        print(f"✅ nyiso_eia import 완료: CSV 생성됨 (저장폴더: {out_dir_used})")
    except Exception as e:
        print(f"⚠️ nyiso_eia import 자동 실행 실패: {e}")
