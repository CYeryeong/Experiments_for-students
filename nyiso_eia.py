# nyiso_eia.py
import io
import requests
import numpy as np
import pandas as pd

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
    """
    Returns: (final_power_df, temp_df, solar_df)
    Also writes: nyiso_power.csv, temp.csv, solar.csv to out_dir
    """
    if seed is not None:
        np.random.seed(seed)

    df_eia = load_eia_csv_with_auto_header(url)

    # 날짜/부하 컬럼 자동 감지
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

    # 비율 적용(시뮬레이션)
    df_sample["NYC"] = df_sample[load_col] * 0.45
    df_sample["NJ"]  = df_sample[load_col] * 0.30
    df_sample["CT"]  = df_sample[load_col] * 0.25

    final_power = df_sample[["Date", "Hour", "NYC", "NJ", "CT"]].reset_index(drop=True)
    final_power.to_csv(f"{out_dir}/nyiso_power.csv", index=False, encoding=encoding)

    # 기온 생성
    dates = pd.to_datetime(df_sample[date_col].values)
    n = len(dates)
    daily_temp = np.sin((final_power["Hour"] - 14) * 2 * np.pi / 24) * 5
    temp_data = 28 + daily_temp + np.random.normal(0, 1, n)
    df_temp = pd.DataFrame({"일시": dates, "기온(°C)": temp_data.round(1)})
    df_temp.to_csv(f"{out_dir}/temp.csv", index=False, encoding=encoding)

    # 태양광 생성
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
    import matplotlib.pyplot as plt  # 모듈 import 시 의존성 부담 줄이기 위해 함수 내부 import

    plt.figure(figsize=(12, 6))
    plt.plot(final_power.index[:n_hours], final_power["NYC"][:n_hours], label="NYC", marker="o")
    plt.title("2024년 7월 전력 수요 패턴 (EIA 실제 데이터)")
    plt.legend()
    plt.grid(True)
    plt.show()
