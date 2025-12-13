# ============================
# import 시 자동 실행 (AUTORUN)
# ============================
from pathlib import Path
import os

DEFAULT_URL = "https://www.eia.gov/electricity/wholesalemarkets/csv/nyiso_load_act_hr_2024.csv"

def run_on_import(
    url: str = DEFAULT_URL,
    out_dir: str | None = None,
    start: str = "2024-07-01",
    end: str = "2024-07-07",
    encoding: str = "cp949",
    seed: int | None = 42,
):
    """
    import만 해도 파일 생성되도록 기본 실행을 묶은 함수.
    out_dir을 안 주면 이 .py 파일이 있는 폴더에 저장.
    """
    if out_dir is None:
        out_dir = str(Path(__file__).resolve().parent)

    return make_nyiso_power_temp_solar(
        url=url,
        start=start,
        end=end,
        out_dir=out_dir,
        encoding=encoding,
        seed=seed,
    )

# ✅ 환경변수로 자동실행 ON/OFF 가능 (기본 ON)
#    Colab에서 끄고 싶으면: os.environ["NYISO_EIA_AUTORUN"] = "0" 하고 import
_AUTORUN = os.environ.get("NYISO_EIA_AUTORUN", "1") == "1"

# ✅ 같은 세션에서 reload 등으로 중복 실행 방지
if _AUTORUN and not globals().get("_NYISO_EIA_ALREADY_RAN", False):
    globals()["_NYISO_EIA_ALREADY_RAN"] = True
    try:
        run_on_import()
        print("✅ nyiso_eia import 완료: nyiso_power.csv, temp.csv, solar.csv 생성됨")
    except Exception as e:
        print(f"⚠️ nyiso_eia import 자동 실행 실패: {e}")
