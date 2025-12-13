import sys
import os

# 'nyiso_eia.py' 파일이 있는 디렉토리를 sys.path에 추가합니다.
repo_path = '/content/Experiments_for-students'
if repo_path not in sys.path:
    sys.path.append(repo_path)

# nyiso_eia 모듈에서 make_nyiso_power_temp_solar 함수를 직접 불러옵니다.
# 이는 모듈 내의 자동 실행 로직에서 __file__ 문제가 발생하는 것을 방지합니다.
from nyiso_eia import make_nyiso_power_temp_solar

# 출력 파일을 저장할 디렉토리를 지정합니다. (기존처럼 /content/에 저장)
output_directory = '/content/'

DEFAULT_URL = "https://www.eia.gov/electricity/wholesalemarkets/csv/nyiso_load_act_hr_2024.csv"

try:
    # make_nyiso_power_temp_solar 함수를 호출하여 데이터를 생성합니다. 'url' 인자를 추가했습니다.
    make_nyiso_power_temp_solar(url=DEFAULT_URL, out_dir=output_directory)
    print(f"✅ nyiso_eia 실행 완료: nyiso_power.csv, temp.csv, solar.csv 파일 생성됨")
except Exception as e:
    print(f"⚠️ nyiso_eia 실행 실패: {e}")
