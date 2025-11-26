import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv

# 상수 정의
load_dotenv()
API_KEY = os.getenv("API_KEY")

if not API_KEY:
    raise ValueError("API_KEY가 .env 파일에 설정되지 않았습니다.")

# 설정 상수
MAX_RETRIES = 3
RETRY_DELAY = 2
API_TIMEOUT = 60
WEATHER_STATION_ID = "108"  # 서울
START_DATE = "20240101"
END_DATE = "20241231"
TOTAL_DAYS = 366

# 디렉토리 경로
RAW_DATA_DIR = "raw_data"
WEATHER_DIR = os.path.join(RAW_DATA_DIR, "weather")
POWER_DIR = os.path.join(RAW_DATA_DIR, "power")


def ensure_directory(dir_path):
    """디렉토리가 없으면 생성"""
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)


def save_daily_csv(output_dir, date_str, data_dict):
    """날짜별 CSV 파일 저장"""
    daily_df = pd.DataFrame([data_dict])
    filepath = os.path.join(output_dir, f"{date_str}.csv")
    daily_df.to_csv(filepath, index=False, encoding="utf-8-sig")


def download_weather_data_by_date():
    """
    기상청 ASOS 일자료를 한 번에 다운로드 후 날짜별 파일로 저장
    raw_data/weather/2024-01-01.csv, 2024-01-02.csv, ...
    """
    print("기상 데이터 다운로드 시작 (일자료 일괄)...")
    ensure_directory(WEATHER_DIR)

    url = "https://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList"
    params = {
        "serviceKey": API_KEY,
        "pageNo": "1",
        "numOfRows": str(TOTAL_DAYS),
        "dataType": "JSON",
        "dataCd": "ASOS",
        "dateCd": "DAY",
        "startDt": START_DATE,
        "endDt": END_DATE,
        "stnIds": WEATHER_STATION_ID
    }

    for attempt in range(MAX_RETRIES):
        try:
            print(f"API 호출 중 (시도 {attempt + 1}/{MAX_RETRIES})...")
            response = requests.get(url, params=params, timeout=API_TIMEOUT)

            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}")

            data = response.json()
            items = data.get("response", {}).get("body", {}).get("items", {}).get("item", [])

            if not items:
                raise Exception("응답 데이터가 비어있습니다")

            # 단일 아이템이면 리스트로 변환
            if isinstance(items, dict):
                items = [items]

            success_count = 0

            # 각 날짜별로 CSV 파일 생성
            for item in items:
                try:
                    date_str = item.get('tm', '')
                    avg_temp = round(float(item.get('avgTa', 0)), 2)
                    avg_hum = round(float(item.get('avgRhm', 0)), 2)

                    save_daily_csv(WEATHER_DIR, date_str, {
                        'datetime': date_str,
                        'temperature': avg_temp,
                        'humidity': avg_hum
                    })
                    success_count += 1

                except Exception as e:
                    print(f"{date_str} 저장 오류: {str(e)}")
                    continue

            print(f"\n✓ 기상 데이터: {success_count}개 파일 생성")
            print(f"✓ 저장 위치: {WEATHER_DIR}/")
            return True

        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"시도 {attempt + 1} 실패: {str(e)}")
                time.sleep(RETRY_DELAY)
            else:
                print(f"다운로드 실패 ({MAX_RETRIES}회 시도): {str(e)}")
                return False

    return False


def fetch_power_data_all_pages(url):
    """전력 API에서 모든 페이지 데이터 가져오기"""
    all_data = []
    page = 1
    per_page = 1000

    while True:
        params = {
            "serviceKey": API_KEY,
            "page": page,
            "perPage": per_page
        }

        try:
            response = requests.get(url, params=params, timeout=API_TIMEOUT)

            if response.status_code != 200:
                print(f"페이지 {page} 실패: HTTP {response.status_code}")
                break

            data = response.json()
            page_data = data.get("data", [])

            if not page_data:
                break

            all_data.extend(page_data)

            if len(page_data) < per_page:
                break

            page += 1
            time.sleep(0.3)

        except Exception as e:
            print(f"페이지 {page} 오류: {str(e)}")
            break

    return all_data


def calculate_daily_average_power(row):
    """시간별 전력 데이터에서 일평균 계산"""
    hourly_values = []
    for h in range(1, 25):
        col = f'{h}시'
        if col in row:
            value = pd.to_numeric(row[col], errors='coerce')
            if pd.notna(value):
                hourly_values.append(value)

    if hourly_values:
        return round(sum(hourly_values) / len(hourly_values), 2)
    return None


def download_power_data_by_date():
    """
    전력 데이터를 날짜별로 다운로드
    raw_data/power/2024-01-01.csv, 2024-01-02.csv, ...
    """
    print("\n전력 데이터 다운로드 시작 (날짜별)...")
    ensure_directory(POWER_DIR)

    url = "https://api.odcloud.kr/api/15065266/v1/uddi:159ec977-0b24-4550-9308-bc5c851804f2"

    # 전체 데이터 다운로드
    all_data = fetch_power_data_all_pages(url)

    if not all_data:
        print("✗ 전력 데이터 다운로드 실패")
        return False

    df = pd.DataFrame(all_data)
    success_count = 0

    # 날짜별로 분할 저장
    for _, row in df.iterrows():
        date = row['날짜']
        avg_power = calculate_daily_average_power(row)

        if avg_power is not None:
            save_daily_csv(POWER_DIR, date, {
                'datetime': date,
                'power_consumption': avg_power
            })
            success_count += 1

    print(f"✓ 전력 데이터: {success_count}개 파일 생성")
    print(f"✓ 저장 위치: {POWER_DIR}/")

    return success_count > 0


if __name__ == "__main__":
    print("=" * 60)
    print("2024년 데이터 다운로드 (날짜별)")
    print("=" * 60 + "\n")

    weather_success = download_weather_data_by_date()
    power_success = download_power_data_by_date()

    print("\n" + "=" * 60)
    if weather_success and power_success:
        print("✓ 모든 데이터 다운로드 완료!")
        print("\n생성된 파일:")
        print("  - raw_data/weather/2024-01-01.csv ~ 2024-12-31.csv")
        print("  - raw_data/power/2024-01-01.csv ~ 2024-12-31.csv")
    else:
        print("✗ 일부 데이터 다운로드 실패")
    print("=" * 60)
