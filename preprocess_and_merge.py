import pandas as pd
import numpy as np
import os

# 상수 정의
RAW_DATA_DIR = "raw_data"
WEATHER_DIR = os.path.join(RAW_DATA_DIR, "weather")
POWER_DIR = os.path.join(RAW_DATA_DIR, "power")
PROCESSED_DATA_DIR = "processed_data"
OUTPUT_FILE = os.path.join(PROCESSED_DATA_DIR, "final_data.csv")

# 날짜 형식
DATE_FORMAT = '%Y-%m-%d'

# 데이터 검증 범위
TEMP_MIN = -30
TEMP_MAX = 45
HUMIDITY_MIN = 0
HUMIDITY_MAX = 100
POWER_MIN = 0

# IQR 이상치 제거 계수
IQR_MULTIPLIER = 3

# 계절 매핑
SEASONS = {
    'spring': [3, 4, 5],
    'summer': [6, 7, 8],
    'fall': [9, 10, 11],
    'winter': [12, 1, 2]
}


def load_csv_files_from_dir(directory, data_name):
    """디렉토리에서 모든 CSV 파일 로드 및 병합"""
    if not os.path.exists(directory):
        raise FileNotFoundError(f"{directory} 폴더가 없습니다. download_data.py를 먼저 실행하세요.")

    csv_files = sorted([f for f in os.listdir(directory) if f.endswith('.csv')])

    if not csv_files:
        raise ValueError(f"{directory}에 CSV 파일이 없습니다.")

    dfs = [pd.read_csv(os.path.join(directory, f)) for f in csv_files]
    merged_df = pd.concat(dfs, ignore_index=True)

    print(f"{data_name} 데이터 로드 완료: {len(csv_files)}개 파일, {len(merged_df)}건")
    return merged_df


def load_raw_data():
    """
    raw_data/weather/, raw_data/power/ 폴더에서 날짜별 CSV 파일들을 로드
    """
    print("원본 데이터 로드 중...")

    weather_df = load_csv_files_from_dir(WEATHER_DIR, "기상")
    power_df = load_csv_files_from_dir(POWER_DIR, "전력")

    return weather_df, power_df


def remove_outliers_and_interpolate(df, column, min_val, max_val):
    """범위 벗어난 이상치 제거 및 선형 보간"""
    df.loc[(df[column] < min_val) | (df[column] > max_val), column] = np.nan
    df[column] = df[column].interpolate(method='linear')
    return df


def preprocess_weather_data(df):
    """
    기상 데이터 전처리
    """
    print("\n기상 데이터 전처리 중...")

    df = df.copy()

    # datetime 컬럼을 표준 datetime 형식으로 변환
    df['datetime'] = pd.to_datetime(df['datetime'], format=DATE_FORMAT)

    # 기온과 습도를 숫자형으로 변환
    df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
    df['humidity'] = pd.to_numeric(df['humidity'], errors='coerce')

    # 결측치 확인
    print(f"기온 결측치: {df['temperature'].isna().sum()}건")
    print(f"습도 결측치: {df['humidity'].isna().sum()}건")

    # 결측치 처리: 선형 보간
    df['temperature'] = df['temperature'].interpolate(method='linear')
    df['humidity'] = df['humidity'].interpolate(method='linear')

    # 이상치 처리 (한국 기준)
    df = remove_outliers_and_interpolate(df, 'temperature', TEMP_MIN, TEMP_MAX)
    df = remove_outliers_and_interpolate(df, 'humidity', HUMIDITY_MIN, HUMIDITY_MAX)

    print(f"전처리 완료: {len(df)}건")

    return df


def remove_outliers_iqr(df, column, multiplier):
    """IQR 방법으로 상위 이상치 제거 및 보간"""
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    upper_bound = Q3 + multiplier * IQR

    df.loc[df[column] > upper_bound, column] = np.nan
    df[column] = df[column].interpolate(method='linear')
    return df


def preprocess_power_data(df):
    """
    전력 데이터 전처리
    """
    print("\n전력 데이터 전처리 중...")

    df = df.copy()

    # datetime을 datetime 타입으로 변환
    df['datetime'] = pd.to_datetime(df['datetime'], format=DATE_FORMAT)

    # 전력소비량을 숫자형으로 변환
    df['power_consumption'] = pd.to_numeric(df['power_consumption'], errors='coerce')

    # 결측치 확인
    print(f"전력소비량 결측치: {df['power_consumption'].isna().sum()}건")

    # 결측치 처리: 선형 보간
    df = df.sort_values('datetime').reset_index(drop=True)
    df['power_consumption'] = df['power_consumption'].interpolate(method='linear')

    # 이상치 처리 (음수값)
    df.loc[df['power_consumption'] < POWER_MIN, 'power_consumption'] = np.nan

    # IQR 방법으로 이상치 제거 (상위 극단값)
    df = remove_outliers_iqr(df, 'power_consumption', IQR_MULTIPLIER)

    # 필요한 컬럼만 선택
    df_result = df[['datetime', 'power_consumption']].copy()

    print(f"전처리 완료: {len(df_result)}건")

    return df_result


def get_season(month):
    """월에 따라 계절 반환"""
    for season, months in SEASONS.items():
        if month in months:
            return season
    return 'winter'


def add_season_column(df):
    """
    계절 컬럼 추가
    봄: 3~5월, 여름: 6~8월, 가을: 9~11월, 겨울: 12~2월
    """
    df['season'] = df['datetime'].dt.month.apply(get_season)
    return df


def add_time_features(df):
    """시간 관련 특성 추가"""
    df['year'] = df['datetime'].dt.year
    df['month'] = df['datetime'].dt.month
    df['day'] = df['datetime'].dt.day
    df['day_of_week'] = df['datetime'].dt.dayofweek  # 0:월 ~ 6:일
    return df


def merge_data(weather_df, power_df):
    """
    기상 데이터와 전력 데이터를 날짜 기준으로 병합
    """
    print("\n데이터 병합 중...")

    # datetime을 기준으로 inner join
    merged_df = pd.merge(weather_df, power_df, on='datetime', how='inner')
    print(f"병합 완료: {len(merged_df)}건")

    # 계절 및 시간 특성 추가
    merged_df = add_season_column(merged_df)
    merged_df = add_time_features(merged_df)

    # 컬럼 순서 정리
    final_columns = [
        'datetime', 'year', 'month', 'day', 'day_of_week', 'season',
        'temperature', 'humidity', 'power_consumption'
    ]

    merged_df = merged_df[final_columns]

    return merged_df


def print_summary_statistics(df):
    """데이터 요약 통계 출력"""
    print("\n[데이터 요약 통계]")
    print(df[['temperature', 'humidity', 'power_consumption']].describe())

    print("\n[계절별 데이터 개수]")
    print(df['season'].value_counts().sort_index())


def save_processed_data(df):
    """
    전처리 완료된 데이터를 processed_data 폴더에 저장
    """
    print("\n처리된 데이터 저장 중...")

    if not os.path.exists(PROCESSED_DATA_DIR):
        os.makedirs(PROCESSED_DATA_DIR)

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"저장 완료: {OUTPUT_FILE}")
    print(f"최종 데이터: {len(df)}건")

    print_summary_statistics(df)

    return OUTPUT_FILE


def main():
    """
    전체 전처리 파이프라인 실행
    """
    print("=" * 60)
    print("데이터 전처리 및 병합 시작")
    print("=" * 60)

    try:
        # 1. 원본 데이터 로드
        weather_df, power_df = load_raw_data()

        # 2. 기상 데이터 전처리
        weather_clean = preprocess_weather_data(weather_df)

        # 3. 전력 데이터 전처리
        power_clean = preprocess_power_data(power_df)

        # 4. 데이터 병합
        final_df = merge_data(weather_clean, power_clean)

        # 5. 저장
        output_path = save_processed_data(final_df)

        print("\n" + "=" * 60)
        print("✓ 전처리 완료!")
        print(f"✓ 파일 위치: {output_path}")
        print("=" * 60)

    except Exception as e:
        print("\n" + "=" * 60)
        print(f"✗ 오류 발생: {str(e)}")
        print("=" * 60)
        raise


if __name__ == "__main__":
    main()
