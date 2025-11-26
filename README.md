# 기상 요인이 전력소비량에 미치는 영향 분석

2024년 서울 지역 기상 데이터와 전국 전력소비량 데이터를 활용한 통계 분석 프로젝트

## 프로젝트 개요

### 데이터 기간
- **시작일**: 2024-01-01
- **종료일**: 2024-12-31
- **총 데이터**: 366건 (일별)

### 분석 목적
기상 요인(기온, 습도)이 전력소비량에 미치는 영향을 정량적으로 분석

## 프로젝트 구조

```
statistic_project/
├── download_data.py          # 데이터 다운로드 스크립트
├── preprocess_and_merge.py   # 데이터 전처리 및 병합 스크립트
├── raw_data/                 # 원본 데이터 (366개 날짜별 CSV)
│   ├── weather/              # 기상 데이터
│   └── power/                # 전력 데이터
├── processed_data/           # 전처리 완료 데이터
│   └── final_data.csv        # 최종 병합 데이터
├── .env                      # API 키 (보안)
├── requirements.txt          # Python 패키지 의존성
└── README.md
```

## 설치 및 실행

### 1. 환경 설정

```bash
# 가상환경 생성 및 활성화
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 패키지 설치
pip install -r requirements.txt
```

### 2. API 키 설정

`.env` 파일 생성 후 API 키 추가:
```
API_KEY=your_api_key_here
```

### 3. 데이터 다운로드

```bash
python download_data.py
```

### 4. 데이터 전처리 및 병합

```bash
python preprocess_and_merge.py
```

## 데이터 구조

### processed_data/final_data.csv

최종 병합 및 전처리 완료된 데이터 (366건)

| 변수명 | 설명 | 단위 | 데이터 타입 | 예시 |
|--------|------|------|------------|------|
| `datetime` | 날짜 | YYYY-MM-DD | datetime64 | 2024-01-01 |
| `year` | 연도 | - | int64 | 2024 |
| `month` | 월 | - | int64 | 1 ~ 12 |
| `day` | 일 | - | int64 | 1 ~ 31 |
| `day_of_week` | 요일 | - | int64 | 0(월) ~ 6(일) |
| `season` | 계절 | - | object | spring, summer, fall, winter |
| `temperature` | 일평균 기온 | °C | float64 | -11.7 ~ 31.8 |
| `humidity` | 일평균 습도 | % | float64 | 28.6 ~ 95.8 |
| `power_consumption` | 일평균 전력소비량 | MWh | float64 | 48,599 ~ 83,202 |

### 계절 정의

- **spring (봄)**: 3월, 4월, 5월
- **summer (여름)**: 6월, 7월, 8월
- **fall (가을)**: 9월, 10월, 11월
- **winter (겨울)**: 12월, 1월, 2월

## 데이터 출처

### 기상 데이터
- **출처**: 기상자료개방포털 (data.kma.go.kr)
- **API**: 기상청 ASOS 일자료 (`AsosDalyInfoService`)
- **관측 지점**: 서울(108)
- **수집 항목**:
  - `avgTa`: 일평균 기온 (°C)
  - `avgRhm`: 일평균 상대습도 (%)

### 전력 데이터
- **출처**: 공공데이터포털 (data.go.kr)
- **제공**: 한국전력거래소 (KPX)
- **데이터**: 시간별 전국 전력수요량
- **처리**: 24시간 데이터를 일평균으로 집계 (MWh)

## 데이터 전처리

### 결측치 처리

모든 변수에 대해 선형 보간법 (`interpolate(method='linear')`) 적용

### 이상치 처리

#### 기상 데이터
- **기온**: -30°C ~ 45°C 범위 벗어난 값 → 결측 처리 후 선형 보간
- **습도**: 0% ~ 100% 범위 벗어난 값 → 결측 처리 후 선형 보간

#### 전력 데이터
- **음수값**: 결측 처리
- **IQR 방법**: Q3 + 3×IQR 초과하는 극단값 → 결측 처리 후 선형 보간

### 주요 전처리 파라미터

파일: `preprocess_and_merge.py`

```python
# 데이터 검증 범위
TEMP_MIN = -30      # 최소 기온 (°C)
TEMP_MAX = 45       # 최대 기온 (°C)
HUMIDITY_MIN = 0    # 최소 습도 (%)
HUMIDITY_MAX = 100  # 최대 습도 (%)
POWER_MIN = 0       # 최소 전력소비량

# IQR 이상치 제거 계수
IQR_MULTIPLIER = 3
```

## 분석 계획

### 1. 단순 회귀분석
-[x] <item> **독립변수 1**: 기온 (°C)
- [] <item> **독립변수 2**: 습도 (%)
- **종속변수**: 전력소비량 (MWh)

### 2. 분산분석 (ANOVA)
- **독립변수**: 계절 (spring, summer, fall, winter)
- **종속변수**: 전력소비량 (MWh)

### 3. 분석 도구
- **R**: 통계 분석 및 시각화

## 주요 통계량

```
temperature: 평균 14.88°C, 표준편차 10.54°C
humidity: 평균 65.85%, 표준편차 13.16%
power_consumption: 평균 64,534 MWh, 표준편차 7,686 MWh
```

## 주의사항

### 공휴일 효과
- 1월 1일 등 공휴일에 전력소비량이 크게 감소
- 이는 정상적인 패턴으로, 이상치가 아님
- 공장/사무실 휴무로 인한 실제 전력 수요 감소

### 데이터 특성
- 계절별 냉난방 수요에 따라 전력소비 패턴 상이
- 여름철 냉방, 겨울철 난방으로 인한 비선형적 관계 존재



    
