# FFXIV KR Data Extractor

FFXIV 한국 서버 데이터를 추출, 정제하여 자동 배포하기 위한 파이프라인 프로젝트입니다.

## 주요 특징

- **통합 자동화 (`run.bat` & `main.py`)**: 데이터 추출부터 파이프라인 정제, 배포까지 전 과정을 자동화합니다.
- **Auto Definition Sync**: 추출 시마다 `xivapi/SaintCoinach`에서 최신 글로벌 데이터 정의(JSON)를 자동으로 가져와 환경을 최신 상태로 유지합니다.
- **RSV 핸들링**: 
    - CSV에서 RSV 키를 자동 스캔하고 [ACT Plugin Overrides](https://github.com/ravahn/FFXIV_ACT_Plugin)와 연동하여 최신 영문 명칭으로 동기화합니다.
- **데이터 필터링**: 
    - 한글 또는 RSV 데이터가 포함된 중요한 열은 보존하고, 데이터가 없는 불필요한 파일은 배제합니다.
- **클라우드 배포**: 정제된 데이터를 `rawexd.zip`으로 압축하여 AWS S3/CloudFront로 자동 업로드합니다.
- **Global Data (JP) 지원**: 글로벌(JP) 클라이언트 데이터를 정제하여 한국 서버 데이터와 비교 가능한 형태로 추출합니다. (한국어 자동 제외 및 JP/EN 추출)

## 프로젝트 구조

```text
.
├── extract/                # 데이터 추출 모듈 (SaintCoinach 기반)
│   ├── SaintCoinach.Cmd/   # SaintCoinach 실행 바이너리 프로젝트
│   ├── Definitions/        # xivapi에서 다운로드된 최신 정의 빌드 디렉토리
│   ├── run.bat             # [Step 1] 추출 전체 자동화 스크립트
│   └── update_definitions.py # 글로벌 데이터 자동 업데이트 및 싱크 스크립트
├── transform/              # 데이터 정제 및 변환 모듈
│   ├── config/             # 설정 파일 (filter, preset, validation 등)
│   ├── lib/                # 코어 라이브러리 (rsv, processor, uploader 등)
│   ├── original/           # 추출된 원본 데이터 저장소 (ko/, jp/ 하위 구조)
│   ├── output/             # 정제된 결과 데이터 저장소 (ko/, jp/ 하위 구조)
│   └── main.py             # [Step 2] 정제 및 배포 통합 진입점
├── .env                    # 환경 변수 (S3 설정 등)
├── .gitignore              # Git 제외 설정
└── requirements.txt        # Python 의존성 패키지 목록
```

## 시작하기

### 1. 사전 요구 사항 (Prerequisites)

- **Python 3.8+**
- **.NET 7.0 SDK** (SaintCoinach 빌드 및 실행용)
- **FFXIV 한국 클라이언트**

### 2. 환경 설정

1. Python 패키지 설치:
   ```powershell
   pip install -r requirements.txt
   ```
2. `.env` 설정 (`.env.example` 참고):
   ```env
   BASE_DIR=.
   S3_BUCKET_NAME=your-bucket-name
   ```
3. 설정 파일 복사:
   `transform/config/*.json.example` 파일들을 복사하여 `.example`이 없는 이름으로 저장합니다.
   - `filter.json.example` -> `filter.json`
   - `preset.json.example` -> `preset.json`
   - `rsv.json.example` -> `rsv.json`

### 3. 사용 방법

#### 단계 1: 데이터 추출 (Extract)

`extract/run.bat`를 실행하여 최신 정의 동기화 및 데이터 추출을 수행합니다.
```powershell
# 사용법: .\run.bat "<FFXIV 한국서버 설치경로>"
cd extract
.\run.bat "C:\Program Files (x86)\FINAL FANTASY XIV - KOREA"
```

#### 단계 2: 정제 및 배포 (Transform & Deploy)

`transform/main.py`를 실행하여 정제 파이프라인을 가동합니다.
```powershell
# 사용법: python transform/main.py <버전명>
python transform/main.py 2026.02.06.0000.0000
```

## 참고 리포지토리

- **[SaintCoinach](https://github.com/GpointChen/SaintCoinach)**: 클라이언트 데이터 추출 및 헥스 태그 처리
- **[xivapi/SaintCoinach](https://github.com/xivapi/SaintCoinach)**: 최신 글로벌 데이터 정의 소스
- **[FFXIV_ACT_Plugin](https://github.com/ravahn/FFXIV_ACT_Plugin)**: RSV 영문 명칭 동기화 소스