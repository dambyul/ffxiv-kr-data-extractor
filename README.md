# FFXIV KR Data Extractor

파이널판타지14 한국 서버 클라이언트 데이터의 추출, 정제, 배포를 자동화하는 파이프라인입니다.

## 주요 기능

- **데이터 파이프라인**: SaintCoinach를 이용한 데이터 추출(`run.bat`)부터 정제, RSV 처리, 검증, 압축, AWS S3 업로드, Discord 알림 전송까지(`main.py`) 전 과정을 스크립트로 처리합니다.
- **필터링 시스템**: Google Sheets와 연동하여 협업 데이터를 실시간으로 가져오며(`managed_filter.tmp.json`), 로컬 수동 설정(`filter.json`)을 우선 적용해 데이터를 제어합니다.
- **데이터 매핑 및 주입**: `Swap_Key`를 통해 원본 데이터를 보존하며 새로운 키로 복제합니다. `Swap_Offset` 기능으로 글로벌 서버(JP/EN) 텍스트를 주입하거나 다른 컬럼의 값을 참조할 수 있습니다.
- **RSV 자동 변환**: CSV 데이터 내 `_rsv_` 키를 감지하여 ACT Plugin Overrides 데이터를 기준으로 영문 명칭으로 자동 변환합니다.

## 프로젝트 구조

```text
.
├── compare/                # 한/글섭 데이터 비교 및 리포트 생성 스크립트
├── extract/                # SaintCoinach 기반 데이터 추출 스크립트
├── transform/              # 데이터 정제 및 배포 스크립트
│   ├── config/             # 필터, 프리셋, RSV 설정 파일
│   ├── lib/                # 데이터 처리 및 외부 연동(S3, Sheets) 모듈
│   └── main.py             # 파이프라인 실행 엔트리포인트
├── .env                    # 환경 변수 설정
└── requirements.txt        # Python 패키지 의존성
```

## 실행 방법

### 1. 요구 사항
- Python 3.8 이상
- FFXIV 한국 서버 클라이언트 (추출용)

### 2. 환경 설정
1. 패키지 설치:
   ```powershell
   pip install -r requirements.txt
   ```
2. 루트 디렉토리에 `.env` 파일을 생성하고 구성합니다 (`.env.example` 참조).
   ```env
   S3_BUCKET_NAME=your-bucket-name
   GOOGLE_SHEET_ID=your-sheet-id
   GOOGLE_CREDS_PATH=google_sheet.json
   DISCORD_WEBHOOK_URL=[https://discord.com/api/webhooks/](https://discord.com/api/webhooks/)... # 선택 사항
   ```
3. 구글 API 서비스 계정 키(`google_sheet.json`)를 프로젝트 루트 디렉토리에 배치합니다.

### 3. 워크플로우

**A. 데이터 추출 및 비교 (유지보수)**
패치 업데이트 시 한국 서버와 글로벌 서버 간의 텍스트 변경점을 식별하고 구글 시트에 동기화합니다.
```powershell
.\extract\run.bat "C:\FFXIV_KR"
.\extract\run.bat "C:\FFXIV_GL"
python compare/client_diff.py --kr "extract/output/KR_VER" --gl "extract/output/GL_VER" --sync
```

**B. 데이터 정제 및 배포 (릴리스)**
한국 서버 데이터를 추출하고 정제하여 S3 버킷에 최종 배포합니다.
```powershell
.\extract\run.bat "C:\FFXIV_KR"
python transform/main.py <KR_VERSION>
```

## 보안

- `google_sheet.json` 및 `.env` 파일은 `.gitignore`에 포함되어 저장소에 업로드되지 않습니다.
- 수동 설정용 `filter.json`은 버전 관리 대상이나, 실행 시 동기화되는 `managed_filter.tmp.json`은 추적 대상에서 제외됩니다.

## 참고 자료

- [SaintCoinach](https://github.com/GpointChen/SaintCoinach): 클라이언트 데이터 추출 도구
- [xivapi/SaintCoinach](https://github.com/xivapi/SaintCoinach): 글로벌 데이터 정의 소스
- [FFXIV_ACT_Plugin](https://github.com/ravahn/FFXIV_ACT_Plugin): RSV 영문 명칭 동기화 기준 데이터