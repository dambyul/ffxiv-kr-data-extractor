# FFXIV KR Data Extractor

파이널판타지14 한국 서버 클라이언트의 텍스트와 이미지 리소스를 추출, 정제, 배포하는 파이프라인입니다.

## 주요 기능

- **데이터 파이프라인**: SaintCoinach 기반 데이터 추출부터 CSV 정제, RSV 처리, 검증, 압축, AWS S3 업로드, Discord 알림까지 자동화합니다.
- **필터링 시스템**: Google Sheets의 협업 설정과 로컬 수동 설정을 병합하고, 수동 설정을 우선 적용합니다.
- **데이터 매핑 및 주입**: `Swap_Key`로 데이터를 새로운 키에 복제하고, `Swap_Offset`으로 글로벌 텍스트나 다른 컬럼의 값을 참조합니다.
- **RSV 자동 변환**: `_rsv_` 키를 ACT Plugin Overrides 데이터에 기반한 영문 명칭으로 변환합니다.
- **일반 채팅 필요 퀘스트 우회**: 한국어 일반 채팅 입력을 요구하는 메시지와 퀘스트 대사를 익명화합니다.
- **한국어 이미지 추출**: `ffxiv-resource-swap`을 통해 지역명·레벨 업·알림(`120000~129999`), 폴 가이즈·도움말·보스 이름 등(`180000~189999`)을 추출합니다. `180101~180103`, `180151~180153`은 제외하며, 글로벌에 대응 경로가 있고 TEX 형식이 호환되는 일반·고해상도 리소스를 포함합니다.
- **통합 버전 관리**: 텍스트와 이미지를 함께 배포하고 공통 `version.txt`를 발행합니다. 이미지 ZIP은 최신 파일 하나로 관리합니다.

## 프로젝트 구조

```text
.
├── compare/                # 한/글섭 데이터 비교 및 리포트 생성
├── extract/                # SaintCoinach 기반 데이터 추출
├── transform/              # 데이터 정제 및 배포
│   ├── config/             # 필터, 프리셋, RSV 설정
│   ├── lib/                # 데이터 처리, 이미지 패키징, 외부 연동
│   ├── main.py             # 텍스트·이미지 통합 파이프라인
│   └── resources.py        # 이미지 리소스 단독 패키징 및 배포
├── .env                    # 환경 변수 설정
└── requirements.txt        # Python 패키지 의존성
```

## 실행 방법

### 1. 요구 사항

- Python 3.9 이상
- FFXIV 한국 서버 클라이언트
- 이미지 추출 시 글로벌 클라이언트, .NET 10 SDK 및 `ffxiv-resource-swap` 프로젝트

### 2. 환경 설정

1. Python 의존성을 설치합니다.

   ```powershell
   pip install -r requirements.txt
   ```

2. 프로젝트 루트에 `.env`를 생성합니다 (`.env.example` 참조).

   ```env
   S3_BUCKET_NAME=your-bucket-name
   GOOGLE_SHEET_ID=your-sheet-id
   GOOGLE_CREDS_PATH=google_sheet.json
   DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/... # 선택 사항

   # 이미지 추출·배포를 포함하는 경우
   RESOURCE_SWAP_PROJECT=D:/dev/ffxiv-resource-swap
   RESOURCE_KR_CLIENT=C:/FFXIV_KR
   RESOURCE_GLOBAL_CLIENT=C:/FFXIV_GL
   RESOURCE_PUBLIC_BASE_URL=https://your-cloudfront-domain/
   ```

   세 리소스 경로를 설정하면 기존 파이프라인에 이미지 추출·업로드가 자동으로 포함됩니다. `RESOURCE_PUBLIC_BASE_URL`은 이미지 단독 배포 시 공개 텍스트 버전을 확인하는 데 사용합니다.

3. Google API 서비스 계정 키를 프로젝트 루트의 `google_sheet.json`에 배치하고 AWS 업로드 자격 증명을 설정합니다.

### 3. 워크플로우

**A. 데이터 추출 및 비교 (유지보수)**

패치 업데이트 시 한국·글로벌 클라이언트의 텍스트 변경점을 비교하고 Google Sheets에 동기화합니다.

```powershell
.\extract\run.bat "C:\FFXIV_KR"
.\extract\run.bat "C:\FFXIV_GL"
python compare/client_diff.py --kr "extract/output/KR_VER" --gl "extract/output/GL_VER" --sync
```

**B. 데이터 정제 및 배포 (릴리스)**

추출한 CSV를 정제하고, 설정된 클라이언트에서 이미지를 추출해 함께 배포합니다.

```powershell
.\extract\run.bat "C:\FFXIV_KR"
python transform/main.py <KR_VERSION>
```

이미지 포함 여부는 `--with-resources` 또는 `--without-resources`로 명시할 수 있습니다. 업로드 순서는 CSV ZIP → 이미지 ZIP → `data.json` → 공통 `version.txt`이며, 업로드 실패 시 새 버전과 성공 알림을 발행하지 않습니다.

**C. 이미지 단독 배포 (보조 작업)**

현재 공개된 텍스트 버전에 맞춰 이미지만 생성하거나 추가 배포합니다. `<TEXT_VERSION>`에는 공개 `version.txt`의 값을 입력합니다.

```powershell
# 추출 및 로컬 ZIP 생성
python transform/resources.py --text-version <TEXT_VERSION>

# 추출 및 S3 업로드
python transform/resources.py --upload --text-version <TEXT_VERSION>

# 기존 추출 디렉토리로 ZIP 생성
python transform/resources.py --package <PACKAGE_DIRECTORY> --text-version <TEXT_VERSION>
```

### 4. 배포 파일

| 파일 | 용도 |
| --- | --- |
| `rawexd.zip` | 정제된 텍스트 데이터 |
| `resources.zip` | 한국어 이미지와 내부 `manifest.json` |
| `data.json` | 적용범위 프리셋 및 이미지 배포 정보(`imageResources`) |
| `version.txt` | 텍스트·이미지 공통 배포 버전 |

S3 루트의 고정 파일을 덮어쓰며 이미지 버전별 폴더를 생성하지 않습니다. ZIP에는 이미지 파일과 manifest만 포함하고, 로더 DLL은 Injector에서 제공합니다. 폰트는 기존 `font.zip`·`font-version.txt`로 별도 관리합니다.

통합 파이프라인은 Stable 배포를 대상으로 합니다. Staging 이미지 단독 배포는 `RESOURCE_RELEASE_CHANNEL=staging`을 지정하고 공개 `dev-version.txt`와 같은 텍스트 버전을 사용합니다. 이미지 ZIP은 하나이므로 여러 채널·클라이언트 버전의 패키지를 동시에 보관하지 않습니다.

## 보안

- `.env`와 `google_sheet.json`은 Git 추적 대상에서 제외합니다.
- 로컬 필터·프리셋·RSV 설정과 실행 중 생성되는 임시 설정은 저장소에 포함하지 않습니다.
- 배포 ZIP에는 클라이언트 절대 경로, 인증 정보 및 실행 파일을 포함하지 않습니다.

## 참고 자료

- [SaintCoinach](https://github.com/GpointChen/SaintCoinach): 클라이언트 데이터 추출 도구
- [xivapi/SaintCoinach](https://github.com/xivapi/SaintCoinach): 글로벌 데이터 정의 소스
- [FFXIV_ACT_Plugin](https://github.com/ravahn/FFXIV_ACT_Plugin): RSV 영문 명칭 동기화 기준 데이터
