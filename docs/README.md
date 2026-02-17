# Python Base (Project Template)

이 저장소는 팀의 표준 개발 환경, 보안 설정(TruffleHog), 코드 품질 분석(SonarQube), AI 커밋 메시지 작성 도구가 통합된 기본 템플릿 저장소입니다.

새로운 프로젝트를 시작할 때 이 저장소를 템플릿으로 사용하여 생성하십시오.

---

## 1. 주요 기능 (Features)

- 보안 (Security): TruffleHog를 통한 비밀 키(Secret Key) 커밋 방지
- 품질 (Quality): SonarQube(정적 분석), Ruff(고속 린터), MyPy(타입 검사) 연동
- 자동화 (Automation): pre-commit 훅을 이용한 코드 포맷팅(Prettier) 및 커밋 메시지 검사, GitHub Actions(CI/CD)를 통한 테스트 및 배포 자동화
- AI 지원 (AI Assistant): OpenCommit(Gemini)을 이용한 커밋 메시지 자동 작성

---

## 2. 필수 요구 사항 (Prerequisites)

이 템플릿을 사용하기 위해 로컬 환경에 다음 도구들이 설치되어 있어야 합니다.

- Node.js (v22 이상)
  - macOS: `brew install node`
  - Ubuntu: `curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash - && sudo apt install -y nodejs`
  - Amazon Linux 2023: `sudo dnf install nodejs22`

- Python (v3.11)
  - macOS: `brew install python@3.11`
  - Ubuntu: `sudo add-apt-repository ppa:deadsnakes/ppa -y && sudo apt install python3.11 python3.11-venv`
  - Amazon Linux 2023: `sudo dnf install python3.11 python3.11-devel`

- GitHub CLI (gh) (시크릿 자동 등록)
  - macOS: `brew install gh`
  - Ubuntu: `sudo apt install gh`
  - Amazon Linux 2023:
    `sudo dnf install 'dnf-command(config-manager)' & sudo dnf config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo & sudo dnf install gh`

---

## 3. 초기 설정 (Setup)

저장소를 Clone 받은 후, 프로젝트 루트에서 다음 명령어를 최초 한 번 실행하십시오.

```bash
npm run setup
```

---

## 4. 가상환경 구성 (Configure Virtual Environment)

이 프로젝트는 Python 3.11 기반의 가상환경에서 실행하는 것을 권장합니다.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip setuptools wheel
```

---

## 5. 패키지 설치 (Install Packages)

이 프로젝트는 Python 의존성 관리를 위해 pyproject.toml 표준을 따르며 개발 진행 중 새로운 라이브러리를 추가할 때는 아래 절차를 따르십시오.

```toml
[project]
name = "__PROJECT_NAME__"
version = "__PROJECT_VERSION__"
description = "__PROJECT_DESCRIPTION__"
# ...
dependencies = [
    "fastapi>=0.110.0",
    "uvicorn[standard]",
    "pydantic>=2.7.0",
    "requests"
]
```

```bash
pip install -e .
```

- pyproject.toml에 명시된 의존성들은 자동으로 설치됩니다.
- -e (editable) 옵션을 사용하면 현재 경로를 기준으로 패키지가 설치됩니다.
- Editable 모드(-e) 대신 일반 설치를 권장합니다.
- Docker 빌드 최적화(layer caching)를 위해 소스 코드 외 의존성 설치만 한다면, requirements.txt 파일을 사용해야 합니다.

```bash
pip freeze | grep -v "__PROJECT_NAME__" > requirements.txt
pip install -r requirements.txt
```

---

## A. 부록 (Appendix)

이 프로젝트는 커밋 시 `pre-commit`을 통해 다음 도구들로 코드 품질과 보안을 자동으로 검사합니다.

### 1. 검사 항목 (Active Hooks)

| 도구 (Tool)      | 역할                                                 | 비고                 |
| :--------------- | :--------------------------------------------------- | :------------------- |
| **TruffleHog**   | **보안**: AWS Key, Password 등 비밀 정보 유출 차단   | 절대 무시 금지       |
| **Prettier**     | **포맷팅**: Markdown, JSON, YAML 등 스타일 자동 정리 |                      |
| **Ruff**         | **Python**: 코드 린팅(오류 검사) 및 포맷팅           |                      |
| **MyPy**         | **Python**: 정적 타입 검사 (`def func() -> int:`)    |                      |
| **Large File**   | **용량**: 100MB 이상 파일 커밋 차단                  |                      |
| **Syntax Check** | **문법**: JSON, YAML 파일 문법 오류 검사             |                      |
| **Commit Msg**   | **규칙**: 브랜치별 커밋 메시지 패턴 강제 검사        | `validate_commit.py` |

### 2. 검사 건너뛰기 (Bypass Hooks)

상황에 따라 검사를 우회해야 할 때, 두 가지 방법을 사용할 수 있습니다.

- `SKIP` 환경변수에 건너뛸 Hook의 ID를 쉼표(`,`)로 구분하여 적습니다. 보안 검사는 살려두고 포맷팅만 끄고 싶을 때 유용합니다.

  ```bash
  SKIP=sqlffluf-lint,sqlffluf-fix git commit
  ```

- 모든 검사를 생략하고 강제로 커밋해야 할 경우, `--no-verify` (또는 `-n`) 옵션을 사용하십시오.

  ```bash
  git commit --no-verify
  ```

### 3. 커밋하지 않고 검사하기 (Manual Check)

- 커밋하지 않고 검사만 미리 돌려보고 싶을 때 사용합니다.

  ```bash
   # 변경된 파일에 대해서만 모든 검사 실행
   pre-commit run

   # 특정 도구만 전체 파일에 대해 실행
   pre-commit run trufflehog --all-files
  ```

### 4. MyPy 타입 검사 예외 처리 (MyPy Overrides)

외부 라이브러리를 설치해 사용할 때, 해당 라이브러리에 타입 정보(Type Stubs)가 없으면 MyPy가 `Module '...' has no attribute` 또는 import 오류를 발생시킬 수 있습니다.

이때 pyproject.toml을 수정하여 특정 라이브러리의 검사를 제외할 수 있습니다.

```toml
[[tool.mypy.overrides]]
# 타입 정보가 없는 라이브러리 목록을 여기에 적습니다.
module = [
    "hvac.*", # 예: HashiCorp Vault 클라이언트
    "boto3.*", # 예: AWS SDK (타입 스텁 미설치 시)
    "pandas.*"     # 예: Pandas
]
ignore_missing_imports = true
```

`ignore_missing_imports = true`: "이 모듈들은 타입 정의 파일(.pyi)이 없더라도 에러를 내지 말고 무시하라"는 뜻입니다.

MyPy는 해당 모듈에서 가져온 객체들을 모두 Any 타입(검사 안 함)으로 취급하게 됩니다.

### 5. Wily 사용 방법 (Wily Usage)

Wily는 Python 코드의 복잡도를 측정하여 변화를 추적하는 도구입니다.

pre-commit 단계에서 자동으로 실행되지만, 초기 설정이나 수동 분석이 필요할 때 다음 가이드를 참고하십시오.

- 초기 데이터 생성 (Build)

  Wily는 Git 커밋 기록을 순회하며 복잡도를 분석하므로, 사용 전 반드시 인덱스를 생성해야 합니다.

  ```bash
  # src 디렉토리를 기준으로 복잡도 분석 데이터 생성
  wily build src
  ```

- "Dirty repository" 오류 해결

  wily build 실행 시 다음과 같은 에러가 발생할 수 있습니다.

  `Failed to setup archiver: 'Dirty repository, make sure you commit/stash files first'`

  Wily는 과거 커밋으로 체크아웃(Checkout)하며 분석을 수행합니다.

  이때 작업 중인 파일(Uncommitted changes)이나 스테이징(Staged)된 파일이 있으면 충돌 방지를 위해 실행이 중단됩니다.

  src 폴더 외의 파일(README.md 등)이 수정된 경우에도 발생합니다. 이 경우 작업 중인 변경 사항을 잠시 치워두고(stash), 빌드 후 다시 복구(pop)해야 합니다.

  ```bash
  # 1. 작업 중인 내용 임시 저장 (작업 트리를 깨끗하게 만듦)
  git stash

  # 2. Wily 빌드 실행
  wily build src

  # 3. 임시 저장했던 변경 사항 복구
  git stash pop
  ```

- 리포트 확인 (Report)

  빌드가 완료된 후, 현재 코드의 복잡도 순위를 확인하거나 특정 파일의 변화를 그래프로 볼 수 있습니다.

  ```bash
  # 가장 복잡한 함수/파일 순위 확인
  wily rank src

  # 특정 파일의 복잡도 변화 리포트
  wily report src/base/utils/common.py
  ```
