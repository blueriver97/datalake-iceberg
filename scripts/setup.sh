#!/usr/bin/env bash

# ---------------------------------------------------------
# 색상 및 태그 정의
# ---------------------------------------------------------
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SUCCESS_TAG="${GREEN}[성공]${NC}"
FAILURE_TAG="${RED}[실패]${NC}"
WARN_TAG="${YELLOW}[주의]${NC}"
INFO_TAG="[진행]"

# ---------------------------------------------------------
# 공통 함수
# ---------------------------------------------------------

# Git 상세 설정 (에디터, 출력 방식 등)
setup_git_config() {
  # vim 에디터 및 한글 경로 깨짐 방지 설정
  git config core.editor "vim"
  git config core.quotepath false
  git config fetch.prune true
  git config merge.ff true
  git config alias.recommit "commit --amend"
}

# 에러 체크 및 스크립트 중단 처리
check_error() {
    local exit_code=$1
    local message=$2

    if [[ $exit_code -ne 0 ]]; then
        echo -e "${FAILURE_TAG} $message"
        echo -e "${RED}오류가 발생하여 스크립트를 중단합니다.${NC}"
        exit 1
    else
        echo -e "${SUCCESS_TAG} $message"
    fi
}

# 설치 가이드 출력 함수
show_install_guide() {
    local tool=$1
    # English log message
    echo "Log: Required tool '$tool' is missing."

    echo -e "${WARN_TAG} ${tool} 도구가 설치되어 있지 않습니다."
    echo "   [즉시 실행 가능한 설치 명령어]"

    case "$tool" in
        "uv")
            # uv 설치를 위한 원라인 명령어
            echo "   curl -LsSf https://astral.sh/uv/install.sh | sh"
            ;;
        "gh")
            # 운영체제 확인 후 macOS는 brew, Linux는 공식 스크립트 제공
            if [[ "$OSTYPE" == "darwin"* ]]; then
                echo "   brew install gh"
            else
                echo "   (type -p curl >/dev/null || sudo apt install curl -y) && curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg && sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg && echo \"deb [arch=\$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main\" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null && sudo apt update && sudo apt install gh -y"
            fi
            ;;
        "npm")
            # Node.js 설치를 위한 명령어
            if [[ "$OSTYPE" == "darwin"* ]]; then
                echo "   brew install node"
            else
                echo "   curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash - && sudo apt-get install -y nodejs"
            fi
            ;;
    esac

    echo -e "${INFO_TAG} Please copy and run the command above, then restart the script."
}

echo "환경 설정을 시작합니다..."

# ---------------------------------------------------------
# 1. 필수 도구 확인 (uv, npm, gh)
# ---------------------------------------------------------

# uv 설치 확인
if ! command -v uv &> /dev/null; then
    show_install_guide "uv"
    exit 1
fi

# npm 설치 확인
if ! command -v npm &> /dev/null; then
    show_install_guide "npm"
    exit 1
fi

# GitHub CLI 확인 및 설정
if ! command -v gh &> /dev/null; then
    show_install_guide "gh"
else
    # GitHub 로그인 상태 확인
    if ! gh auth status &> /dev/null; then
        echo -e "${WARN_TAG} GitHub CLI 로그인이 필요합니다."
        gh auth login
    else
        # SONAR_TOKEN 등록 여부 확인 및 설정
        if ! gh secret list | grep -q "SONAR_TOKEN"; then
            if [[ -z "$SONAR_TOKEN" ]]; then
                read -sp "   > SonarCloud 토큰을 입력하세요 (건너뛰려면 Enter): " INPUT_TOKEN
                echo ""
                [[ -n "$INPUT_TOKEN" ]] && export SONAR_TOKEN="$INPUT_TOKEN"
            fi
            if [[ -n "$SONAR_TOKEN" ]]; then
                gh secret set SONAR_TOKEN --body "$SONAR_TOKEN"
                check_error $? "GitHub Secrets에 SONAR_TOKEN 등록 완료"
            fi
        fi
    fi
fi

# ---------------------------------------------------------
# 2. 패키지 및 파이썬 도구 설치 (uv tool 활용)
# ---------------------------------------------------------

# Node.js 프로젝트 의존성 설치
if [[ -f "package.json" ]]; then
    echo "   > NPM 패키지 설치 중..."
    npm install > /dev/null 2>&1
    check_error $? "NPM 패키지 설치 완료"
fi

# pre-commit 도구 설치 (uv tool로 독립 환경 설치)
echo "   > uv를 통해 pre-commit 도구를 설치합니다..."
uv tool install pre-commit --force > /dev/null 2>&1
check_error $? "pre-commit 도구 설치 완료"

# TruffleHog 설치 (보안 검사 도구)
echo "   > uv를 통해 TruffleHog 보안 도구를 설치합니다..."
uv tool install trufflehog --force > /dev/null 2>&1
check_error $? "TruffleHog 설치 완료"

# Wily 설치 (코드 복잡도 분석 도구)
echo "   > uv를 통해 Wily 도구를 설치합니다..."
uv tool install wily --force > /dev/null 2>&1
check_error $? "Wily 설치 완료"

# Wily 초기 데이터 생성 (uvx 활용)
if [[ -d "src" ]]; then
    echo "   > uvx를 통해 Wily 초기 데이터를 생성합니다..."
    uvx wily build src > /dev/null 2>&1
    check_error $? "Wily 초기 설정 완료"
else
    echo -e "${WARN_TAG} 'src' 디렉토리가 없어 Wily 빌드를 건너뜁니다."
fi

# ---------------------------------------------------------
# 3. AI 도구 설정 (OpenCommit)
# ---------------------------------------------------------

# OpenCommit 설치 확인 및 설치
if ! command -v oco &> /dev/null; then
    echo "   > OpenCommit을 설치 중입니다..."
    npm install -g opencommit > /dev/null 2>&1
fi

# Gemini API 키 설정
if [[ -z "$GEMINI_API_KEY" ]]; then
    read -sp "   > Gemini API Key를 입력하세요 (건너뛰려면 Enter): " INPUT_KEY
    echo ""
    [[ -n "$INPUT_KEY" ]] && export GEMINI_API_KEY="$INPUT_KEY"
fi

if [[ -n "$GEMINI_API_KEY" ]]; then
    echo "   > OpenCommit 설정 적용 중..."
    oco config set OCO_API_KEY="$GEMINI_API_KEY" > /dev/null 2>&1
    oco config set OCO_AI_PROVIDER=gemini > /dev/null 2>&1
    oco config set OCO_MODEL=gemini-2.5-flash-lite > /dev/null 2>&1
    oco config set OCO_LANGUAGE=ko > /dev/null 2>&1
    oco config set OCO_GITPUSH=false > /dev/null 2>&1
    oco config set OCO_TOKENS_MAX_OUTPUT=512 > /dev/null 2>&1
    oco config set OCO_TOKENS_MAX_INPUT=40960 > /dev/null 2>&1
    oco config set OCO_ONE_LINE_COMMIT=true > /dev/null 2>&1
    check_error $? "OpenCommit 설정 완료"
fi

# ---------------------------------------------------------
# 4. Git Hook 및 사용자 설정
# ---------------------------------------------------------

# uvx를 사용해 격리된 환경에서 후크 설치 (신뢰성 확보)
echo "   > uvx를 사용하여 Git Hooks를 설치합니다..."
uvx pre-commit install --hook-type pre-commit --hook-type commit-msg --hook-type prepare-commit-msg > /dev/null 2>&1
check_error $? "Git Hooks 설치 완료"

# Git 사용자 정보 설정
[[ -z "$GIT_USER" ]] && read -p "   > Git 사용자 이름 입력: " GIT_USER
[[ -z "$GIT_EMAIL" ]] && read -p "   > Git 이메일 입력: " GIT_EMAIL

if [[ -n "$GIT_USER" && -n "$GIT_EMAIL" ]]; then
    git config user.name "$GIT_USER"
    git config user.email "$GIT_EMAIL"
    setup_git_config
    check_error $? "Git 사용자 및 기본 설정 완료"
fi

# ---------------------------------------------------------
# 5. SonarQube 프로젝트 키 자동화
# ---------------------------------------------------------
SONAR_FILE="sonar-project.properties"
if [[ -f "$SONAR_FILE" ]]; then
    CURRENT_DIR_NAME=$(basename "$PWD")
    ORG_NAME=$(grep "^sonar.organization=" "$SONAR_FILE" | cut -d'=' -f2)
    [[ -z "$ORG_NAME" ]] && ORG_NAME="blueriver97"
    NEW_KEY="${ORG_NAME}_${CURRENT_DIR_NAME}"

    # 운영체제별 sed 명령어 차이 대응
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "s/^sonar.projectKey=.*/sonar.projectKey=$NEW_KEY/" "$SONAR_FILE"
    else
        sed -i "s/^sonar.projectKey=.*/sonar.projectKey=$NEW_KEY/" "$SONAR_FILE"
    fi
    check_error $? "SonarQube 프로젝트 키 업데이트 완료: $NEW_KEY"
fi

echo -e "${GREEN}모든 환경 설정이 성공적으로 완료되었습니다.${NC}"
