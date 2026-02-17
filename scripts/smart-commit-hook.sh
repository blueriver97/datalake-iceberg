#!/usr/bin/env bash

# Git이 전달하는 인자
COMMIT_MSG_FILE="$1"
COMMIT_SOURCE="$2"
SHA1="$3"

# 1. 예외 처리: Merge, Amend, 혹은 이미 메시지가 있는 경우 건너뜀
if [[ "$COMMIT_SOURCE" = "merge" ]] || [[ "$COMMIT_SOURCE" = "commit" ]]; then
    exit 0
fi

# 2. OpenCommit 설치 확인
if ! command -v oco &> /dev/null; then
    echo "OpenCommit이 설치되지 않아 AI 메시지 생성을 건너뜁니다."
    exit 0
fi

# 3. 현재 브랜치 정보
current_branch=$(git branch --show-current)
echo "AI가 커밋 메시지를 작성 중입니다..."

# 4. AI 메시지 생성 및 파싱 (핵심 수정 부분)
# ---------------------------------------------------------
# CI=1: 상호작용(프롬프트)을 강제로 끄는 표준 환경변수입니다. (TTY 에러 방지)
# 2>&1: 에러 로그도 변수에 담아서 화면에 지저분하게 뜨지 않게 합니다.
# || true: oco가 에러로 종료되어도 스크립트가 멈추지 않게 합니다.
RAW_OUTPUT=$(CI=1 oco --fg 2>&1 || true)

# 4. 메시지 추출 (구분선 파싱)
GENERATED_MSG=$(echo "$RAW_OUTPUT" | awk '/^——————————————————$/ {if (p) exit; p=1; next} p')
if [[ -z "$GENERATED_MSG" ]]; then
    # 패턴으로 찾기
    # ^(feat|...): 커밋 타입으로 시작하고
    # (\(.*\))?: 옵션으로 스코프가 있으며
    # :\s+: 콜론 뒤에 공백이 있고
    # .{5,}: 그 뒤에 최소 5글자 이상의 내용이 있어야 함
    GENERATED_MSG=$(echo "$RAW_OUTPUT" | grep -E '^(feat|fix|docs|style|refactor|perf|test|chore|revert|build|ci)(\(.*\))?:' | head -n 1)
fi
# 접두어(feat:) 제거
if [[ -n "$GENERATED_MSG" ]]; then
    CLEAN_MSG=$(echo "$GENERATED_MSG" | sed -E 's/^(feat|fix|docs|style|refactor|perf|test|chore|revert|build|ci)(\(.*\))?:\s*//')

    # ${#CLEAN_MSG}: 문자열 길이 반환
    if [[ ${#CLEAN_MSG} -gt 0 ]]; then
        GENERATED_MSG="$CLEAN_MSG"
    fi
fi

if [[ -z "$GENERATED_MSG" ]]; then
    echo "메시지 생성 실패. 기본 에디터를 엽니다."
    # echo "# [Debug] AI Output Log:" >> "$COMMIT_MSG_FILE"
    # echo "$RAW_OUTPUT" | sed 's/^/# /' >> "$COMMIT_MSG_FILE"
    exit 0
fi

# 5. 브랜치별 메시지 가공 (기존 로직 유지)
if [[ "$current_branch" = "main" ]]; then
    # Main 브랜치: 버전 업 로직
    LAST_COMMIT_MSG=$(git log -1 --pretty=%B)

    if [[ $LAST_COMMIT_MSG =~ ^v([0-9]+)\.([0-9]+)\.([0-9]+) ]]; then
        major=${BASH_REMATCH[1]}
        minor=${BASH_REMATCH[2]}
        patch=${BASH_REMATCH[3]}

        if [[ $patch -ge 100 ]]; then
            new_minor=$((minor + 1))
            new_patch=1
            new_version="v${major}.${new_minor}.${new_patch}"
        else
            new_patch=$((patch + 1))
            new_version="v${major}.${minor}.${new_patch}"
        fi
    else
        new_version="v0.0.1"
    fi

    FINAL_MSG="#$new_version: $GENERATED_MSG"

else
    # 일반 브랜치: 이슈 번호/브랜치명 태깅
    FINAL_MSG="#$current_branch: $GENERATED_MSG"
fi

# 6. 결과 파일 작성
# 임시 파일을 만들어 '새 메시지' -> '빈 줄' -> '기존 깃 템플릿' 순서로 병합
TEMP_FILE=$(mktemp)

echo "$FINAL_MSG" > "$TEMP_FILE"
echo "" >> "$TEMP_FILE"
cat "$COMMIT_MSG_FILE" >> "$TEMP_FILE"

# 합친 내용을 원본 파일로 이동
mv "$TEMP_FILE" "$COMMIT_MSG_FILE"
