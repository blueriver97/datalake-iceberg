#!/usr/bin/env python3
import sys
import re
import subprocess


def get_current_branch():
    try:
        return subprocess.check_output(["git", "symbolic-ref", "--short", "HEAD"]).decode("utf-8").strip()
    except Exception as e:
        sys.stderr.write(f"Unexpected error in get_current_branch: {e}\n")
        return ""


def main():
    # pre-commit은 커밋 메시지가 담긴 임시 파일의 경로를 첫 번째 인자로 전달.
    commit_msg_filepath = sys.argv[1]

    with open(commit_msg_filepath, "r", encoding="utf-8") as f:
        commit_msg = f.read().strip()

    branch = get_current_branch()
    branch_type = branch.split("/")[0] if "/" in branch else branch

    # 정규식 패턴 정의
    patterns = {
        "main": r"^v\d+\.\d+\.\d+: .+",
        "release": r"^release/v\d+\.\d+\.\d+: .+",
        "hotfix": r"^hotfix/[a-zA-Z0-9_-]+: .+",
        "develop": r"^develop: .+",
        "feature": r"^feature/[a-zA-Z0-9_-]+: .+",
    }
    # 예시 문장
    examples = {
        "main": "vX.Y.Z: 커밋 메시지",
        "release": "release/vX.Y.Z: 커밋 메시지",
        "hotfix": "hotfix/issue-192: 커밋 메시지",
        "develop": "develop: 커밋 메시지",
        "feature": "feature/issue-283: 커밋 메시지",
    }

    if branch_type in patterns:
        if not re.match(patterns[branch_type], commit_msg):
            print(f"\n[Error] 커밋 메시지 형식이 브랜치 '{branch_type}' 규칙에 맞지 않습니다.")
            print(f"   요구 형식: {examples[branch_type]}")
            print(f"   입력된 메시지: {commit_msg}\n")
            sys.exit(1)
    else:
        # main, release 등이 아닌 경우 (예: 로컬 실험용 브랜치)는 통과시킬지, 막을지 결정
        print(f"\n[Error] 알 수 없는 브랜치 유형입니다: {branch_type}")
        print("   사용 가능한 유형: main, release, hotfix, develop, feature")
        sys.exit(1)


if __name__ == "__main__":
    main()
