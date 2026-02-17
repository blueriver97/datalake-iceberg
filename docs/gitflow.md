# 브랜치 전략

### **브랜치 구성**

| 브랜치 유형 | 설명                      |
| ----------- | ------------------------- |
| main        | 배포 가능한 안정적인 코드 |
| develop     | 최신 개발 코드            |
| feature/\*  | 새로운 기능 개발          |
| release/\*  | 릴리스 준비               |
| hotfix/\*   | 긴급 수정                 |

### **작업 흐름**

기능 개발 (feature):

- develop에서 feature/PROJ-123 브랜치를 생성합니다. (Jira 이슈 ID: PROJ-123)
- 기능 개발 완료 후 develop 브랜치로 병합(Merge)합니다.

릴리스 준비 (release):

- develop에서 release/v1.1.0 브랜치를 생성합니다.
- 버그 수정, 문서화 등 릴리스에 필요한 최종 작업을 수행합니다.
- 준비 완료 후, main 브랜치에 병합하고 v1.1.0 태그를 생성합니다.
- 릴리스 과정에서 발생한 변경 사항을 develop 브랜치에도 병합하여 최신 상태를 반영합니다.

긴급 수정 (hotfix):

- **main**에서 hotfix/PROJ-125 브랜치를 생성합니다.
- 긴급 수정 완료 후, main 브랜치와 develop 브랜치 모두에 병합합니다.

### 커밋 메시지 가이드라인

| 브랜치     | 작성 샘플                        | 예시                                     |
| ---------- | -------------------------------- | ---------------------------------------- |
| main       | vX.Y.Z: <변경 요약>              | v1.0.0: 배포                             |
|            |                                  | v1.0.1: Merge branch hotfix/001          |
|            |                                  | v1.1.0: Merge branch release/001         |
| release/\* | release/vX.Y.Z: <내용>           | release/v1.0.0: 변경 이력, 안정화        |
| hotfix/\*  | hotfix/<id>: <버그 설명>         | hotfix/001: 버그 긴급 수정               |
| develop    | develop: <내용>                  | develop: 코드 리팩토링, 오타, 오류 수정  |
|            | develop: Merge branch <브랜치명> | develop: Merge branch feature/001 - 내용 |
|            |                                  | develop: Merge branch hotfix/001 - 내용  |
|            |                                  | develop: Merge branch release/v1.0.0     |
| feature/\* | feature/<id>: <내용>             | feature/001: 기능 추가                   |

### develop, release, main 사용 예시

```bash
(3)  * b2971c6 v0.0.50: D,E,F 기능 추가 (main, tag: v0.0.50)
     |\  -----------------------------------------------------
     | | (release) git switch main ← main 브랜치 전환
     | | (main)    git merge --no-ff release -m "v0.0.50: D,E,F 기능 추가" ← main 커밋 생성(b2971c6), --no-ff 사용 시 명시적인 커밋을 남김
     | | (main)    git tag v0.0.50 ← 태그 생성
     | | (main)    git branch -D release develop    ← develop, release 브랜치 삭제
     | | (main)    git push origin --delete develop ← (옵션) remote develop 브랜치 삭제
     | | -----------------------------------------------------
     | |
     | * 1ec483c release/v0.0.50: D,E,F 통합
     |/  -----------------------------------------------------
     |   (develop) git checkout -b release ← release 브랜치 생성
     |   (release) git reset --soft main   ← develop 브랜치 3개 커밋(efa3285, 5fe908d, 53f593e)을 돌려 변경사항을 staged 상태로 변경
     |   (release) git commit -m "release/v0.0.50: D,E,F 통합" ← release 브랜치 1개 커밋 생성(1ec483c)
     |   -----------------------------------------------------
     |
     | * 53f593e develop: 기능 F 구현
     | * 5fe908d develop: 기능 E 구현
     | * efa3285 develop: 기능 D 구현
     |/
     | (main) git checkout -b develop   ← develop 브랜치 생성
(2)  * 64c31f5 v0.0.49: A,B,C 기능 추가 (main, tag: v0.0.49)
     | -----------------------------------------------------
     | (release) git switch main ← main 브랜치 전환
     | (main)    git merge release -m "v0.0.49: A,B,C 기능 추가" ← main 커밋 생성(64c31f5)
     | (main)    git tag v0.0.49 ← 태그 생성
     | (main)    git branch -D release develop    ← develop, release 브랜치 삭제
     | (main)    git push origin --delete develop ← (옵션) remote develop 브랜치 삭제
     | -----------------------------------------------------
     |
     | * 773a1cc release/v0.0.49: A,B,C 통합
     |/  -----------------------------------------------------
     |   (develop) git checkout -b release ← release 브랜치 생성
     |   (release) git reset --soft main   ← develop 브랜치 3개 커밋(d99dc3d, 8915b17, 9d0fc88)을 돌려 변경사항을 staged 상태로 변경
     |   (release) git commit -m "release/v0.0.49: A,B,C 통합" ← release 브랜치 1개 커밋 생성(773a1cc)
     |   -----------------------------------------------------
     |
     | * 9d0fc88 develop: 기능 C 구현
     | * 8915b17 develop: 기능 B 구현
     | * d99dc3d develop: 기능 A 구현
     |/
     | (main) git checkout -b develop   ← develop 브랜치 생성
(1)  * 5a1ca5f v0.0.48: 이전 릴리즈 (main, tag: v0.0.48)
     * 2d89f1f v0.0.47: 이전 릴리즈 (main, tag: v0.0.47)
```
