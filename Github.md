# GitHub CLI (`gh`) 사용법

## 설치

```bash
brew install gh
```

## 인증

```bash
gh auth login
```

## 레포지토리 생성

### 수동 생성 (웹)
<https://github.com/new>

### CLI로 생성 및 푸시 (권장)

```bash
# 1. 로컬 디렉토리에 레포지토리 생성 후 푸시
gh repo create <username>/<repo-name> --public --source=. --push

# 2. 빈 레포지토리만 생성 (소스 없이)
gh repo create <repo-name> --public
```

## 기존 레포지토리 작업

```bash
# 원격지 확인
git remote -v

# 원격지 주소 변경
git remote set-url origin https://github.com/<username>/<repo-name>.git

# 푸시
git push -u origin main

# 최신 변경 가져오기
git pull origin main
```

## 주요 명령어

| 명령어 | 설명 |
|--------|------|
| `gh repo create` | 새 레포지토리 생성 |
| `gh repo list` | 내 레포지토리 목록 |
| `gh repo view` | 브라우저에서 열기 |
| `gh pr create` | Pull Request 생성 |
| `gh issue list` | 이슈 목록 조회 |
| `gh run list` | GitHub Actions 실행 목록 |

## 전체 워크플로우 예시

```bash
# 1. 프로젝트 폴더에서
cd /path/to/project

# 2. Git 초기화
git init
git add -A
git commit -m "Initial commit"

# 3. gh로 생성 + 푸시 (한 번에)
gh repo create <username>/<repo-name> --public --source=. --push
```
