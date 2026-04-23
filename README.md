# Oracle to MSSQL DDL Converter

Oracle 데이터베이스 스키마를 분석하여 MSSQL 호환 DDL로 변환하는 도구입니다.

## 기능

- **테이블**: 컬럼, 제약조건 (PK, FK, UK, CHECK), 인덱스, 코멘트
- **뷰**: SQL 변환 (SYSDATE, NVL, ||, SUBSTR 등)
- **시퀀스**: MSSQL SEQUENCE 객체 (SQL Server 2012+)
- **프로시저/함수**: 파라미터, 데이터 타입, 함수 변환
- **트리거**: BEFORE/AFTER, :NEW/:OLD 변환

## 설치

```bash
pip install -r requirements.txt
```

Oracle 클라이언트가 필요합니다. `oracledb`는 Thin 모드로 동작하므로 별도 클라이언트 없이 사용 가능합니다.

## 사용법

### 명령행 인자

```bash
# 기본 사용법
python main.py --host localhost --service-name ORCL --user scott --password tiger --schema SCOTT

# SID 사용
python main.py --host localhost --sid ORCL --user scott --password tiger

# 설정 파일 사용
python main.py --config config.json

# 단일 파일 출력
python main.py --host localhost --service-name ORCL --user scott --password tiger --single-file

# 특정 객체만 추출
python main.py --host localhost --service-name ORCL --user scott --password tiger --include-tables --include-views --exclude-procedures

# 출력 디렉토리 지정
python main.py --host localhost --service-name ORCL --user scott --password tiger --output ./migration
```

### 설정 파일

```bash
cp example_config.json config.json
# config.json 편집 후
python main.py --config config.json
```

### 환경 변수

```bash
export ORACLE_HOST=localhost
export ORACLE_SERVICE_NAME=ORCL
export ORACLE_USERNAME=scott
export ORACLE_PASSWORD=tiger
export ORACLE_SCHEMA=SCOTT
export OUTPUT_DIR=./output

python main.py
```

## 데이터 타입 변환

| Oracle | MSSQL |
|--------|-------|
| VARCHAR2(n) | NVARCHAR(n) |
| NVARCHAR2(n) | NVARCHAR(n) |
| NUMBER | DECIMAL(18,2) |
| NUMBER(p,s) | DECIMAL(p,s) |
| NUMBER(n,0) | INT/BIGINT |
| DATE | DATETIME2(6) |
| TIMESTAMP | DATETIME2(6) |
| TIMESTAMP WITH TIME ZONE | DATETIMEOFFSET(6) |
| CLOB/NCLOB | NVARCHAR(MAX) |
| BLOB | VARBINARY(MAX) |
| BFILE | VARBINARY(MAX) |
| RAW | VARBINARY |
| FLOAT/REAL | FLOAT/REAL |

## 함수/구문 변환

| Oracle | MSSQL |
|--------|-------|
| SYSDATE | GETDATE() |
| SYSTIMESTAMP | SYSDATETIME() |
| NVL(a,b) | ISNULL(a,b) |
| \|\| | + |
| SUBSTR(s,n,m) | SUBSTRING(s,n,m) |
| INSTR(s,p) | CHARINDEX(p,s) |
| TO_CHAR() | CONVERT(VARCHAR, ) |
| TO_DATE() | CONVERT(DATETIME2, ) |
| TO_NUMBER() | CAST() |
| TRUNC() | CAST() |
| DECODE() | CASE |
| LISTAGG() | STRING_AGG() |
| ROWNUM | TOP n |
| CONNECT BY | Recursive CTE |
| :NEW.col | INSERTED.col |
| :OLD.col | DELETED.col |
| DUAL | (제거) |
| BEFORE TRIGGER | INSTEAD OF TRIGGER |

## 출력 구조

### 단일 파일 모드
```
output/
└── migration_20240101_120000.sql
```

### 다중 파일 모드
```
output/
├── 01_sequences_20240101_120000.sql
├── 02_tables_20240101_120000.sql
├── 03_views_20240101_120000.sql
├── 04_procedures_20240101_120000.sql
└── 05_triggers_20240101_120000.sql
```

## 주의사항

1. **수동 검토 필요 항목**:
   - CONNECT BY 계층적 쿼리
   - 복잡한 DECODE 표현식
   - Oracle 내장 패키지 사용 (DBMS_*, UTL_*)
   - 사용자 정의 타입
   - 오브젝트 타입
   - 중첩 테이블

2. **호환성**:
   - 일반 테이블/뷰: ~95%
   - 시퀀스: ~100% (SQL Server 2012+)
   - 프로시저/함수: ~80% (복잡도에 따라 다름)
   - 트리거: ~85%

## 구성 옵션

| 옵션 | 설명 | 기본값 |
|------|------|--------|
| `include_tables` | 테이블 포함 | true |
| `include_views` | 뷰 포함 | true |
| `include_sequences` | 시퀀스 포함 | true |
| `include_procedures` | 프로시저 포함 | true |
| `include_functions` | 함수 포함 | true |
| `include_triggers` | 트리거 포함 | true |
| `include_indexes` | 인덱스 포함 | true |
| `remove_schema_prefix` | 스키마 접두어 제거 | true |
| `target_schema` | 대상 MSSQL 스키마 | dbo |
| `handle_auto_increment` | 자동 증가 변환 | true |
| `single_file` | 단일 파일 출력 | false |

## 라이선스

MIT
