## 프로젝트 소개
Kaggle 의 "Financial Statement Extracts" 데이터셋(총 6개의 JSON / 5GB) 을 대상으로 
데이터 파이프라인을 구축한 프로젝트입니다.

<br>

## 데이터셋 정보
* 각 JSON 파일 세부 내용은 key-value 딕셔너리 구조로 구성되어 있습니다. 딕셔너리의 키는 다음과 같습니다.
  헤더 뿐만 아니라 세부 데이터 필드는 TSV 형태로 구분되며, row는 개행문자로 구분됩니다.
  * num.txt: 재무 수치 (숫자 데이터)
    * `adsh`: 제출번호
    * `tag`: 태그명
    * `version`: taxonomy
    * `ddate`: 보고일자
    * `qtrs`: 분기 수 (0=시점값, >0=기간값)
    * `uom`: 단위 (USD, shares 등)
    * `value`: 수치 값
    * `footnote`: 각주
  * pre.txt // 프레젠테이션 링크 - 재무제표 항목 간 관계
    * `adsh`: 제출번호
    * `report`: 보고서 그룹 번호
    * `line`: 라인 순서
    * `stmt`: 재무제표 유형 (BS, IS, CF, EQ, CI 등)
    * `tag`: 태그명
    * `plabel`: 표시된 라벨
    * `inpth`: 괄호(부연설명) 여부
    * `rfile`: EDGAR 렌더링 형식(.htm / .xml)
  * sub.txt // 회사 제출 정보 (메타데이터)
    * `adsh`: EDGAR 제출번호 (기본 키)
    * `cik`: SEC 기업 고유번호
    * `name`: 기업명
    * `sic`: 표준 산업 코드
    * `countryba`, `stprba`, `cityba`, `zipba`: 사업장 주소
    * `countryinc`, `stprinc`: 설립 국가/주
    * `ein`: IRS 기업 식별번호
    * `form`: 제출 서식 (예: 10-K, 10-Q)
    * `period`: 재무제표 기준일
    * `fy`, `fp`: 회계연도 / 분기
    * `filed`, `accepted`: 제출일, 접수일시
    * `wksi`: Well-Known Seasoned Issuer 여부
    * `detail`: 주석 포함 여부
    * `instance`: 제출된 XBRL 인스턴스 파일명
  * tag.txt // 태그 정보
    * `version`: 표준 taxonomy 또는 커스텀 정의 (adsh)
    * `custom`: 커스텀 여부 (1=커스텀)
    * `abstract`: 숫자 사용 여부 (1=숫자 아님)
    * `tag`: 태그명
    * `da  tatype`: 데이터 타입 (monetary, shares 등)
    * `iord`: 시점(Point-in-time, I) / 기간(Duration, D)
    * `crdr`: 차변/대변 속성
    * `tlabel`: 라벨
    * `doc`: 정의
   
<br>

## 구현 / 고려 내용
* 
