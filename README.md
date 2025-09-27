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
* VMware 에서 Hadoop Cluster 구축 (Master Node 1, Data Node 4 - 이 중 Data Node 2대에서 환경 구축)
* 파이프라인 구조도
  <img width="392" height="402" alt="image" src="https://github.com/user-attachments/assets/2132d5bc-cd72-464a-96f3-e581d5230316" />

* HDFS 에 처리할 데이터셋을 업로드
* <스크립트 1> - spark_producer.py
  * HDFS 특정 폴더에 존재하는 파일을 읽어와 PySpark 로 데이터를 파싱 및 정제하고 spark DataFrame 으로 저장
  * Kafka 큐에 인입 전에 topic에 사용될 key 존재 유무를 체크
  * Kafka 에 메시지 Producer
* <스크립트 2> - kafka_to_mongo_consumer.py
  * Kafka 에서 메시지를 Consumer
  * 해당 메시지를 MongoDB에 적재
    <img width="455" height="322" alt="image" src="https://github.com/user-attachments/assets/b9cc385e-1716-4885-986e-01d1075b9ac7" />

* 분산 DB 시스템으로 사용 기술을 선택할 때 CAP 이론 검토
  * CAP
    * Consistency : 일관성
      * RDB 트랜잭션의 속성인 ACID 에서의 C는 데이터 타입과 제약조건이 일관되게 적용되고 있는지에 대한 확인으로서 분산 DB 시스템의 C와 다른 개념임
    * Availability : 가용성
    * Partition tolerance : 확장 용이성
  * CAP 중 CP 특성을 갖고 있는 Mongo DB 로 선택
    * 대상 데이터셋 Financial Statement Extracts 중 sub.txt 회사 제출 정보 (메타데이터) 가 가변적인 성격으로 필드가 자주 바뀔 수 있겠다는 생각도 Mongo DB를 선택한 이유 중 하나
    * 서비스 운영 관점에서 "P" 를 포기할 수 없는 요소이기 때문에, CP / AP 가 주된 고려사항, AP 성격의 DB 는 Cassandra
    * 장애 상황을 대비한 스택을 결정할 때에 CAP 이론을 사용한다면, 정상 상황까지 고려할 때 PACELC 이론이 적절함을 참고
      * Partition-Availability-Consistency-Else-Latency-Consistency
* 고가용성을 위한 클러스터 구축 (Datanode 3, Datanode 4)
  * kafka1, 2 // mongo db 1, 2, arbiter // spark master, worker
  * docker-compose 를 이용해서 클러스터를 구축
  * 브릿지 네트워크 및 IP Address Management (서브넷, 게이트웨이 지정 가능) 172.20.0.0/16 서브넷 지정
  * Mongo DB ReplicaSet 구성을 위해 동일한 이름으로 설정 (--replSet rs0)
  * Mongo DB Primary 가 죽은 경우 전체 노드 개수가 짝수이면 split-brain 문제로 일관성이 깨지는 경우가 발생할 수 있기 때문에 arbiter (투표전용 노드) 추가
  <img width="642" height="267" alt="image" src="https://github.com/user-attachments/assets/e88f5a45-d72f-4865-adbc-d850950abea1" />
<br>

## 알게 된 내용
* Kafka 심화 이해
  * 메시지 전송 중 오류가 발생할 경우 Consumer 측에서 --reset-offsets 옵션을 통해 특정 시점의 offset 으로 되돌릴 수 있음
  * 선착순과 같이 동시에 처리되면 안되는 ID 등을 메시지의 키로 설정하면 순차처리 및 병렬처리
    * 동일한 ID가 동일한 Key로 설정되니 같은 파티션으로 배정
  * 중간에 컨슈머가 추가/삭제 시 할당된 파티션이 다시 고르게 분배하는 과정을 리밸런싱이라고 하며, 이 과정 중에는 컨슈머가 메시지를 읽을 수 없음
* Zookeeper
  * 분산 App 을 위한 코디네이션 시스템
  * 디렉토리 기반 znode (key-value 형식) 데이터 저장 객체 사용
    * znode 종류는 persistent, ephemeral, sequence
  * 활용점: 클러스터 간 통신용 Queue / 서버 설정 정보 저장소 / 공유 자원 접근 시 글로벌 Lock
* docker-compose 클러스터 구축 시에 의존성 주의
  * kafka1, 2 컨테이너는 Zookeeper 컨테이너 활성화 이후 동작하도록 의존성 설정 등
<br>

## 향후 계획
* Airflow 로 파이프라인 작업 자동화
  * 주기적인 시간에 동작
  * 새로 인입된 파일과 처리 완료된 파일 HDFS 폴더 구분
 
* 파이프라인 검증
  * 어느 정도의 서버 리소스에서는 어느 성능까지 나온다는 것을 확인
  * 데이터 수집 및 처리 정상 작동 검증 (누락된 데이터 없는지)
  * Cluster 환경에서 Kafka 브로커 장애 시 정상 작동 확인
  * Mongo DB 노드 장애 시 정상 작동 확인
<br>
