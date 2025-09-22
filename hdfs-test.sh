#!/bin/bash
# scripts/hdfs-test.sh - HDFS 연결 테스트 스크립트 (Hadoop 설치 환경용)

# HDFS 타겟 파일 경로 (인자로 받을 수 있도록 설정)
TARGET_FILE=${1:-/user/hadoop3/test.json}

echo "=== HDFS 연결 테스트 시작 (Hadoop-enabled 환경) ==="

# 연결 테스트 함수
test_connection() {
  local host=$1
  local port=$2
  local service=$3
  echo "테스트 중: $service ($host:$port)"
  if timeout 10 bash -c "</dev/null >/dev/tcp/$host/$port" 2>/dev/null; then
    echo "✓ $service 연결 성공"
    return 0
  else
    echo "✗ $service 연결 실패"
    return 1
  fi
}

# 1. Hadoop 환경 변수 확인
echo "1. Hadoop 환경 변수 확인..."
echo "HADOOP_HOME: ${HADOOP_HOME:-'설정되지 않음'}"
echo "HADOOP_CONF_DIR: ${HADOOP_CONF_DIR:-'설정되지 않음'}"
echo "PATH: $PATH"

# 2. Hadoop 명령어 사용 가능 여부 확인
echo "2. Hadoop 명령어 테스트..."
if command -v hadoop >/dev/null 2>&1; then
    echo "✓ hadoop 명령어 사용 가능"
    echo "  Hadoop 버전:"
    hadoop version 2>/dev/null | head -n 1 || echo "  버전 조회 실패"
else
    echo "✗ hadoop 명령어 사용 불가. PATH 또는 Hadoop 설치를 확인하세요."
    exit 1
fi

# 3. 네임노드 및 데이터노드 연결 테스트
echo "3. HDFS 노드 연결 테스트..."
test_connection namenode 9000 "HDFS NameNode"
namenode_status=$?

# extra_hosts 또는 네트워크 설정에 따라 호스트 이름으로 접근
for i in 3 4; do
    test_connection datanode${i} 9864 "DataNode${i}" || echo "  DataNode${i} 연결 실패 (Spark 작업에 영향을 줄 수 있음)"
done

# 4. Hadoop 설정 파일 확인
echo "4. Hadoop 설정 파일 확인..."
if [ -f "$HADOOP_CONF_DIR/core-site.xml" ]; then
    echo "✓ core-site.xml 존재"
    grep -A 2 -B 1 fs.defaultFS "$HADOOP_CONF_DIR/core-site.xml" || echo "  fs.defaultFS 설정 없음"
else
    echo "✗ core-site.xml 없음 ($HADOOP_CONF_DIR)"
fi

# 5. HDFS 파일시스템 접근 테스트
if [ $namenode_status -eq 0 ]; then
    echo "5. HDFS 파일시스템 접근 테스트..."
    if hadoop fs -ls / > /tmp/hdfs_ls.log 2>&1; then
        echo "✓ HDFS 루트 디렉토리 접근 성공"
        echo "  루트 디렉토리 내용:"
        cat /tmp/hdfs_ls.log
    else
        echo "✗ HDFS 루트 디렉토리 접근 실패"
        echo "  오류 내용:"
        cat /tmp/hdfs_ls.log
    fi

    # 6. 타겟 파일 존재 확인
    echo "6. 타겟 JSON 파일($TARGET_FILE) 확인..."
    if hadoop fs -test -e "$TARGET_FILE"; then
        echo "✓ $TARGET_FILE 파일 존재"
        echo "  파일 정보:"
        hadoop fs -ls "$TARGET_FILE"
    else
        echo "✗ $TARGET_FILE 파일 없음"
        echo "  상위 디렉토리 확인 시도:"
        hadoop fs -ls "$(dirname "$TARGET_FILE")" 2>/dev/null || echo "  상위 디렉토리 접근 불가"
    fi
else
    echo "5-6. 네임노드 연결 실패로 HDFS 파일시스템 테스트를 건너뜁니다."
fi

# 7. Spark/Kafka 관련 연결 테스트
echo "7. 기타 서비스 연결 테스트..."
test_connection spark-master 7077 "Spark Master"
test_connection 192.168.0.12 9092 "Kafka Broker 1"
test_connection 192.168.0.13 9093 "Kafka Broker 2"


echo "=== HDFS 연결 테스트 완료 ==="

# Clean up
rm -f /tmp/hdfs_ls.log
