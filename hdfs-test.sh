#!/bin/bash
# scripts/hdfs-test.sh - HDFS 연결 테스트 스크립트 (bitnami/spark 컨테이너용)

echo "=== HDFS 연결 테스트 시작 (bitnami/spark 환경) ==="

# 연결 테스트 함수 (nc 없이)
test_connection() {
  local host=$1
  local port=$2
  local service=$3
  echo "테스트 중: $service ($host:$port)"
  
  # timeout과 /dev/tcp를 사용한 연결 테스트
  if timeout 10 bash -c "cat < /dev/null > /dev/tcp/$host/$port" 2>/dev/null; then
    echo "✓ $service 연결 성공"
    return 0
  else
    echo "✗ $service 연결 실패"
    return 1
  fi
}

# 1. 네임노드 연결 테스트
echo "1. HDFS 네임노드 연결 테스트..."
test_connection namenode 9000 "HDFS NameNode"
namenode_status=$?

# 2. 데이터노드들 연결 테스트 (선택적)
echo "2. 데이터노드들 연결 테스트..."
for i in 1 2 3 4; do
    test_connection datanode${i} 9864 "DataNode${i}" || echo "  DataNode${i} 연결 실패 (정상일 수 있음)"
done

# 3. Hadoop 환경 변수 확인
echo "3. Hadoop 환경 변수 확인..."
echo "HADOOP_CONF_DIR: $HADOOP_CONF_DIR"
echo "JAVA_HOME: $JAVA_HOME"
echo "SPARK_HOME: $SPARK_HOME"

# 4. Hadoop 설정 파일 확인
echo "4. Hadoop 설정 파일 확인..."
if [ -f "/etc/hadoop/core-site.xml" ]; then
    echo "✓ core-site.xml 존재"
    echo "  defaultFS 설정:"
    grep -A1 -B1 "fs.defaultFS" /etc/hadoop/core-site.xml | grep -v "^--$" || echo "  fs.defaultFS 설정 없음"
else
    echo "✗ core-site.xml 없음"
fi

if [ -f "/etc/hadoop/hdfs-site.xml" ]; then
    echo "✓ hdfs-site.xml 존재"
else
    echo "✗ hdfs-site.xml 없음"
fi

# 5. Hadoop 명령어 사용 가능 여부 확인
echo "5. Hadoop 명령어 테스트..."
if command -v hadoop >/dev/null 2>&1; then
    echo "✓ hadoop 명령어 사용 가능"
    echo "  Hadoop 버전:"
    timeout 10 hadoop version | head -1 || echo "  버전 조회 실패"
else
    echo "✗ hadoop 명령어 사용 불가"
fi

# 6. HDFS 파일시스템 테스트 (네임노드 연결이 성공한 경우에만)
if [ $namenode_status -eq 0 ]; then
    echo "6. HDFS 파일시스템 접근 테스트..."
    if timeout 30 hadoop fs -ls / > /tmp/hdfs_ls.out 2>&1; then
        echo "✓ HDFS 루트 디렉토리 접근 성공"
        echo "  루트 디렉토리 내용:"
        cat /tmp/hdfs_ls.out
    else
        echo "✗ HDFS 루트 디렉토리 접근 실패"
        echo "  오류 내용:"
        cat /tmp/hdfs_ls.out
    fi
    
    # 7. 타겟 파일 존재 확인
    echo "7. 타겟 JSON 파일 확인..."
    target_file="/user/hadoop3/test.json"
    if timeout 30 hadoop fs -test -e "$target_file" 2>/dev/null; then
        echo "✓ $target_file 파일 존재"
        echo "  파일 정보:"
        timeout 15 hadoop fs -ls "$target_file" 2>/dev/null || echo "  파일 정보 조회 실패"
        echo "  파일 크기:"
        timeout 15 hadoop fs -du -h "$target_file" 2>/dev/null || echo "  파일 크기 조회 실패"
    else
        echo "✗ $target_file 파일 없음"
        echo "  상위 디렉토리 확인:"
        timeout 15 hadoop fs -ls /user/hadoop3/ 2>/dev/null || echo "  /user/hadoop3/ 디렉토리 접근 불가"
        timeout 15 hadoop fs -ls /user/ 2>/dev/null || echo "  /user/ 디렉토리 접근 불가"
    fi
else
    echo "6-7. 네임노드 연결 실패로 HDFS 파일시스템 테스트 건너뜀"
fi

# 8. Kafka 브로커 연결 테스트
echo "8. Kafka 브로커 연결 테스트..."
test_connection 192.168.0.12 9092 "Kafka Broker 1"
test_connection 192.168.0.13 9093 "Kafka Broker 2"

# 9. Spark 마스터 연결 테스트
echo "9. Spark 마스터 연결 테스트..."
test_connection spark-master 7077 "Spark Master"
test_connection spark-master 8080 "Spark Master Web UI"

echo "=== HDFS 연결 테스트 완료 ==="

# 테스트 결과 요약
echo ""
echo "=== 테스트 결과 요약 ==="
if [ $namenode_status -eq 0 ]; then
    echo "✓ 기본 HDFS 연결: 성공"
else
    echo "✗ 기본 HDFS 연결: 실패"
fi

# 클린업
rm -f /tmp/hdfs_ls.out /tmp/hdfs_test.out 2>/dev/null