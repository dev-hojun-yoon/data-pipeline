#!/bin/bash
# fix-mongodb-replica.sh - MongoDB 레플리카셋 초기화 문제 해결

set -e

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_info "MongoDB 레플리카셋 초기화 문제 해결 시작"

# 1단계: 현재 상태 확인
log_info "1. 현재 MongoDB 상태 확인"
docker exec mongodb-1 mongosh -u admin -p password123 --authenticationDatabase admin --eval "
try {
  var status = rs.status();
  print('Replica set is already initialized');
  printjson(status);
} catch (error) {
  if (error.code === 94) {
    print('Replica set not yet initialized - this is expected');
  } else {
    print('Error: ' + error);
  }
}
" || log_warn "MongoDB 접속 중 에러 발생 (정상적일 수 있음)"

# 2단계: DN4 MongoDB 서비스 상태 확인
log_info "2. DN4 MongoDB 서비스 상태 확인"
echo "DN4의 MongoDB 서비스들이 시작되었는지 확인하세요:"
echo "- mongodb-2 (포트 27018)"
echo "- mongodb-arbiter (포트 27019)"
echo ""
read -p "DN4의 MongoDB 서비스들이 실행중인가요? (y/n): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    log_error "먼저 DN4에서 MongoDB 서비스들을 시작하세요:"
    echo "docker compose -f docker-compose-dn4.yml up -d mongodb-2 mongodb-arbiter"
    exit 1
fi

# 3단계: 네트워크 연결 확인
log_info "3. DN4 MongoDB 서비스 연결 테스트"
if nc -z 192.168.0.13 27018; then
    log_info "✅ mongodb-2 연결 성공 (192.168.0.13:27018)"
else
    log_error "❌ mongodb-2 연결 실패"
    echo "DN4에서 다음 명령어로 확인: docker ps | grep mongodb-2"
    exit 1
fi

if nc -z 192.168.0.13 27019; then
    log_info "✅ mongodb-arbiter 연결 성공 (192.168.0.13:27019)"
else
    log_error "❌ mongodb-arbiter 연결 실패"
    echo "DN4에서 다음 명령어로 확인: docker ps | grep mongodb-arbiter"
    exit 1
fi

# 4단계: 레플리카셋 수동 초기화
log_info "4. 레플리카셋 수동 초기화 시작"
docker exec mongodb-1 mongosh -u admin -p password123 --authenticationDatabase admin --eval "
try {
  print('Initializing replica set...');
  var config = {
    _id: 'rs0',
    members: [
      { _id: 0, host: '192.168.0.12:27017', priority: 2 },
      { _id: 1, host: '192.168.0.13:27018', priority: 1 },
      { _id: 2, host: '192.168.0.13:27019', arbiterOnly: true }
    ]
  };
  
  var result = rs.initiate(config);
  if (result.ok === 1) {
    print('✅ Replica set initialization successful');
  } else {
    print('❌ Replica set initialization failed');
    printjson(result);
  }
} catch (error) {
  if (error.code === 23) {
    print('⚠️ Replica set already initialized');
  } else {
    print('❌ Error during initialization: ' + error);
  }
}
"

# 5단계: 초기화 대기 및 확인
log_info "5. Primary 선출 대기 (30초)"
sleep 30

# 6단계: 레플리카셋 상태 최종 확인
log_info "6. 레플리카셋 상태 최종 확인"
docker exec mongodb-1 mongosh -u admin -p password123 --authenticationDatabase admin --eval "
try {
  print('=== Replica Set Status ===');
  var status = rs.status();
  
  if (status.ok === 1) {
    print('✅ Replica set is healthy');
    status.members.forEach(function(member) {
      var icon = member.stateStr === 'PRIMARY' ? '👑' : 
                member.stateStr === 'SECONDARY' ? '👥' : 
                member.stateStr === 'ARBITER' ? '⚖️' : '❓';
      print(icon + ' ' + member.name + ' - ' + member.stateStr + ' (health: ' + member.health + ')');
    });
    
    // Primary 확인
    var primary = status.members.find(m => m.stateStr === 'PRIMARY');
    if (primary) {
      print('\\n🎉 Current PRIMARY: ' + primary.name);
    } else {
      print('\\n⚠️ No PRIMARY found yet, waiting for election...');
    }
    
  } else {
    print('❌ Replica set status error');
    printjson(status);
  }
} catch (error) {
  print('❌ Error getting replica set status: ' + error);
}
"

# 7단계: Oplog 생성 확인
log_info "7. Oplog 생성 확인"
docker exec mongodb-1 mongosh -u admin -p password123 --authenticationDatabase admin --eval "
try {
  var oplogStats = db.getSiblingDB('local').oplog.rs.stats();
  print('✅ Oplog collection exists');
  print('Oplog size: ' + (oplogStats.size / 1024 / 1024).toFixed(2) + ' MB');
} catch (error) {
  print('⚠️ Oplog not ready yet: ' + error);
}
"

log_info "MongoDB 레플리카셋 초기화 완료!"
echo ""
echo "다음 단계:"
echo "1. 몇 분 후 에러 로그가 사라지는지 확인: docker logs -f mongodb-1"
echo "2. 다른 서비스들 시작: docker compose -f docker-compose-dn3.yml up -d"
echo "3. Consumer 애플리케이션의 MongoDB 연결 문자열이 레플리카셋 형식인지 확인"