#!/bin/bash
# mongodb-auth-fix.sh - MongoDB 인증 문제 해결

set -e

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

log_info "MongoDB 인증 문제 해결 시작"

# 방법 1: 인증 없이 재시작하여 레플리카셋 초기화
log_step "방법 1: 임시로 인증 없이 재시작"

log_info "1. 현재 MongoDB 컨테이너 중지"
docker-compose -f docker-compose-dn3.yml stop mongodb-1


log_info "2. 인증 없이 임시 MongoDB 시작"
docker run -d --name mongodb-temp \
  --network bridge \
  -p 27017:27017 \
  -v $(pwd)/scripts/mongo-keyfile:/etc/mongo/mongo-keyfile:ro \
  -v mongodb-1-data:/data/db \
  -v mongodb-1-config:/data/configdb \
  mongo:7.0 \
  bash -c "
    if [ -f /etc/mongo/mongo-keyfile ]; then
      chmod 400 /etc/mongo/mongo-keyfile
      chown mongodb:mongodb /etc/mongo/mongo-keyfile
    fi
    mongod --replSet rs0 --bind_ip_all --keyFile /etc/mongo/mongo-keyfile
  "

log_info "3. MongoDB 시작 대기 (30초)"
sleep 30

# 연결 테스트
log_info "4. 인증 없이 연결 테스트"
if docker exec mongodb-temp mongosh --port 27017 --eval "db.runCommand({ping: 1})" > /dev/null 2>&1; then
    log_info "✅ 인증 없이 연결 성공"
    
    # 관리자 사용자 생성 (이미 있으면 무시)
    log_info "5. 관리자 사용자 생성/확인"
    docker exec mongodb-temp mongosh --port 27017 --eval "
    try {
        db.getSiblingDB('admin').createUser({
            user: 'admin',
            pwd: 'password123',
            roles: [{ role: 'root', db: 'admin' }]
        });
        print('✅ Admin user created');
    } catch (error) {
        if (error.code === 51003) {
            print('✅ Admin user already exists');
        } else {
            print('❌ Error: ' + error);
        }
    }
    "
    
    # DN4 MongoDB 연결 확인
    log_info "6. DN4 MongoDB 연결 상태 확인"
    if nc -z 192.168.0.13 27018 && nc -z 192.168.0.13 27019; then
        log_info "✅ DN4 MongoDB 서비스들 연결 가능"
        
        # 레플리카셋 초기화
        log_info "7. 레플리카셋 초기화"
        docker exec mongodb-temp mongosh --port 27017 --eval "
        try {
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
                print('✅ Replica set initialized successfully');
            } else {
                print('❌ Replica set initialization failed');
                printjson(result);
            }
        } catch (error) {
            if (error.code === 23) {
                print('⚠️ Replica set already initialized');
            } else {
                print('❌ Error: ' + error);
            }
        }
        "
        
        log_info "8. Primary 선출 대기 (30초)"
        sleep 30
        
        # 레플리카셋 상태 확인
        log_info "9. 레플리카셋 상태 확인"
        docker exec mongodb-temp mongosh --port 27017 --eval "
        try {
            var status = rs.status();
            if (status.ok === 1) {
                print('✅ Replica set is healthy');
                status.members.forEach(function(member) {
                    print('- ' + member.name + ': ' + member.stateStr);
                });
            }
        } catch (error) {
            print('❌ Status error: ' + error);
        }
        "
        
    else
        log_error "❌ DN4 MongoDB 서비스에 연결할 수 없습니다"
        echo "DN4에서 다음 명령어를 실행하세요:"
        echo "docker-compose -f docker-compose-dn4.yml up -d mongodb-2 mongodb-arbiter"
    fi
    
else
    log_error "❌ MongoDB 임시 컨테이너 연결 실패"
fi

log_info "10. 임시 컨테이너 정리 및 원본 서비스 재시작"
docker stop mongodb-temp || true
docker rm mongodb-temp || true

log_info "11. 원본 MongoDB 서비스 재시작"
docker-compose -f docker-compose-dn3.yml up -d mongodb-1

log_info "12. 서비스 시작 대기 (30초)"
sleep 30

log_info "13. 최종 인증 테스트"
if docker exec mongodb-1 mongosh -u admin -p password123 --authenticationDatabase admin --eval "
    try {
        var status = rs.status();
        print('✅ Authentication successful');
        print('Replica set status: ' + status.ok);
        status.members.forEach(function(member) {
            print('- ' + member.name + ': ' + member.stateStr);
        });
    } catch (error) {
        print('❌ Error: ' + error);
    }
"; then
    log_info "🎉 MongoDB 인증 문제 해결 완료!"
else
    log_error "❌ 여전히 인증 문제가 있습니다. 대안 방법을 시도하세요."
fi

echo ""
echo "=== 다음 단계 ==="
echo "1. 인증 테스트: docker exec -it mongodb-1 mongosh -u admin -p password123 --authenticationDatabase admin"
echo "2. 레플리카셋 상태: rs.status()"
echo "3. 다른 서비스 시작: docker-compose -f docker-compose-dn3.yml up -d"