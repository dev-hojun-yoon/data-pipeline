#!/bin/bash
# setup-mongodb.sh - MongoDB 초기 설정 스크립트

# 디렉토리 생성
mkdir -p ./scripts

# MongoDB 키파일 생성 (레플리카셋 인증용)
echo "Creating MongoDB keyfile..."
openssl rand -base64 756 > ./scripts/mongo-keyfile
chmod 400 ./scripts/mongo-keyfile

echo "MongoDB keyfile created at ./scripts/mongo-keyfile"

# 레플리카셋 초기화 스크립트 (사용하지 않음 - 컨테이너에서 직접 처리)
cat > ./scripts/init-replica.js << 'EOF'
// 이 파일은 참조용입니다. 실제 초기화는 컨테이너 command에서 처리됩니다.
print("MongoDB replica set initialization is handled in container startup");
EOF

echo "Setup completed!"
echo ""
echo "Next steps:"
echo "1. Run: docker compose up -d mongodb-1"
echo "2. Wait for MongoDB to initialize (about 1-2 minutes)"
echo "3. Test connection: docker exec -it mongodb-1 mongosh -u admin -p password123 --authenticationDatabase admin"