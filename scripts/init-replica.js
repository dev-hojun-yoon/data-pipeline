
// MongoDB Replica Set 초기화 스크립트

// 현재 레플리카 설정을 가져오거나 없으면 null
var config = rs.conf();

if (config == null) {
  print("Initiating new replica set...");
  rs.initiate({
    _id: "rs0",
    members: [
      { _id: 0, host: "192.168.0.12:27017", priority: 2 },
      { _id: 1, host: "192.168.0.13:27018", priority: 1 },
      { _id: 2, host: "192.168.0.13:27019", arbiterOnly: true }
    ]
  });
} else {
  print("Replica set already exists.");
}

// 잠시 기다린 후 Primary 노드가 선출되었는지 확인
print("Waiting for replica set to have a primary...");
while (!rs.status().members.some(m => m.stateStr === 'PRIMARY')) {
  sleep(1000);
}
print("Primary elected. Proceeding with user creation.");

// 사용자 생성 (Primary 노드에서만 실행)
db = db.getSiblingDB('financial_db');

var user = db.getUser('admin');
if (!user) {
    print("Creating user 'admin'...");
    db.createUser({
        user: 'admin',
        pwd: 'password123',
        roles: [{ role: 'root', db: 'admin' }]
    });
} else {
    print("User 'admin' already exists.");
}

// 초기 설정 확인
printjson(rs.conf());
printjson(rs.status());
