#!/usr/bin/env python3
"""
Kafka에서 데이터를 구독하여 MongoDB에 저장하는 Consumer
"""

import json
import os
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError, BulkWriteError
import logging
from datetime import datetime
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor
import signal
import sys

# 로깅 설정
log_dir = "/app/logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "kafka_consumer.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class KafkaToMongoConsumer:
    def __init__(self, kafka_hosts='kafka:29092', mongo_host='mongodb',
                 mongo_port=27017, mongo_db='financial_db',
                 mongo_user=None, mongo_password=None, batch_size=100):
        """
        Kafka to MongoDB Consumer 초기화
        """
        self.kafka_hosts = kafka_hosts.split(',')
        self.mongo_host = mongo_host
        self.mongo_port = mongo_port
        self.mongo_db_name = mongo_db
        self.batch_size = batch_size
        self.shutdown_flag = threading.Event()
        
        # MongoDB 연결
        self.mongo_client = MongoClient(
            mongo_host,
            mongo_port,
            username=mongo_user,
            password=mongo_password,
            authSource='admin'
        )
        self.db = self.mongo_client[mongo_db]
        
        # 토픽과 컬렉션 매핑
        self.topic_collection_mapping = {
            'financial_numbers': 'num_data',
            'presentation_links': 'pre_data',
            'submission_info': 'sub_data',
            'tag_info': 'tag_data',
            'readme_info': 'readme_data'
        }
        
        # 데이터 타입 변환 함수 매핑
        self.data_processors = {
            'financial_numbers': self.process_num_data,
            'presentation_links': self.process_pre_data,
            'submission_info': self.process_sub_data,
            'tag_info': self.process_tag_data,
            'readme_info': self.process_readme_data
        }
        
        # MongoDB 인덱스 생성
        self.create_indexes()
        
        logger.info("KafkaToMongoConsumer 초기화 완료")
    
    def create_indexes(self):
        """ MongoDB 컬렉션별 인덱스를 생성합니다. """
        try:
            self.db.num_data.create_index([("adsh", ASCENDING)])
            self.db.num_data.create_index([("tag", ASCENDING)])
            self.db.num_data.create_index([("ddate", ASCENDING)])
            self.db.pre_data.create_index([("adsh", ASCENDING)])
            self.db.pre_data.create_index([("tag", ASCENDING)])
            self.db.pre_data.create_index([("stmt", ASCENDING)])
            self.db.sub_data.create_index([("adsh", ASCENDING)], unique=True)
            self.db.sub_data.create_index([("cik", ASCENDING)])
            self.db.sub_data.create_index([("name", ASCENDING)])
            self.db.tag_data.create_index([("tag", ASCENDING)])
            self.db.tag_data.create_index([("version", ASCENDING)])
            logger.info("MongoDB 인덱스 생성 확인 및 적용 완료")
        except Exception as e:
            logger.error(f"인덱스 생성 실패: {str(e)}")
    
    def process_num_data(self, data):
        """재무 수치 데이터 처리"""
        processed = data.copy()
        if processed.get('qtrs'):
            try:
                processed['qtrs'] = int(processed['qtrs'])
            except (ValueError, TypeError):
                processed['qtrs'] = 0
        if processed.get('value'):
            try:
                processed['value'] = float(processed['value'])
            except (ValueError, TypeError):
                processed['value'] = None
        if processed.get('ddate'):
            try:
                processed['ddate'] = datetime.strptime(processed['ddate'], '%Y%m%d')
            except (ValueError, TypeError):
                processed['ddate'] = None
        return processed
    
    def process_pre_data(self, data):
        """프레젠테이션 링크 데이터 처리"""
        processed = data.copy()
        for field in ['report', 'line']:
            if processed.get(field):
                try:
                    processed[field] = int(processed[field])
                except (ValueError, TypeError):
                    processed[field] = None
        if processed.get('inpth'):
            processed['inpth'] = processed['inpth'] == '1'
        return processed
    
    def process_sub_data(self, data):
        """회사 제출 정보 데이터 처리"""
        processed = data.copy()
        date_fields = ['period', 'filed', 'accepted']
        for field in date_fields:
            if processed.get(field):
                try:
                    if field == 'period':
                        processed[field] = datetime.strptime(processed[field], '%Y%m%d')
                    elif field == 'accepted':
                        if len(processed[field]) >= 8:
                            date_part = processed[field][:8]
                            processed[field] = datetime.strptime(date_part, '%Y%m%d')
                    else:
                        processed[field] = datetime.strptime(processed[field], '%Y%m%d')
                except (ValueError, TypeError):
                    processed[field] = None
        if processed.get('fy'):
            try:
                processed['fy'] = int(processed['fy'])
            except (ValueError, TypeError):
                processed['fy'] = None
        for field in ['wksi', 'detail']:
            if processed.get(field):
                processed[field] = processed[field] == '1'
        return processed
    
    def process_tag_data(self, data):
        """태그 정보 데이터 처리"""
        processed = data.copy()
        for field in ['custom', 'abstract']:
            if processed.get(field):
                processed[field] = processed[field] == '1'
        return processed
    
    def process_readme_data(self, data):
        """README 데이터 처리"""
        return data.copy()
    
    def create_consumer(self, topics):
        """Kafka Consumer 생성"""
        return KafkaConsumer(
            *topics,
            bootstrap_servers=self.kafka_hosts,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='financial-data-consumer',
            consumer_timeout_ms=10000,
            max_poll_records=500
        )
    
    def process_messages_batch(self, messages, topic):
        """메시지 배치 처리"""
        if not messages:
            return 0
        try:
            collection_name = self.topic_collection_mapping.get(topic)
            if not collection_name:
                logger.warning(f"알 수 없는 토픽: {topic}")
                return 0
            collection = self.db[collection_name]
            processor = self.data_processors.get(topic, lambda x: x)
            documents = []
            for message in messages:
                try:
                    raw_data = message.value
                    processed_data = processor(raw_data)
                    processed_data['created_at'] = datetime.now()
                    processed_data['kafka_metadata'] = {
                        'topic': message.topic,
                        'partition': message.partition,
                        'offset': message.offset,
                        'timestamp': message.timestamp
                    }
                    documents.append(processed_data)
                except Exception as e:
                    logger.error(f"메시지 처리 실패: {str(e)}, Message: {message.value}")
            if documents:
                try:
                    result = collection.insert_many(documents, ordered=False)
                    logger.info(f"{topic}: {len(result.inserted_ids)}개 문서 삽입 완료")
                    return len(result.inserted_ids)
                except BulkWriteError as e:
                    inserted_count = e.details.get('nInserted', 0)
                    logger.warning(f"{topic}: {inserted_count}개 삽입, {len(e.details.get('writeErrors', []))}개 오류")
                    return inserted_count
            return 0
        except Exception as e:
            logger.error(f"배치 처리 실패 - Topic: {topic}, Error: {str(e)}")
            return 0
    
    def consume_topic(self, topic):
        """특정 토픽 구독 및 처리"""
        logger.info(f"{topic} 토픽 구독 시작")
        consumer = self.create_consumer([topic])
        message_buffer = []
        total_processed = 0
        try:
            while not self.shutdown_flag.is_set():
                try:
                    message_batch = consumer.poll(timeout_ms=5000)
                    if not message_batch:
                        if message_buffer:
                            processed = self.process_messages_batch(message_buffer, topic)
                            total_processed += processed
                            message_buffer = []
                        continue
                    for topic_partition, messages in message_batch.items():
                        message_buffer.extend(messages)
                    if len(message_buffer) >= self.batch_size:
                        processed = self.process_messages_batch(message_buffer, topic)
                        total_processed += processed
                        message_buffer = []
                except Exception as e:
                    logger.error(f"{topic} 소비 중 오류: {str(e)}")
                    time.sleep(5)
        finally:
            if message_buffer:
                processed = self.process_messages_batch(message_buffer, topic)
                total_processed += processed
            consumer.close()
            logger.info(f"{topic} 토픽 소비 완료 - 총 {total_processed}개 메시지 처리")
    
    def start_consuming(self):
        """모든 토픽 구독 시작"""
        topics = list(self.topic_collection_mapping.keys())
        logger.info(f"다음 토픽들 구독 시작: {topics}")
        with ThreadPoolExecutor(max_workers=len(topics)) as executor:
            futures = [executor.submit(self.consume_topic, topic) for topic in topics]
            try:
                for future in futures:
                    future.result()
            except KeyboardInterrupt:
                logger.info("종료 신호 수신")
                self.shutdown_flag.set()
    
    def shutdown(self):
        """정상 종료"""
        logger.info("Consumer 종료 중...")
        self.shutdown_flag.set()
        self.mongo_client.close()
        logger.info("Consumer 종료 완료")

def signal_handler(signum, frame):
    """시그널 핸들러"""
    logger.info(f"신호 {signum} 수신, 종료 준비 중...")
    global consumer_instance
    if consumer_instance:
        consumer_instance.shutdown()
    sys.exit(0)

def main():
    global consumer_instance
    
    kafka_hosts = os.getenv('KAFKA_HOSTS', 'kafka:29092')
    mongo_host = os.getenv('MONGO_HOST', 'mongodb')
    mongo_port = int(os.getenv('MONGO_PORT', 27017))
    mongo_db = os.getenv('MONGO_DB', 'financial_db')
    mongo_user = os.getenv('MONGO_USER')
    mongo_password = os.getenv('MONGO_PASSWORD')
    batch_size = int(os.getenv('BATCH_SIZE', 100))

    parser = argparse.ArgumentParser(description='Kafka to MongoDB Consumer')
    parser.add_argument('--kafka-hosts', default=kafka_hosts, help='Kafka broker hosts')
    parser.add_argument('--mongo-host', default=mongo_host, help='MongoDB host')
    parser.add_argument('--mongo-port', default=mongo_port, type=int, help='MongoDB port')
    parser.add_argument('--mongo-db', default=mongo_db, help='MongoDB database name')
    parser.add_argument('--mongo-user', default=mongo_user, help='MongoDB username')
    parser.add_argument('--mongo-password', default=mongo_password, help='MongoDB password')
    parser.add_argument('--batch-size', default=batch_size, type=int, help='Batch size for processing')
    parser.add_argument('--topics', nargs='+', help='Specific topics to consume')
    
    args = parser.parse_args()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    retries = 10
    wait_time = 15

    for i in range(retries):
        try:
            logger.info(f"Attempting to start consumer... Try {i+1}/{retries}")
            
            consumer_instance = KafkaToMongoConsumer(
                kafka_hosts=args.kafka_hosts,
                mongo_host=args.mongo_host,
                mongo_port=args.mongo_port,
                mongo_db=args.mongo_db,
                mongo_user=args.mongo_user,
                mongo_password=args.mongo_password,
                batch_size=args.batch_size
            )
            
            if args.topics:
                filtered_mapping = {k: v for k, v in consumer_instance.topic_collection_mapping.items() if k in args.topics}
                consumer_instance.topic_collection_mapping = filtered_mapping
                logger.info(f"지정된 토픽만 처리: {args.topics}")
            
            consumer_instance.start_consuming()
            
            logger.info("Consumption finished.")
            return 0

        except NoBrokersAvailable:
            logger.warning(f"Could not connect to Kafka. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            continue
        except Exception as e:
            logger.error(f"Consumer failed with an unexpected error: {str(e)}")
            return 1
    
    logger.error("Failed to connect to Kafka after multiple retries. Exiting.")
    return 1

if __name__ == "__main__":
    consumer_instance = None
    exit(main())
