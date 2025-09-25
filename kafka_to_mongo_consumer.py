#!/usr/bin/env python3
"""
Kafka에서 데이터를 구독하여 MongoDB에 저장하는 Consumer (confluent-kafka 사용)
"""

import json
import os
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
import logging
from datetime import datetime
import time
import argparse
import threading
import sys
import signal

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
    def __init__(self, kafka_hosts, mongo_host='mongodb',
                 mongo_port=27017, mongo_db='financial_db',
                 mongo_user=None, mongo_password=None, batch_size=100,
                 dlq_topic_suffix='_dlq'):
        self.kafka_hosts = kafka_hosts
        self.mongo_host = mongo_host
        self.mongo_port = mongo_port
        self.mongo_db_name = mongo_db
        self.batch_size = batch_size
        self.shutdown_flag = threading.Event()
        self.dlq_topic_suffix = dlq_topic_suffix

        # MongoDB 연결
        self.mongo_client = MongoClient(
            mongo_host,
            mongo_port,
            username=mongo_user,
            password=mongo_password,
            authSource='admin'
        )
        self.db = self.mongo_client[mongo_db]

        # DLQ를 위한 Kafka Producer 생성
        producer_conf = {'bootstrap.servers': self.kafka_hosts}
        self.dlq_producer = Producer(producer_conf)
        logger.info("DLQ용 Kafka Producer 생성 완료")

        self.topic_collection_mapping = {
            'financial_numbers': 'num_data',
            'presentation_links': 'pre_data',
            'submission_info': 'sub_data',
            'tag_info': 'tag_data',
            'readme_info': 'readme_data'
        }
        
        self.data_processors = {
            'financial_numbers': self.process_num_data,
            'presentation_links': self.process_pre_data,
            'submission_info': self.process_sub_data,
            'tag_info': self.process_tag_data,
            'readme_info': self.process_readme_data
        }
        
        self.create_indexes()
        logger.info("KafkaToMongoConsumer 초기화 완료")

    def create_indexes(self):
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
            raise

    def process_num_data(self, data):
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
        processed = data.copy()
        for field in ['custom', 'abstract']:
            if processed.get(field):
                processed[field] = processed[field] == '1'
        return processed

    def process_readme_data(self, data):
        return data.copy()

    def create_consumer(self, topics):
        consumer_conf = {
            'bootstrap.servers': self.kafka_hosts,
            'group.id': 'financial-data-consumer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe(topics)
        return consumer

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"DLQ 메시지 전송 실패: {err}")
        else:
            logger.warning(f"메시지를 토픽 {msg.topic()} [{msg.partition()}]으로 전송 완료.")

    def send_to_dlq(self, topic, failed_message_value, error):
        dlq_topic = f"{topic}{self.dlq_topic_suffix}"
        dlq_message = {
            'original_topic': topic,
            'message_payload': failed_message_value,
            'error_details': str(error),
            'failed_at': datetime.now().isoformat()
        }
        try:
            self.dlq_producer.produce(
                dlq_topic,
                value=json.dumps(dlq_message).encode('utf-8'),
                callback=self.delivery_report
            )
            self.dlq_producer.poll(0)
        except Exception as e:
            logger.error(f"DLQ 전송 중 예외 발생: {e}")

    def process_messages_batch(self, messages, topic):
        if not messages:
            return 0
        
        collection_name = self.topic_collection_mapping.get(topic)
        if not collection_name:
            logger.warning(f"알 수 없는 토픽: {topic}")
            for msg in messages:
                self.send_to_dlq(topic, msg.value().decode('utf-8'), "Unknown topic")
            return 0
        
        collection = self.db[collection_name]
        processor = self.data_processors.get(topic, lambda x: x)
        documents = []
        
        for message in messages:
            try:
                raw_data = json.loads(message.value().decode('utf-8'))
                processed_data = processor(raw_data)
                
                processed_data['created_at'] = datetime.now()
                processed_data['kafka_metadata'] = {
                    'topic': message.topic(),
                    'partition': message.partition(),
                    'offset': message.offset(),
                    'timestamp': message.timestamp()[1] if message.timestamp()[0] != 0 else None
                }
                documents.append(processed_data)
            except Exception as e:
                logger.error(f"메시지 처리 실패 (DLQ로 전송): {str(e)}, Message: {message.value().decode('utf-8')}")
                self.send_to_dlq(topic, message.value().decode('utf-8'), e)

        if not documents:
            return 0

        try:
            result = collection.insert_many(documents, ordered=False)
            logger.info(f"{topic}: {len(result.inserted_ids)}개 문서 삽입 완료")
            return len(result.inserted_ids)
        except BulkWriteError as e:
            inserted_count = e.details.get('nInserted', 0)
            write_errors = e.details.get('writeErrors', [])
            logger.warning(f"{topic}: {inserted_count}개 삽입, {len(write_errors)}개 쓰기 오류 발생")
            for err in write_errors[:3]:
                logger.debug(f"  오류 코드 {err['code']}: {err['errmsg']}")
            return inserted_count
        except Exception as e:
            logger.error(f"배치 삽입 실패 - Topic: {topic}, Error: {str(e)}")
            for doc, msg in zip(documents, messages):
                self.send_to_dlq(topic, msg.value().decode('utf-8'), e)
            return 0

    def consume_topic(self, topic):
        logger.info(f"{topic} 토픽 구독 시작")
        consumer = self.create_consumer([topic])
        message_buffer = []
        total_processed = 0
        try:
            while not self.shutdown_flag.is_set():
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    if message_buffer:
                        processed = self.process_messages_batch(message_buffer, topic)
                        total_processed += processed
                        message_buffer = []
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        break
                
                message_buffer.append(msg)
                if len(message_buffer) >= self.batch_size:
                    processed = self.process_messages_batch(message_buffer, topic)
                    total_processed += processed
                    message_buffer = []
        finally:
            if message_buffer:
                processed = self.process_messages_batch(message_buffer, topic)
                total_processed += processed
            consumer.close()
            logger.info(f"{topic} 토픽 소비 완료 - 총 {total_processed}개 메시지 처리")

    def start_consuming(self):
        topics = list(self.topic_collection_mapping.keys())
        logger.info(f"다음 토픽들 구독 시작: {topics}")
        
        threads = []
        for topic in topics:
            thread = threading.Thread(target=self.consume_topic, args=(topic,))
            thread.start()
            threads.append(thread)
            
        try:
            for thread in threads:
                thread.join()
        except KeyboardInterrupt:
            logger.info("종료 신호 수신")
            self.shutdown_flag.set()

    def shutdown(self):
        logger.info("Consumer 종료 중...")
        self.shutdown_flag.set()
        if self.dlq_producer:
            self.dlq_producer.flush()
            logger.info("DLQ Producer 종료 완료")
        self.mongo_client.close()
        logger.info("Consumer 종료 완료")

consumer_instance = None

def signal_handler(signum, frame):
    logger.info(f"신호 {signum} 수신, 종료 준비 중...")
    global consumer_instance
    if consumer_instance:
        consumer_instance.shutdown()
    sys.exit(0)

def main():
    global consumer_instance
    
    parser = argparse.ArgumentParser(description='Kafka to MongoDB Consumer')
    parser.add_argument('--kafka-hosts', default=os.getenv('KAFKA_HOSTS'), help='Kafka broker hosts')
    parser.add_argument('--mongo-host', default=os.getenv('MONGO_HOST', 'mongodb'), help='MongoDB host')
    parser.add_argument('--mongo-port', default=int(os.getenv('MONGO_PORT', 27017)), type=int, help='MongoDB port')
    parser.add_argument('--mongo-db', default=os.getenv('MONGO_DB', 'financial_db'), help='MongoDB database name')
    parser.add_argument('--mongo-user', default=os.getenv('MONGO_USER'), help='MongoDB username')
    parser.add_argument('--mongo-password', default=os.getenv('MONGO_PASSWORD'), help='MongoDB password')
    parser.add_argument('--batch-size', default=int(os.getenv('BATCH_SIZE', 100)), type=int, help='Batch size for processing')
    parser.add_argument('--topics', nargs='+', help='Specific topics to consume')
    
    args = parser.parse_args()

    if not args.kafka_hosts:
        logger.error("KAFKA_HOSTS 환경 변수 또는 --kafka-hosts 인자가 설정되지 않았습니다.")
        sys.exit(1)

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

        except KafkaException as e:
            logger.warning(f"Could not connect to Kafka: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            continue
        except Exception as e:
            logger.error(f"Consumer failed with an unexpected error: {e}", exc_info=True)
            return 1
    
    logger.error("Failed to connect to Kafka after multiple retries. Exiting.")
    return 1

if __name__ == "__main__":
    exit(main())