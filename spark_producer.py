import argparse
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType, BooleanType, StructType, StructField
import logging

# 로깅 설정
log_dir = "/app/logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "spark_producer.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_schemas():
    """각 데이터 타입에 대한 Spark 스키마를 반환합니다."""
    return {
        'num.txt': StructType([
            StructField("adsh", StringType(), True), 
            StructField("tag", StringType(), True),
            StructField("version", StringType(), True), 
            StructField("coreg", StringType(), True),
            StructField("ddate", StringType(), True), 
            StructField("qtrs", IntegerType(), True),
            StructField("uom", StringType(), True), 
            StructField("value", FloatType(), True),
            StructField("footnote", StringType(), True),
        ]),
        'pre.txt': StructType([
            StructField("adsh", StringType(), True), 
            StructField("report", IntegerType(), True),
            StructField("line", IntegerType(), True), 
            StructField("stmt", StringType(), True),
            StructField("inpth", BooleanType(), True), 
            StructField("rfile", StringType(), True),
            StructField("tag", StringType(), True), 
            StructField("version", StringType(), True),
            StructField("plabel", StringType(), True),
        ]),
        'sub.txt': StructType([
            StructField("adsh", StringType(), True), 
            StructField("cik", IntegerType(), True),
            StructField("name", StringType(), True), 
            StructField("sic", IntegerType(), True),
            StructField("countryba", StringType(), True), 
            StructField("stprba", StringType(), True),
            StructField("cityba", StringType(), True), 
            StructField("zipba", StringType(), True),
            StructField("baszip", StringType(), True), 
            StructField("countryinc", StringType(), True),
            StructField("stprinc", StringType(), True), 
            StructField("ein", StringType(), True),
            StructField("former", StringType(), True), 
            StructField("changed", StringType(), True),
            StructField("afs", StringType(), True), 
            StructField("wksi", StringType(), True),
            StructField("fye", StringType(), True), 
            StructField("form", StringType(), True),
            StructField("period", StringType(), True), 
            StructField("fy", IntegerType(), True),
            StructField("fp", StringType(), True), 
            StructField("filed", StringType(), True),
            StructField("accepted", StringType(), True), 
            StructField("prevrpt", BooleanType(), True),
            StructField("detail", StringType(), True), 
            StructField("instance", StringType(), True),
            StructField("nciks", IntegerType(), True), 
            StructField("aciks", StringType(), True),
        ]),
        'tag.txt': StructType([
            StructField("tag", StringType(), True), 
            StructField("version", StringType(), True),
            StructField("custom", BooleanType(), True), 
            StructField("abstract", BooleanType(), True),
            StructField("datatype", StringType(), True), 
            StructField("iord", StringType(), True),
            StructField("crdr", StringType(), True), 
            StructField("tlabel", StringType(), True),
            StructField("doc", StringType(), True),
        ])
    }

def convert_value(value, data_type):
    """값을 적절한 데이터 타입으로 변환합니다."""
    if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
        return None
    
    try:
        if isinstance(data_type, IntegerType):
            # 문자열을 정수로 변환
            if isinstance(value, str):
                value = value.strip()
            return int(float(value))  # float을 거쳐서 변환 (소수점이 있을 수 있음)
        elif isinstance(data_type, FloatType):
            if isinstance(value, str):
                value = value.strip()
            return float(value)
        elif isinstance(data_type, BooleanType):
            if isinstance(value, str):
                value = value.strip().lower()
                return value in ('true', '1', 'yes', 't', 'y')
            return bool(value)
        else:
            # StringType인 경우
            if isinstance(value, str):
                return value.strip()
            return str(value)
    except (ValueError, AttributeError, TypeError) as e:
        logger.warning(f"데이터 타입 변환 실패: '{value}' -> {data_type.__class__.__name__}: {str(e)}")
        return None

def process_row(row_data, schema):
    """행 데이터를 스키마에 맞게 변환합니다."""
    converted_row = []
    for i, field in enumerate(schema.fields):
        if i < len(row_data):
            converted_value = convert_value(row_data[i], field.dataType)
            converted_row.append(converted_value)
        else:
            converted_row.append(None)
    
    # 디버깅을 위한 로깅 (첫 번째 행만)
    if len(converted_row) > 0:
        logger.debug(f"변환된 행 데이터: {dict(zip([f.name for f in schema.fields], converted_row))}")
    
    return converted_row

def adjust_schema_to_data(header, schema, topic):
    """실제 데이터 헤더에 맞게 스키마를 조정합니다."""
    header_count = len(header)
    schema_count = len(schema.fields)
    
    logger.info(f"토픽 {topic}: 헤더 필드 수 {header_count}, 스키마 필드 수 {schema_count}")
    logger.info(f"헤더: {header}")
    logger.info(f"스키마 필드명: {[f.name for f in schema.fields]}")
    
    if header_count == schema_count:
        return schema
    
    # 헤더가 스키마보다 적은 경우: 스키마를 헤더 크기로 축소
    if header_count < schema_count:
        logger.warning(f"토픽 {topic}: 헤더 필드가 스키마보다 적습니다. 스키마를 {header_count}개로 축소합니다.")
        adjusted_fields = schema.fields[:header_count]
        return StructType(adjusted_fields)
    
    # 헤더가 스키마보다 많은 경우: 추가 필드를 StringType으로 처리
    else:
        logger.warning(f"토픽 {topic}: 헤더 필드가 스키마보다 많습니다. 추가 필드를 문자열로 처리합니다.")
        adjusted_fields = list(schema.fields)
        
        # 추가 필드를 문자열 타입으로 추가
        for i in range(schema_count, header_count):
            if i < len(header):
                field_name = header[i].strip()
            else:
                field_name = f"extra_field_{i}"
            adjusted_fields.append(StructField(field_name, StringType(), True))
        
        return StructType(adjusted_fields)

def validate_and_log_data(df, topic, schema):
    """파싱된 DataFrame의 데이터 품질을 검증하고 로깅합니다."""
    logger.info(f"=== 토픽 {topic} 데이터 검증 시작 ===")
    
    # 기본 통계
    total_count = df.count()
    logger.info(f"총 레코드 수: {total_count}")
    
    if total_count == 0:
        logger.warning("데이터가 없습니다.")
        return False
    
    # 각 컬럼별 null 값 검사
    logger.info("컬럼별 NULL 값 통계:")
    for field in schema.fields:
        try:
            null_count = df.filter(col(field.name).isNull()).count()
            null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
            logger.info(f"  {field.name}: {null_count}개 ({null_percentage:.2f}%)")
            
            if null_percentage > 80:
                logger.warning(f"  경고: {field.name} 컬럼의 NULL 비율이 80%를 초과합니다.")
        except Exception as e:
            logger.warning(f"  {field.name} 컬럼 NULL 검사 실패: {str(e)}")
    
    # 샘플 데이터 출력 (처음 3행)
    logger.info("샘플 데이터 (처음 3행):")
    try:
        sample_rows = df.limit(3).collect()
        for i, row in enumerate(sample_rows, 1):
            row_dict = row.asDict()
            logger.info(f"  행 {i}: {row_dict}")
    except Exception as e:
        logger.warning(f"샘플 데이터 수집 실패: {str(e)}")
    
    # 숫자형 컬럼의 기본 통계
    numeric_columns = [field.name for field in schema.fields 
                      if isinstance(field.dataType, (IntegerType, FloatType))]
    
    if numeric_columns:
        logger.info("숫자형 컬럼 기본 통계:")
        for col_name in numeric_columns:
            try:
                stats = df.select(col_name).describe().collect()
                stats_dict = {row['summary']: row[col_name] for row in stats}
                logger.info(f"  {col_name}: {stats_dict}")
            except Exception as e:
                logger.warning(f"  {col_name} 통계 계산 실패: {str(e)}")
    
    logger.info(f"=== 토픽 {topic} 데이터 검증 완료 ===")
    return True

def parse_tsv_line(line):
    """TSV 라인을 탭으로 분리하고 정리합니다."""
    # 탭으로 분리
    fields = line.split('\t')
    
    # 각 필드의 공백 제거 및 빈 문자열 처리
    cleaned_fields = []
    for field in fields:
        cleaned = field.strip() if field else ''
        # 빈 문자열을 None으로 변환하지 않고 그대로 유지 (나중에 convert_value에서 처리)
        cleaned_fields.append(cleaned)
    
    return cleaned_fields

def process_and_send(spark, kafka_bootstrap_servers, topic, tsv_data, schema):
    """TSV 데이터를 파싱하고 Kafka로 전송합니다."""
    if not tsv_data:
        logger.warning(f"토픽 {topic}에 대한 데이터가 비어있습니다.")
        return

    try:
        # TSV 데이터를 줄 단위로 분리
        lines = tsv_data.strip().split('\n')
        lines = [line for line in lines if line.strip()]  # 빈 줄 제거
        
        logger.info(f"토픽 {topic}: 원본 데이터 {len(lines)}줄 (헤더 포함)")
        
        if len(lines) < 2:
            logger.warning(f"토픽 {topic}에 데이터가 없습니다 (헤더만 존재하거나 데이터 없음).")
            return
        
        # 헤더 파싱
        header = parse_tsv_line(lines[0])
        logger.info(f"파싱된 헤더 컬럼 ({len(header)}개): {header}")
        
        # 스키마를 실제 데이터에 맞게 조정
        adjusted_schema = adjust_schema_to_data(header, schema, topic)
        schema_fields = [field.name for field in adjusted_schema.fields]
        logger.info(f"조정된 스키마 필드 ({len(schema_fields)}개): {schema_fields}")
        
        # 데이터 라인들 파싱 (헤더 제외)
        data_lines = lines[1:]
        logger.info(f"데이터 라인 수: {len(data_lines)}")
        
        # 파싱된 데이터를 저장할 리스트
        parsed_data = []
        invalid_lines = []
        
        for line_num, line in enumerate(data_lines, start=2):  # 헤더가 1번째 라인이므로 2부터 시작
            try:
                parsed_line = parse_tsv_line(line)
                
                # 필드 수 검증
                expected_fields = len(adjusted_schema.fields)
                actual_fields = len(parsed_line)
                
                if actual_fields == expected_fields:
                    # 데이터 타입 변환
                    converted_row = process_row(parsed_line, adjusted_schema)
                    parsed_data.append(converted_row)
                    
                    # 첫 번째 유효한 행의 파싱 결과 로깅
                    if len(parsed_data) == 1:
                        sample_dict = dict(zip(schema_fields, converted_row))
                        logger.info(f"첫 번째 파싱 결과 예시: {sample_dict}")
                        
                elif actual_fields < expected_fields:
                    # 부족한 필드를 None으로 채우기
                    while len(parsed_line) < expected_fields:
                        parsed_line.append('')
                    converted_row = process_row(parsed_line, adjusted_schema)
                    parsed_data.append(converted_row)
                    logger.debug(f"라인 {line_num}: 필드 부족으로 None 값으로 채움")
                    
                elif actual_fields > expected_fields:
                    # 초과 필드는 제거
                    trimmed_line = parsed_line[:expected_fields]
                    converted_row = process_row(trimmed_line, adjusted_schema)
                    parsed_data.append(converted_row)
                    logger.debug(f"라인 {line_num}: 초과 필드 제거 (원본: {actual_fields}, 사용: {expected_fields})")
                    
            except Exception as e:
                invalid_lines.append((line_num, line, str(e)))
                logger.warning(f"라인 {line_num} 파싱 실패: {str(e)}")
                continue
        
        logger.info(f"토픽 {topic}: 총 {len(data_lines)}개 라인 중 {len(parsed_data)}개 성공, {len(invalid_lines)}개 실패")
        
        # 실패한 라인이 있으면 로깅
        if invalid_lines:
            logger.warning(f"파싱 실패한 라인들:")
            for line_num, line, error in invalid_lines[:3]:  # 처음 3개만 출력
                line_preview = line[:100] + "..." if len(line) > 100 else line
                logger.warning(f"  라인 {line_num}: {error} | 내용: {line_preview}")
        
        # 유효한 데이터가 없는 경우
        if not parsed_data:
            logger.error(f"토픽 {topic}: 파싱된 유효한 데이터가 없습니다.")
            return
        
        # Spark DataFrame 생성
        logger.info(f"토픽 {topic}: DataFrame 생성 중... ({len(parsed_data)}개 행)")
        
        # RDD 생성
        rdd = spark.sparkContext.parallelize(parsed_data)
        
        # DataFrame 생성
        df = spark.createDataFrame(rdd, adjusted_schema)

        logger.info(f"토픽 {topic}에 대한 DataFrame 스키마:")
        df.printSchema()
        
        # 데이터 검증 및 로깅
        is_valid = validate_and_log_data(df, topic, adjusted_schema)
        
        if not is_valid:
            logger.warning(f"토픽 {topic}에 전송할 데이터가 없습니다.")
            return

        # Kafka로 전송할 JSON 형태로 변환
        logger.info(f"토픽 {topic}: JSON 변환 시작...")
        kafka_df = df.select(to_json(struct([col(c) for c in df.columns])).alias("value"))
        
        # JSON 변환 결과 샘플 확인
        try:
            json_samples = kafka_df.limit(2).collect()
            for i, sample in enumerate(json_samples, 1):
                json_str = sample['value']
                json_preview = json_str[:200] + "..." if len(json_str) > 200 else json_str
                logger.info(f"JSON 변환 샘플 {i} (길이: {len(json_str)}): {json_preview}")
                
                # JSON 파싱 테스트
                try:
                    parsed = json.loads(json_str)
                    logger.info(f"  JSON 파싱 성공, 필드 수: {len(parsed)}")
                    # 첫 번째 샘플의 키-값 쌍 출력
                    if i == 1:
                        sample_fields = list(parsed.items())[:3]  # 처음 3개 필드만
                        logger.info(f"  샘플 필드: {sample_fields}")
                except json.JSONDecodeError as e:
                    logger.error(f"  JSON 파싱 실패: {str(e)}")
                    raise
        except Exception as e:
            logger.warning(f"JSON 변환 테스트 중 오류: {str(e)}")

        # Kafka에 데이터 쓰기
        logger.info(f"토픽 {topic}: Kafka로 데이터 전송 시작...")
        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("topic", topic) \
            .option("kafka.retries", "3") \
            .option("kafka.acks", "all") \
            .option("kafka.max.request.size", "10485760") \
            .save()
        
        final_count = df.count()
        logger.info(f"토픽 '{topic}'으로 {final_count}건의 데이터 전송 완료")
        
    except Exception as e:
        logger.error(f"토픽 {topic} 처리 중 오류 발생: {str(e)}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        raise

def read_json_from_hdfs(spark, hdfs_path):
    """HDFS에서 JSON 파일을 안전하게 읽어옵니다."""
    try:
        logger.info(f"HDFS 경로 접근 시도: {hdfs_path}")
        
        # DataFrame으로 텍스트 파일 읽기
        df = spark.read.text(hdfs_path)
        json_lines = df.collect()
        
        if not json_lines:
            raise ValueError("HDFS 파일이 비어있습니다.")
        
        logger.info(f"HDFS에서 {len(json_lines)}개 라인 읽음")
        
        # 모든 행을 합쳐서 하나의 JSON 문자열로 만들기
        full_json_string = "".join([row['value'] for row in json_lines])
        
        # JSON 파싱 전에 내용 미리보기
        preview = full_json_string[:500] + "..." if len(full_json_string) > 500 else full_json_string
        logger.info(f"JSON 내용 미리보기: {preview}")
        
        return json.loads(full_json_string)
        
    except Exception as e:
        logger.error(f"HDFS에서 JSON 파일 읽기 실패: {str(e)}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        raise

def main():
    parser = argparse.ArgumentParser(
        description="Spark Kafka Producer from a JSON file on HDFS",
        epilog="Example: --json-file /input/test.json --kafka-brokers kafka:9092"
    )
    parser.add_argument("--json-file", required=True, help="Path to the input JSON file on HDFS")
    parser.add_argument("--kafka-brokers", required=True, help="Kafka bootstrap servers")
    args = parser.parse_args()

    # Spark 세션 생성
    spark = SparkSession.builder \
        .appName("JSON to Kafka Producer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    logger.info("Spark 세션 생성 완료")
    logger.info(f"Spark 버전: {spark.version}")

    try:
        topic_mapping = {
            'num.txt': 'financial_numbers', 
            'pre.txt': 'presentation_links',
            'sub.txt': 'submission_info', 
            'tag.txt': 'tag_info'
        }
        schemas = get_schemas()

        # HDFS 경로 구성
        hdfs_path = f"hdfs://namenode:9000{args.json_file}"
        logger.info(f"HDFS에서 JSON 파일 읽는 중: {hdfs_path}")
        
        # JSON 데이터 읽기
        data = read_json_from_hdfs(spark, hdfs_path)
        logger.info(f"JSON 파일에서 발견된 키: {list(data.keys())}")
        
        # 각 키에 대한 데이터 크기 정보
        for key in data.keys():
            if isinstance(data[key], str):
                data_size = len(data[key])
                line_count = data[key].count('\n') + 1 if data[key] else 0
                logger.info(f"키 '{key}': {data_size} 문자, {line_count} 라인")
        
        # 각 파일 키에 대해 처리
        processed_topics = 0
        for file_key, topic in topic_mapping.items():
            if file_key in data:
                logger.info(f"=== 처리 시작: {file_key} -> {topic} ===")
                try:
                    process_and_send(spark, args.kafka_brokers, topic, data[file_key], schemas[file_key])
                    processed_topics += 1
                    logger.info(f"=== 처리 완료: {file_key} -> {topic} ===")
                except Exception as e:
                    logger.error(f"토픽 {topic} 처리 중 오류: {str(e)}")
                    continue
            else:
                logger.warning(f"JSON 파일에 키가 없습니다: {file_key}")
        
        logger.info(f"총 {processed_topics}개의 토픽 처리 완료")
        
    except Exception as e:
        logger.error(f"애플리케이션 실행 중 오류 발생: {str(e)}")
        import traceback
        logger.error(f"상세 오류: {traceback.format_exc()}")
        raise
    finally:
        spark.stop()
        logger.info("Spark 세션 종료")

if __name__ == "__main__":
    main()