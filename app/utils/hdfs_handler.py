from hdfs import InsecureClient
from app.config import Config, logger


class HDFSConnector:
    def __init__(self, hdfs_url=Config.HDFS_URL, user=Config.HDFS_USER):
        # HDFS 연결 설정
        try:
            self.client = InsecureClient(hdfs_url, user=user)
            logger.info(f"HDFS 연결 성공: {hdfs_url} (사용자: {user})")
        except Exception as e:
            logger.critical(f"HDFS 연결 실패: {e}")
            raise

    def upload_file(self, local_path, hdfs_path):
        # 로컬 파일을 HDFS로 업로드
        try:
            self.client.upload(hdfs_path, local_path, overwrite=True)
            logger.info(f"파일이 성공적으로 HDFS에 업로드되었습니다: {hdfs_path}")
        except Exception as e:
            logger.error(f"HDFS 파일 업로드 중 오류 발생: {e}")

    def close_connection(self):
        logger.info("HDFS 작업 완료")  # HDFS는 명시적으로 닫을 필요가 없다고 함
