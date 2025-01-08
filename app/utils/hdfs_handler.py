import os
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
            logger.error(f"HDFS 파일 업로드 중 에러 발생: {e}")

    def upload_directory(self, local_dir, hdfs_dir):
        # 로컬 디렉터리를 HDFS로 업로드 (디렉터리 구조 포함)
        try:
            # 로컬 디렉터리 내 모든 파일과 하위 디렉터리 탐색
            for root, _, files in os.walk(local_dir):
                relative_path = os.path.relpath(root, local_dir)
                hdfs_subdir = os.path.join(hdfs_dir, relative_path)

                # HDFS 상에 디렉터리 생성
                self.client.makedirs(hdfs_subdir)
                logger.info(f"HDFS 디렉터리 생성 완료: {hdfs_subdir}")

                # 해당 디렉터리의 모든 파일 업로드
                for file in files:
                    local_file_path = os.path.join(root, file)
                    hdfs_file_path = os.path.join(hdfs_subdir, file)
                    self.upload_file(local_file_path, hdfs_file_path)
        except Exception as e:
            logger.error(f"디렉터리 업로드 중 에러 발생: {e}")

    def delete_hdfs_directory(self, hdfs_path):
        try:
            if self.client.status(hdfs_path, strict=False):
                self.client.delete(hdfs_path, recursive=True)
                logger.info(f"HDFS 디렉터리가 삭제되었습니다: {hdfs_path}")
        except Exception as e:
            logger.error(f"HDFS 디렉터리 삭제 실패: {e}")

    def close_connection(self):
        logger.info("HDFS 작업 완료")  # HDFS는 명시적으로 닫을 필요가 없다고 함
