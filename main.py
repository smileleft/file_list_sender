

import os
import json
import logging
import pickle
import hashlib
from pathlib import Path
from typing import List, Generator, Set, Dict, Optional
import pika
from pika.exceptions import AMQPConnectionError
import argparse
import sys
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class AppConfig:
    """애플리케이션 설정 클래스"""
    directory_path: str
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_username: str = "guest"
    rabbitmq_password: str = "guest"
    topic_name: str = "sample_data"
    batch_size: int = 100
    state_file: str = "app_state.json"


@dataclass
class AppState:
    """애플리케이션 상태 클래스"""
    config_hash: str
    last_processed_directory: Optional[str] = None
    processed_files: Set[str] = None
    total_files_processed: int = 0
    total_batches_sent: int = 0
    last_update: str = None
    
    def __post_init__(self):
        if self.processed_files is None:
            self.processed_files = set()
        if self.last_update is None:
            self.last_update = datetime.now().isoformat()


class StateManager:
    """애플리케이션 상태 관리 클래스"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.state_file = Path(config.state_file)
        self.logger = logging.getLogger(__name__)
        self.config_hash = self._generate_config_hash()
    
    def _generate_config_hash(self) -> str:
        """설정 변경 감지를 위한 해시 생성"""
        config_data = {
            'directory_path': self.config.directory_path,
            'rabbitmq_host': self.config.rabbitmq_host,
            'rabbitmq_port': self.config.rabbitmq_port,
            'rabbitmq_username': self.config.rabbitmq_username,
            'topic_name': self.config.topic_name,
            'batch_size': self.config.batch_size
        }
        config_str = json.dumps(config_data, sort_keys=True)
        return hashlib.sha256(config_str.encode()).hexdigest()[:16]
    
    def load_state(self) -> AppState:
        """상태 파일에서 애플리케이션 상태 로드"""
        if not self.state_file.exists():
            self.logger.info("상태 파일이 없습니다. 새로운 상태로 시작합니다.")
            return AppState(config_hash=self.config_hash)
        
        try:
            with open(self.state_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 설정이 변경되었는지 확인
            if data.get('config_hash') != self.config_hash:
                self.logger.warning("설정이 변경되었습니다. 새로운 상태로 시작합니다.")
                self._backup_old_state()
                return AppState(config_hash=self.config_hash)
            
            # processed_files를 set으로 변환
            processed_files = set(data.get('processed_files', []))
            
            state = AppState(
                config_hash=data['config_hash'],
                last_processed_directory=data.get('last_processed_directory'),
                processed_files=processed_files,
                total_files_processed=data.get('total_files_processed', 0),
                total_batches_sent=data.get('total_batches_sent', 0),
                last_update=data.get('last_update')
            )
            
            self.logger.info(f"이전 상태 로드 완료: {len(processed_files)}개 파일 처리 완료, "
                           f"{state.total_batches_sent}개 배치 전송 완료")
            
            if state.last_processed_directory:
                self.logger.info(f"마지막 처리 디렉터리: {state.last_processed_directory}")
            
            return state
            
        except Exception as e:
            self.logger.error(f"상태 파일 로드 중 오류: {e}")
            self.logger.info("새로운 상태로 시작합니다.")
            self._backup_old_state()
            return AppState(config_hash=self.config_hash)
    
    def save_state(self, state: AppState):
        """애플리케이션 상태를 파일에 저장"""
        try:
            state.last_update = datetime.now().isoformat()
            
            # set을 list로 변환하여 JSON 직렬화 가능하게 만듦
            data = {
                'config_hash': state.config_hash,
                'last_processed_directory': state.last_processed_directory,
                'processed_files': list(state.processed_files),
                'total_files_processed': state.total_files_processed,
                'total_batches_sent': state.total_batches_sent,
                'last_update': state.last_update
            }
            
            # 임시 파일에 먼저 저장한 후 원자적으로 이동
            temp_file = self.state_file.with_suffix('.tmp')
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            temp_file.replace(self.state_file)
            
        except Exception as e:
            self.logger.error(f"상태 저장 중 오류: {e}")
    
    def _backup_old_state(self):
        """기존 상태 파일을 백업"""
        if self.state_file.exists():
            backup_file = self.state_file.with_suffix('.backup')
            try:
                self.state_file.replace(backup_file)
                self.logger.info(f"기존 상태 파일을 백업했습니다: {backup_file}")
            except Exception as e:
                self.logger.warning(f"상태 파일 백업 실패: {e}")


class FileSystemScanner:
    """파일시스템 스캔 담당 클래스 (재시작 지원)"""
    
    def __init__(self, directory_path: str, state: AppState):
        self.directory_path = Path(directory_path)
        self.state = state
        self.logger = logging.getLogger(__name__)
        self.should_skip = True if state.last_processed_directory else False
    
    def validate_directory_access(self) -> bool:
        """디렉터리 접근 권한 확인"""
        try:
            if not self.directory_path.exists():
                self.logger.error(f"디렉터리가 존재하지 않습니다: {self.directory_path}")
                return False
            
            if not self.directory_path.is_dir():
                self.logger.error(f"지정된 경로가 디렉터리가 아닙니다: {self.directory_path}")
                return False
            
            if not os.access(self.directory_path, os.R_OK):
                self.logger.error(f"디렉터리에 읽기 권한이 없습니다: {self.directory_path}")
                return False
            
            self.logger.info(f"디렉터리 접근 확인 완료: {self.directory_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"디렉터리 접근 확인 중 오류 발생: {e}")
            return False
    
    def scan_files(self) -> Generator[tuple[str, str], None, None]:
        """디렉터리의 모든 파일을 재귀적으로 스캔 (재시작 지점부터)"""
        try:
            # 디렉터리를 정렬하여 일관된 순서 보장
            directories_to_process = []
            
            for root, dirs, files in os.walk(self.directory_path):
                # 디렉터리 정렬
                dirs.sort()
                
                # 재시작 로직: 마지막 처리 디렉터리 이후부터 시작
                if self.should_skip and self.state.last_processed_directory:
                    if root == self.state.last_processed_directory:
                        self.should_skip = False
                        self.logger.info(f"재시작 지점에 도달했습니다: {root}")
                        continue  # 이미 처리된 디렉터리는 스킵
                    elif self.should_skip:
                        self.logger.debug(f"스킵: {root}")
                        continue
                
                # 파일을 정렬하여 일관된 순서 보장
                files.sort()
                
                for file in files:
                    file_path = os.path.join(root, file)
                    
                    # 이미 처리된 파일인지 확인
                    if file_path in self.state.processed_files:
                        self.logger.debug(f"이미 처리됨 (스킵): {file_path}")
                        continue
                    
                    yield file_path, root
                    
        except PermissionError as e:
            self.logger.warning(f"권한 없음으로 스킵: {e}")
        except Exception as e:
            self.logger.error(f"파일 스캔 중 오류 발생: {e}")


class RabbitMQPublisher:
    """RabbitMQ 메시지 발행 담당 클래스"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.connection = None
        self.channel = None
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> bool:
        """RabbitMQ 서버에 연결"""
        try:
            credentials = pika.PlainCredentials(
                self.config.rabbitmq_username, 
                self.config.rabbitmq_password
            )
            
            parameters = pika.ConnectionParameters(
                host=self.config.rabbitmq_host,
                port=self.config.rabbitmq_port,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # 토픽(큐) 선언
            self.channel.queue_declare(queue=self.config.topic_name, durable=True)
            
            self.logger.info(f"RabbitMQ 연결 성공: {self.config.rabbitmq_host}:{self.config.rabbitmq_port}")
            return True
            
        except AMQPConnectionError as e:
            self.logger.error(f"RabbitMQ 연결 실패: {e}")
            return False
        except Exception as e:
            self.logger.error(f"RabbitMQ 연결 중 예상치 못한 오류: {e}")
            return False
    
    def publish_file_batch(self, file_paths: List[str], batch_number: int) -> bool:
        """파일 경로 배치를 RabbitMQ에 발행"""
        try:
            if not self.channel:
                self.logger.error("RabbitMQ 채널이 없습니다. 먼저 연결하세요.")
                return False
            
            message = {
                "batch_number": batch_number,
                "batch_id": f"batch_{batch_number}_{int(datetime.now().timestamp())}",
                "file_count": len(file_paths),
                "file_paths": file_paths,
                "timestamp": datetime.now().isoformat()
            }
            
            self.channel.basic_publish(
                exchange='',
                routing_key=self.config.topic_name,
                body=json.dumps(message, ensure_ascii=False, indent=2),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # 메시지를 디스크에 저장 (지속성)
                    content_type='application/json'
                )
            )
            
            self.logger.info(f"배치 #{batch_number} 발행 완료 - 파일 수: {len(file_paths)}")
            return True
            
        except Exception as e:
            self.logger.error(f"메시지 발행 중 오류: {e}")
            return False
    
    def close(self):
        """연결 종료"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            self.logger.info("RabbitMQ 연결 종료")
        except Exception as e:
            self.logger.warning(f"연결 종료 중 오류: {e}")


class FileRabbitMQApp:
    """메인 애플리케이션 클래스 (재시작 및 중복 방지 기능)"""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.state_manager = StateManager(config)
        self.state = self.state_manager.load_state()
        self.scanner = FileSystemScanner(config.directory_path, self.state)
        self.publisher = RabbitMQPublisher(config)
        self.logger = logging.getLogger(__name__)
    
    def run(self) -> int:
        """애플리케이션 실행"""
        try:
            # 1. 디렉터리 접근 권한 확인
            if not self.scanner.validate_directory_access():
                return 1
            
            # 2. RabbitMQ 연결
            if not self.publisher.connect():
                return 1
            
            # 3. 재시작 정보 출력
            if self.state.last_processed_directory:
                self.logger.info("===== 재시작 모드 =====")
                self.logger.info(f"이전까지 처리된 파일 수: {len(self.state.processed_files)}")
                self.logger.info(f"이전까지 전송된 배치 수: {self.state.total_batches_sent}")
                self.logger.info(f"마지막 처리 디렉터리: {self.state.last_processed_directory}")
                self.logger.info("======================")
            else:
                self.logger.info("===== 새로운 시작 =====")
            
            # 4. 파일 스캔 및 배치 처리
            file_batch = []
            current_directory = None
            save_interval = 10  # 10개 배치마다 상태 저장
            
            self.logger.info("파일 스캔 시작...")
            
            for file_path, directory in self.scanner.scan_files():
                current_directory = directory
                file_batch.append(file_path)
                
                # 배치 크기에 도달하면 발행
                if len(file_batch) >= self.config.batch_size:
                    self.state.total_batches_sent += 1
                    
                    if self.publisher.publish_file_batch(file_batch, self.state.total_batches_sent):
                        # 처리된 파일들을 상태에 추가
                        self.state.processed_files.update(file_batch)
                        self.state.total_files_processed += len(file_batch)
                        self.state.last_processed_directory = current_directory
                        
                        self.logger.info(f"배치 #{self.state.total_batches_sent} 발행 완료 "
                                       f"({len(file_batch)}개 파일) - "
                                       f"총 처리: {self.state.total_files_processed}개")
                        
                        # 주기적으로 상태 저장
                        if self.state.total_batches_sent % save_interval == 0:
                            self.state_manager.save_state(self.state)
                            self.logger.debug("상태 저장 완료")
                    else:
                        self.logger.error("배치 발행 실패")
                        self.state_manager.save_state(self.state)
                        return 1
                    
                    file_batch = []  # 배치 초기화
            
            # 5. 남은 파일들 발행
            if file_batch:
                self.state.total_batches_sent += 1
                
                if self.publisher.publish_file_batch(file_batch, self.state.total_batches_sent):
                    self.state.processed_files.update(file_batch)
                    self.state.total_files_processed += len(file_batch)
                    self.state.last_processed_directory = current_directory
                    
                    self.logger.info(f"마지막 배치 #{self.state.total_batches_sent} 발행 완료 "
                                   f"({len(file_batch)}개 파일)")
                else:
                    self.logger.error("마지막 배치 발행 실패")
                    self.state_manager.save_state(self.state)
                    return 1
            
            # 6. 완료 상태로 설정
            self.state.last_processed_directory = None  # 완료 표시
            self.state_manager.save_state(self.state)
            
            self.logger.info("===== 작업 완료 =====")
            self.logger.info(f"총 처리된 파일: {self.state.total_files_processed}개")
            self.logger.info(f"총 전송된 배치: {self.state.total_batches_sent}개")
            self.logger.info("====================")
            
            return 0
            
        except KeyboardInterrupt:
            self.logger.info("사용자에 의해 중단됨")
            self.logger.info("상태를 저장하고 종료합니다...")
            self.state_manager.save_state(self.state)
            self.logger.info("다음 실행 시 중단된 지점에서 재시작됩니다.")
            return 1
        except Exception as e:
            self.logger.error(f"예상치 못한 오류: {e}")
            self.state_manager.save_state(self.state)
            return 1
        finally:
            # 7. 정리
            self.publisher.close()
    
    def reset_state(self):
        """상태 초기화 (새로운 시작을 원할 때)"""
        if self.state_manager.state_file.exists():
            self.state_manager._backup_old_state()
        self.logger.info("상태가 초기화되었습니다.")


def setup_logging(level: str = "INFO"):
    """로깅 설정"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('file_rabbitmq_app.log', encoding='utf-8')
        ]
    )


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description="파일시스템 스캔 후 RabbitMQ로 발행하는 애플리케이션 (재시작 지원)")
    
    parser.add_argument(
        '--directory', '-d',
        required=True,
        help='스캔할 디렉터리 경로'
    )
    parser.add_argument(
        '--rabbitmq-host',
        default='localhost',
        help='RabbitMQ 호스트 (기본값: localhost)'
    )
    parser.add_argument(
        '--rabbitmq-port',
        type=int,
        default=5672,
        help='RabbitMQ 포트 (기본값: 5672)'
    )
    parser.add_argument(
        '--username',
        default='guest',
        help='RabbitMQ 사용자명 (기본값: guest)'
    )
    parser.add_argument(
        '--password',
        default='guest',
        help='RabbitMQ 비밀번호 (기본값: guest)'
    )
    parser.add_argument(
        '--topic',
        default='sample_data',
        help='RabbitMQ 토픽명 (기본값: sample_data)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='배치 크기 (기본값: 100)'
    )
    parser.add_argument(
        '--state-file',
        default='app_state.json',
        help='상태 파일 경로 (기본값: app_state.json)'
    )
    parser.add_argument(
        '--reset-state',
        action='store_true',
        help='기존 상태를 초기화하고 새로 시작'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='로그 레벨 (기본값: INFO)'
    )
    
    args = parser.parse_args()
    
    # 로깅 설정
    setup_logging(args.log_level)
    
    # 설정 생성
    config = AppConfig(
        directory_path=args.directory,
        rabbitmq_host=args.rabbitmq_host,
        rabbitmq_port=args.rabbitmq_port,
        rabbitmq_username=args.username,
        rabbitmq_password=args.password,
        topic_name=args.topic,
        batch_size=args.batch_size,
        state_file=args.state_file
    )
    
    # 애플리케이션 생성
    app = FileRabbitMQApp(config)
    
    # 상태 초기화 요청 시
    if args.reset_state:
        app.reset_state()
        print("상태가 초기화되었습니다. 다시 실행하세요.")
        sys.exit(0)
    
    # 애플리케이션 실행
    exit_code = app.run()
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
