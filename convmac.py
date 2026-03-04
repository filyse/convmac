# Основные модули Python
import os
import re
import subprocess
import sys
import signal
import time
import json
from typing import Dict, List, Optional, Callable
from datetime import datetime, timedelta

# PyQt5 компоненты
from PyQt5 import sip
from PyQt5.QtWidgets import (
    QApplication,
    QMainWindow,
    QVBoxLayout,
    QHBoxLayout,
    QWidget,
    QLabel,
    QPushButton,
    QFileDialog,
    QTextEdit,
    QProgressBar,
    QGroupBox,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QMenu,
    QSizePolicy,
    QToolTip,
    QAction,
    QDialog,
    QScrollArea
)
from PyQt5.QtCore import (
    QThread,
    pyqtSignal,
    pyqtSlot,
    Qt,
    QMimeData,
    QUrl,
    QEvent,
    QMetaObject,
    Q_ARG,
    QTimer
)
from PyQt5.QtGui import (
    QColor,
    QBrush,
    QDragEnterEvent,
    QDropEvent,
    QKeySequence,
    QCursor,
    QFont
)

# Дополнительные модули (если используются)
try:
    import psutil
except ImportError:
    psutil = None
    
LOG_SOURCE = "Encoder"

def _log(level, msg):
    """Единый формат: SOURCE | LEVEL | message"""
    one_line = str(msg).replace("\n", " ").strip()
    print(f"{LOG_SOURCE:<10} | {level:<5} | {one_line}")

try:
    from kafka import KafkaConsumer, KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    _log("WARN", "Kafka не установлен, функциональность Kafka недоступна")

# Топики Kafka для цепочки Conv → FTP
KAFKA_TOPIC_FTP_QUEUE = 'ftp_upload_queue'
KAFKA_TOPIC_ENCODING_READY = 'encoding_ready'

# NVEncC (rigaya) — только Windows; на macOS используем ffmpeg (libx264/libx265)
NVENC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "NVEncC")
NVENC_EXE = os.path.join(NVENC_DIR, "NVEncC64.exe") if sys.platform == "win32" else None

# Папка по умолчанию для сохранения (macOS)
DEFAULT_OUTPUT_BASE_MAC = os.path.expanduser("~/Movies/Rip")
    
class KafkaConsumerThread(QThread):
    message_received = pyqtSignal(str)  # Сигнал с путем к файлу
    
    def __init__(self, bootstrap_servers='192.168.1.223:9092', topic='video_encoding_tasks', kafka_available=False):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.running = True
        self.consumer = None
        self.kafka_available = kafka_available
        
    def run(self):
        """Основной метод выполнения потребителя Kafka"""
        if not self.kafka_available:
            self.message_received.emit("ERROR: Kafka не установлен")
            return
            
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=[self.bootstrap_servers],
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='video_encoder_group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000  # Добавим таймаут для более быстрого реагирования на остановку
            )
            
            _log("KAFKA", "подключен")
            
            while self.running:
                try:
                    # Таймаут для возможности прерывания
                    msg_pack = self.consumer.poll(timeout_ms=1000)
                    
                    if not self.running:
                        break
                        
                    for tp, messages in msg_pack.items():
                        for message in messages:
                            try:
                                file_path = message.value.get('file_path')
                                if file_path and os.path.exists(file_path):
                                    self.message_received.emit(file_path)
                                    _log("KAFKA", f"Получена задача: {os.path.basename(file_path)}")
                                else:
                                    self.message_received.emit(f"ERROR: Неверный путь к файлу в сообщении Kafka: {file_path}")
                            except Exception as e:
                                self.message_received.emit(f"ERROR: Ошибка обработки сообщения Kafka: {str(e)}")
                except Exception as e:
                    if self.running:  # Только если не остановлено принудительно
                        _log("WARN", f"Kafka Consumer: {str(e)}")
                    break
                            
        except Exception as e:
            if self.running:  # Только если не остановлено принудительно
                self.message_received.emit(f"ERROR: Ошибка Kafka потребителя: {str(e)}")
                _log("ERR", f"Ошибка Kafka потребителя: {str(e)}")
        finally:
            self._close_consumer()
            _log("KAFKA", "поток завершён")

    def _close_consumer(self):
        """Безопасное закрытие Kafka consumer"""
        if self.consumer:
            try:
                self.consumer.close()
                self.consumer = None
                _log("KAFKA", "закрыт")
            except Exception as e:
                _log("WARN", f"Ошибка при закрытии Kafka consumer: {str(e)}")
            finally:
                self.consumer = None

    def stop(self):
        """Остановка Kafka потребителя"""
        _log("KAFKA", "остановка...")
        self.running = False
        
        # Закрываем consumer, чтобы прервать блокирующий poll
        self._close_consumer()
        
        # Ждем завершения потока, но не слишком долго
        if self.isRunning():
            if not self.wait(2000):  # Ждем 2 секунды
                _log("WARN", "Kafka Consumer не завершился за 2 сек, принудительная остановка")
                self.terminate()
                self.wait(1000)

class EncodingThread(QThread):
    update_signal = pyqtSignal(str, str, str)
    progress_signal = pyqtSignal(str, str, int)
    finished_signal = pyqtSignal(str, str, bool, str)

    def __init__(self, task_id, input_file, output_format, logger=None, task_type='', output_base_dir=None):
        super().__init__()
        self.task_id = task_id
        self.input_file = input_file
        self.output_format = output_format
        self.running = True
        self.process = None
        self.pid = None
        self.logger = logger
        self._log_file_handle = None
        self.task_type = task_type
        self.output_base_dir = output_base_dir or DEFAULT_OUTPUT_BASE_MAC

    def run(self):
        """Основной метод выполнения кодирования с полной обработкой ошибок"""
        try:
            self.update_signal.emit(self.task_id, self.output_format, f"Starting thread for {self.output_format}")
            _log("INFO", f"Encoding thread start: task {self.task_id}, format {self.output_format}")
            
            self._validate_inputs()
            
            if self.output_format == "xvid":
                self.encode_xvid()
            elif self.output_format == "400p":
                self.encode_400p()
            elif self.output_format == "720p":
                self.encode_720p()
            elif self.output_format == "anidub":
                self.create_anidub_mkv()
            elif self.output_format == "x265":
                self.encode_x265()
            else:
                raise ValueError(f"Unknown output format: {self.output_format}")
                
        except Exception as e:
            self._handle_error(e)
        finally:
            self._cleanup_resources()
            self.update_signal.emit(self.task_id, self.output_format, "Thread finished")
            _log("INFO", f"Encoding thread finished: task {self.task_id}, format {self.output_format}")
            
    def safe_quit(self):
        """Безопасное завершение потока"""
        self.running = False  # Устанавливаем флаг остановки
        
        if hasattr(self, 'process') and self.process:
            try:
                # Для Windows
                if sys.platform == "win32":
                    os.kill(self.pid, signal.CTRL_C_EVENT)
                # Для Unix-систем
                else:
                    os.killpg(os.getpgid(self.pid), signal.SIGTERM)
                
                # Даем процессу время на завершение
                self.process.wait(2000)
            except:
                pass

    def _validate_inputs(self):
        """Проверка входных данных и системных требований"""
        if not os.path.isfile(self.input_file):
            raise FileNotFoundError(f"Input file not found: {self.input_file}")

        # Проверка свободного места
        free_space = self._get_free_space(os.path.dirname(os.path.abspath(self.input_file)))
        min_space = 500 * 1024 * 1024  # 500MB
        if free_space < min_space:
            space_mb = free_space // (1024 * 1024)
            raise IOError(f"Not enough disk space: {space_mb}MB available, need {min_space//(1024*1024)}MB")

    def _get_free_space(self, dirpath):
        """Кроссплатформенная проверка свободного места"""
        try:
            if sys.platform == 'win32':
                import ctypes
                free_bytes = ctypes.c_ulonglong(0)
                ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                    ctypes.c_wchar_p(dirpath), None, None, ctypes.pointer(free_bytes))
                return free_bytes.value
            else:
                stat = os.statvfs(dirpath)
                return stat.f_bavail * stat.f_frsize
        except Exception:
            return float('inf')  # Возвращаем "бесконечное" место при ошибке

    def run_ffmpeg(self, cmd, log_file, output_file):
        """Запуск процесса ffmpeg с отслеживанием прогресса и реального ETA (по скорости кодирования)."""
        try:
            # Создаем папку для логов, если ее нет
            log_dir = os.path.dirname(log_file)
            os.makedirs(log_dir, exist_ok=True)

            # Инициализация лог-файла - сохраняем дескриптор
            start_time = datetime.now()
            self._log_file_handle = open(log_file, 'w', encoding='utf-8')
            self._log_file_handle.write(f"[START TIME] {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            self._log_file_handle.write(f"[TASK TYPE] {self.task_type}\n")  # ДОБАВИЛИ
            self._log_file_handle.write(f"[COMMAND] {' '.join(cmd)}\n")
            self._log_file_handle.write(f"[INPUT] {self.input_file}\n")
            self._log_file_handle.write(f"[OUTPUT] {output_file}\n\n")
            self._log_file_handle.flush()

            self.update_signal.emit(self.task_id, self.output_format, "Starting encoding process...")

            # Настройки процесса для разных платформ
            startupinfo = None
            creationflags = 0
            if sys.platform == "win32":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = subprocess.SW_HIDE
                creationflags = subprocess.CREATE_NO_WINDOW

            # Запуск процесса
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
                startupinfo=startupinfo,
                creationflags=creationflags
            )
            self.pid = self.process.pid

            duration = None
            last_percent = 0
            progress_pattern = re.compile(r'time=(\d+):(\d+):(\d+\.\d+)')
            duration_pattern = re.compile(r'Duration:\s*(\d+):(\d+):(\d+\.\d+)')
            user_interrupted = False  # Флаг ручной остановки

            # Чтение вывода в реальном времени
            while True:
                if not self.running:  # Проверка флага остановки
                    user_interrupted = True
                    break

                line = self.process.stdout.readline()
                if not line:  # Конец вывода
                    break

                line = line.strip()
                if self._log_file_handle:
                    self._log_file_handle.write(f"{line}\n")
                    self._log_file_handle.flush()

                # Определение длительности
                if duration is None and "Duration:" in line:
                    try:
                        match = duration_pattern.search(line)
                        if match:
                            h, m, s = match.groups()
                            duration = int(h) * 3600 + int(m) * 60 + float(s)
                            self.update_signal.emit(self.task_id, self.output_format, 
                                                  f"Duration detected: {h}:{m}:{s}")
                    except Exception as e:
                        self.update_signal.emit(self.task_id, self.output_format,
                                              f"Duration parse error: {str(e)}")

                # Прогресс + реальный ETA по скорости
                elif "time=" in line:
                    try:
                        match = progress_pattern.search(line)
                        if match and duration and duration > 0:
                            h, m, s = match.groups()
                            current_time = int(h) * 3600 + int(m) * 60 + float(s)

                            # 1) прогресс, ограничим 99 чтобы оставить 100 на завершение
                            percent = min(99, int((current_time / duration) * 100))

                            # 2) скорость кодирования (X realtime) и реальный ETA по стеночному времени
                            elapsed = (datetime.now() - start_time).total_seconds()
                            if elapsed > 0.5:
                                speed = current_time / elapsed  # X реального времени
                            else:
                                speed = 1e-6  # защита от деления на ноль в самом начале

                            remaining_sec = max(0.0, (duration - current_time) / max(speed, 1e-6))

                            rh = int(remaining_sec // 3600)
                            rm = int((remaining_sec % 3600) // 60)
                            rs = int(remaining_sec % 60)
                            eta_str = f"{rh:02d}:{rm:02d}:{rs:02d}"

                            # Отправляем ETA как статус (ловится в GUI и ставится в текст QProgressBar)
                            self.update_signal.emit(self.task_id, self.output_format, f"ETA {eta_str}")

                            if percent > last_percent:
                                last_percent = percent
                                self.progress_signal.emit(self.task_id, self.output_format, percent)
                                # ограничим частоту обновлений
                                time.sleep(0.1)
                    except Exception as e:
                        self.update_signal.emit(self.task_id, self.output_format,
                                              f"Progress parse error: {str(e)}")

                # Проверка завершения кодирования (типичный маркер ffmpeg)
                elif "video:" in line and "audio:" in line:
                    self.progress_signal.emit(self.task_id, self.output_format, 100)
                    break

            # Обработка завершения процесса/остановки
            if user_interrupted:
                if self._log_file_handle:
                    self._log_file_handle.write("\n[USER INTERRUPT] Stopped by user\n")
                    self._log_file_handle.flush()
                self.finished_signal.emit(self.task_id, self.output_format, False, "Stopped by user")
                return
                
            return_code = self.process.wait()
            end_time = datetime.now()

            if self._log_file_handle:
                self._log_file_handle.write(f"\n[FINISH TIME] {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                self._log_file_handle.write(f"[STATUS] {'COMPLETED' if return_code == 0 else 'FAILED'}\n")
                self._log_file_handle.write(f"[RETURN CODE] {return_code}\n")
                self._log_file_handle.flush()

            if return_code == 0:
                if os.path.exists(output_file):
                    self.finished_signal.emit(self.task_id, self.output_format, True, output_file)
                else:
                    self.finished_signal.emit(self.task_id, self.output_format, False, "Output file was not created")
            else:
                self.finished_signal.emit(self.task_id, self.output_format, False, f"Process failed with code {return_code}")

        except Exception as e:
            error_msg = f"FFmpeg execution error: {str(e)}"
            _log("ERR", error_msg)
            if self._log_file_handle:
                self._log_file_handle.write(f"\n[ERROR] {error_msg}\n")
                self._log_file_handle.flush()
            self.finished_signal.emit(self.task_id, self.output_format, False, error_msg)
        finally:
            # Гарантированное закрытие ресурсов
            # Закрываем лог-файл
            if hasattr(self, '_log_file_handle') and self._log_file_handle:
                try:
                    self._log_file_handle.close()
                except:
                    pass
                finally:
                    self._log_file_handle = None
            
            # Закрываем процесс ffmpeg
            if hasattr(self, 'process') and self.process is not None:
                try:
                    for stream in [self.process.stdout, self.process.stderr]:
                        if stream:
                            try:
                                stream.close()
                            except:
                                pass
                    if self.process.poll() is None:
                        try:
                            self.process.terminate()
                            self.process.wait(timeout=2)
                        except:
                            pass
                finally:
                    self.process = None
                    self.pid = None
                    
    def _kill_ffmpeg_process(self):
        """Принудительное завершение процесса ffmpeg с защитой от ошибок"""
        # Проверяем что процесс существует и все еще работает
        if not hasattr(self, 'process') or self.process is None:
            return
            
        if self.process.poll() is not None:  # Процесс уже завершен
            return
            
        try:
            # Для Windows
            if sys.platform == "win32":
                try:
                    # Используем os.kill для более точного контроля
                    os.kill(self.pid, signal.SIGTERM)
                    # Даем время на завершение
                    time.sleep(1)
                    
                    # Если еще жив, используем taskkill
                    if self.process.poll() is None:
                        subprocess.run(
                            ['taskkill', '/F', '/T', '/PID', str(self.pid)],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL,
                            timeout=3
                        )
                except Exception as e:
                    _log("WARN", f"Windows kill failed: {str(e)}")
                    try:
                        # Последняя попытка
                        self.process.kill()
                    except:
                        pass
            # Для Unix-систем
            else:
                try:
                    # Отправляем SIGKILL
                    os.killpg(os.getpgid(self.pid), signal.SIGKILL)
                except ProcessLookupError:
                    # Процесс уже завершен
                    pass
                except Exception as e:
                    _log("ERR", f"Failed to kill process group: {str(e)}")
                    
            # Гарантируем завершение
            try:
                self.process.wait(timeout=2)
            except:
                pass
                
        except Exception as e:
            _log("ERR", f"Error in _kill_ffmpeg_process: {str(e)}")
        finally:
            # Гарантируем освобождение ресурсов
            try:
                # Закрываем потоки
                for stream in [self.process.stdout, self.process.stderr]:
                    if stream:
                        try:
                            stream.close()
                        except:
                            pass
            except:
                pass
                
            # Сбрасываем ссылки
            self.process = None
            self.pid = None

    def run_nvenc(self, cmd, log_file, output_file):
        """Запуск NVEncC с отслеживанием прогресса (вывод в stderr, парсим процент). На macOS не используется."""
        try:
            if not NVENC_EXE or not os.path.isfile(NVENC_EXE):
                self.finished_signal.emit(
                    self.task_id, self.output_format, False,
                    f"NVEncC не найден: {NVENC_EXE}"
                )
                return
            log_dir = os.path.dirname(log_file)
            os.makedirs(log_dir, exist_ok=True)
            start_time = datetime.now()
            self._log_file_handle = open(log_file, 'w', encoding='utf-8')
            self._log_file_handle.write(f"[START TIME] {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            self._log_file_handle.write(f"[TASK TYPE] {self.task_type}\n")
            self._log_file_handle.write(f"[COMMAND] {' '.join(cmd)}\n")
            self._log_file_handle.write(f"[INPUT] {self.input_file}\n")
            self._log_file_handle.write(f"[OUTPUT] {output_file}\n\n")
            self._log_file_handle.flush()
            startupinfo = None
            creationflags = 0
            if sys.platform == "win32":
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = subprocess.SW_HIDE
                creationflags = subprocess.CREATE_NO_WINDOW
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
                startupinfo=startupinfo,
                creationflags=creationflags
            )
            self.pid = self.process.pid
            last_percent = 0
            nvenc_percent_pattern = re.compile(r'\[\s*([\d.]+)\s*%\]')
            nvenc_frames_pattern = re.compile(r'(\d+)\s*/\s*(\d+)\s+frames')
            user_interrupted = False
            while True:
                if not self.running:
                    user_interrupted = True
                    break
                line = self.process.stdout.readline()
                if not line:
                    break
                line_stripped = line.strip()
                if self._log_file_handle:
                    self._log_file_handle.write(f"{line_stripped}\n")
                    self._log_file_handle.flush()
                percent_match = nvenc_percent_pattern.search(line_stripped)
                if percent_match:
                    p = float(percent_match.group(1))
                    percent = min(99, int(p))
                    if percent > last_percent:
                        last_percent = percent
                        self.progress_signal.emit(self.task_id, self.output_format, percent)
                        # ETA по прошедшему времени и проценту (как для ffmpeg)
                        if percent > 0:
                            elapsed = (datetime.now() - start_time).total_seconds()
                            if elapsed > 0.5:
                                remaining_sec = max(0.0, elapsed * (100.0 - percent) / percent)
                                rh = int(remaining_sec // 3600)
                                rm = int((remaining_sec % 3600) // 60)
                                rs = int(remaining_sec % 60)
                                eta_str = f"{rh:02d}:{rm:02d}:{rs:02d}"
                                self.update_signal.emit(self.task_id, self.output_format, f"ETA {eta_str}")
                else:
                    frames_match = nvenc_frames_pattern.search(line_stripped)
                    if frames_match and int(frames_match.group(2)) > 0:
                        curr, total = int(frames_match.group(1)), int(frames_match.group(2))
                        percent = min(99, int(100.0 * curr / total))
                        if percent > last_percent:
                            last_percent = percent
                            self.progress_signal.emit(self.task_id, self.output_format, percent)
                            if percent > 0:
                                elapsed = (datetime.now() - start_time).total_seconds()
                                if elapsed > 0.5:
                                    remaining_sec = max(0.0, elapsed * (100.0 - percent) / percent)
                                    rh = int(remaining_sec // 3600)
                                    rm = int((remaining_sec % 3600) // 60)
                                    rs = int(remaining_sec % 60)
                                    eta_str = f"{rh:02d}:{rm:02d}:{rs:02d}"
                                    self.update_signal.emit(self.task_id, self.output_format, f"ETA {eta_str}")
            if user_interrupted:
                if self._log_file_handle:
                    self._log_file_handle.write("\n[USER INTERRUPT] Stopped by user\n")
                    self._log_file_handle.flush()
                self.finished_signal.emit(self.task_id, self.output_format, False, "Stopped by user")
                return
            return_code = self.process.wait()
            self.progress_signal.emit(self.task_id, self.output_format, 100)
            end_time = datetime.now()
            if self._log_file_handle:
                self._log_file_handle.write(f"\n[FINISH TIME] {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                self._log_file_handle.write(f"[STATUS] {'COMPLETED' if return_code == 0 else 'FAILED'}\n")
                self._log_file_handle.write(f"[RETURN CODE] {return_code}\n")
                self._log_file_handle.flush()
            if return_code == 0:
                if os.path.exists(output_file):
                    self.finished_signal.emit(self.task_id, self.output_format, True, output_file)
                else:
                    self.finished_signal.emit(self.task_id, self.output_format, False, "Output file was not created")
            else:
                self.finished_signal.emit(self.task_id, self.output_format, False, f"NVEncC failed with code {return_code}")
        except Exception as e:
            error_msg = f"NVEncC execution error: {str(e)}"
            _log("ERR", error_msg)
            if hasattr(self, '_log_file_handle') and self._log_file_handle:
                try:
                    self._log_file_handle.write(f"\n[ERROR] {error_msg}\n")
                    self._log_file_handle.flush()
                except Exception:
                    pass
            self.finished_signal.emit(self.task_id, self.output_format, False, error_msg)
        finally:
            if hasattr(self, '_log_file_handle') and self._log_file_handle:
                try:
                    self._log_file_handle.close()
                except Exception:
                    pass
                self._log_file_handle = None
            if hasattr(self, 'process') and self.process is not None:
                try:
                    for stream in [self.process.stdout, self.process.stderr]:
                        if stream:
                            try:
                                stream.close()
                            except Exception:
                                pass
                    if self.process.poll() is None:
                        try:
                            self.process.terminate()
                            self.process.wait(timeout=2)
                        except Exception:
                            pass
                finally:
                    self.process = None
                    self.pid = None

    def create_anidub_mkv(self):
        """Простое создание MKV контейнера без перекодирования (без обращения к GUI из потока)."""
        try:
            output_dir = os.path.join(self.output_base_dir, "Anidub")
            os.makedirs(output_dir, exist_ok=True)
            log_dir = os.path.join(output_dir, "Logs")
            os.makedirs(log_dir, exist_ok=True)

            base_name = os.path.splitext(os.path.basename(self.input_file))[0]
            file_name = base_name.replace('_', '.')
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

            log_file = os.path.join(log_dir,  f"{file_name}-{timestamp}.log")
            output_file = os.path.join(output_dir, f"{file_name}.mkv")

            # ВАЖНО: НЕ трогаем self.logger и ничего из GUI внутри потока!

            # Просто перепаковываем в MKV без изменений
            cmd = [
                "ffmpeg",
                "-i", self.input_file,
                "-c:v", "copy",
                "-c:a", "copy",
                "-y",
                output_file
            ]

            self.run_ffmpeg(cmd, log_file, output_file)

        except Exception as e:
            self.finished_signal.emit(self.task_id, "anidub", False, str(e))

    def _run_command_with_progress(self, cmd, log_file, progress_start, progress_end, progress_message):
        """Вспомогательный метод для выполнения команд с прогрессом"""
        self.update_signal.emit(self.task_id, "anidub", progress_message)
        
        with open(log_file, 'a') as log:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
            )
            
            for line in process.stdout:
                if not self.running:
                    process.kill()
                    break
                log.write(line)
                # Плавное увеличение прогресса
                current_progress = progress_start + (progress_end - progress_start) * 0.9  # 90% диапазона
                self.progress_signal.emit(self.task_id, "anidub", int(current_progress))
            
            if self.running:
                process.wait()
                # Гарантированно устанавливаем конечный прогресс
                self.progress_signal.emit(self.task_id, "anidub", progress_end)

    def encode_xvid(self):
        try:
            output_dir = os.path.join(self.output_base_dir, "Xvid")
            log_dir = os.path.join(output_dir, "Logs")
            os.makedirs(log_dir, exist_ok=True)
            
            base_name = os.path.splitext(os.path.basename(self.input_file))[0]
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            
            # Формируем правильное имя для XviD
            base_name = base_name.replace("HD1080p.", "").replace("WEBRip.", "WEBRip.XviD.")
            output_name = f"{base_name}.avi"
            
            log_file = os.path.join(log_dir, f"{base_name}-{timestamp}.log")
            output_file = os.path.join(output_dir, output_name)
            
            # Определяем фильтры в зависимости от типа задачи
            if self.task_type == 'tvhub_encoding':
                # Для TVHub: сохраняем соотношение сторон
                video_filter = "scale=720:-2:flags=lanczos,hqdn3d=1.0:1.0:3.0:3.0,unsharp=5:5:0.5:5:5:0.5,setsar=1:1"
            else:
                # Для обычного кодирования: растягиваем
                video_filter = "scale=720:400:flags=lanczos,hqdn3d=1.0:1.0:3.0:3.0,unsharp=5:5:0.5:5:5:0.5,setsar=1:1"
            
            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-stats_period", "1",
                "-i", self.input_file,
                "-progress", "pipe:1",
                "-vf", video_filter,
                "-c:v", "libxvid",
                "-b:v", "1500k",
                "-c:a", "libmp3lame",
                "-b:a", "192k",
                "-y",
                output_file
            ]
            
            self.run_ffmpeg(cmd, log_file, output_file)
            
        except Exception as e:
            error_msg = f"XviD encoding failed: {str(e)}"
            self.finished_signal.emit(self.task_id, "xvid", False, error_msg)

    def encode_400p(self):
        try:
            output_info = self._generate_output_filename('x264', '.mp4')
            output_file = output_info['output']
            log_file = output_info['log']

            if sys.platform == "darwin" or not NVENC_EXE:
                # macOS (или без NVEncC): ffmpeg libx264
                _log("INFO", f"{os.path.basename(self.input_file)} -> 400p (ffmpeg)")
                scale = "scale=720:-2" if self.task_type == 'tvhub_encoding' else "scale=720:400"
                cmd = [
                    "ffmpeg",
                    "-hide_banner", "-stats_period", "1",
                    "-i", self.input_file,
                    "-progress", "pipe:1",
                    "-vf", scale + ":flags=lanczos",
                    "-c:v", "libx264",
                    "-b:v", "1500k",
                    "-maxrate", "1500k",
                    "-bufsize", "3000k",
                    "-profile:v", "high",
                    "-level", "3.2",
                    "-c:a", "aac",
                    "-b:a", "192k",
                    "-y", output_file
                ]
                self.run_ffmpeg(cmd, log_file, output_file)
            else:
                _log("INFO", f"{os.path.basename(self.input_file)} -> 400p (NVEncC)")
                out_res = "720x-2" if self.task_type == 'tvhub_encoding' else "720x400"
                cmd = [
                    NVENC_EXE,
                    "-i", self.input_file,
                    "-o", output_file,
                    "-c", "h264",
                    "--vbr", "1500",
                    "--max-bitrate", "1500",
                    "--vbv-bufsize", "3000",
                    "--profile", "high",
                    "--level", "3.2",
                    "--output-res", out_res,
                    "--audio-codec", "aac",
                    "--audio-bitrate", "192",
                    "-f", "mp4",
                ]
                self.run_nvenc(cmd, log_file, output_file)

        except Exception as e:
            self.finished_signal.emit(self.task_id, "400p", False, str(e))

    def encode_720p(self):
        try:
            output_info = self._generate_output_filename('720p', '.mkv')
            output_file = output_info['output']
            log_file = output_info['log']

            if sys.platform == "darwin" or not NVENC_EXE:
                _log("INFO", f"{os.path.basename(self.input_file)} -> 720p (ffmpeg)")
                scale = "scale=1280:-2" if self.task_type == 'tvhub_encoding' else "scale=1280:720"
                cmd = [
                    "ffmpeg",
                    "-hide_banner", "-stats_period", "1",
                    "-i", self.input_file,
                    "-progress", "pipe:1",
                    "-vf", scale + ":flags=lanczos",
                    "-c:v", "libx264",
                    "-b:v", "4000k",
                    "-maxrate", "4000k",
                    "-bufsize", "8000k",
                    "-profile:v", "high",
                    "-level", "4.1",
                    "-c:a", "copy",
                    "-y", output_file
                ]
                self.run_ffmpeg(cmd, log_file, output_file)
            else:
                _log("INFO", f"{os.path.basename(self.input_file)} -> 720p (NVEncC)")
                out_res = "1280x-2" if self.task_type == 'tvhub_encoding' else "1280x720"
                cmd = [
                    NVENC_EXE,
                    "-i", self.input_file,
                    "-o", output_file,
                    "-c", "h264",
                    "--vbr", "4000",
                    "--max-bitrate", "4000",
                    "--vbv-bufsize", "8000",
                    "--profile", "high",
                    "--level", "4.1",
                    "--output-res", out_res,
                    "--audio-copy",
                    "-f", "mkv",
                ]
                self.run_nvenc(cmd, log_file, output_file)

        except Exception as e:
            self.finished_signal.emit(self.task_id, "720p", False, str(e))
            
    def encode_x265(self):
        try:
            output_dir = os.path.join(self.output_base_dir, "x265")
            os.makedirs(output_dir, exist_ok=True)
            log_dir = os.path.join(output_dir, "Logs")
            os.makedirs(log_dir, exist_ok=True)

            base_name = os.path.splitext(os.path.basename(self.input_file))[0]
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            base_name = base_name.replace("HD1080p.", "").replace("WEBRip.", "WEBRip.x265.")
            output_name = f"{base_name}.mp4"
            log_file = os.path.join(log_dir, f"{base_name}-{timestamp}.log")
            output_file = os.path.join(output_dir, output_name)

            if sys.platform == "darwin" or not NVENC_EXE:
                _log("INFO", f"{os.path.basename(self.input_file)} -> x265 (ffmpeg libx265)")
                cmd = [
                    "ffmpeg",
                    "-hide_banner", "-stats_period", "1",
                    "-i", self.input_file,
                    "-progress", "pipe:1",
                    "-c:v", "libx265",
                    "-preset", "medium",
                    "-crf", "22",
                    "-tag:v", "hvc1",
                    "-c:a", "copy",
                    "-y", output_file
                ]
                self.run_ffmpeg(cmd, log_file, output_file)
            else:
                _log("INFO", f"{os.path.basename(self.input_file)} -> x265 (NVEncC)")
                cmd = [
                    NVENC_EXE,
                    "-i", self.input_file,
                    "-o", output_file,
                    "-c", "hevc",
                    "-u", "p7",
                    "--tune", "hq",
                    "--vbr", "2000",
                    "--max-bitrate", "4000",
                    "--vbv-bufsize", "6000",
                    "--profile", "main",
                    "--audio-copy",
                    "-f", "mp4",
                ]
                self.run_nvenc(cmd, log_file, output_file)
        except Exception as e:
            error_msg = f"x265 encoding failed: {str(e)}"
            self.finished_signal.emit(self.task_id, "x265", False, error_msg)

    def _run_command(self, cmd, log_file):
        """Запуск отдельной команды с логированием"""
        try:
            with open(log_file, 'a') as log:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True,
                    bufsize=1,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
                )
                
                for line in process.stdout:
                    if not self.running:
                        process.kill()
                        break
                    log.write(line)
                
                if self.running:
                    process.wait()
                    return process.returncode == 0
                return False
        except Exception as e:
            _log("ERR", f"Command execution failed: {str(e)}")
            return False
                
    def _generate_output_filename(self, replacement_suffix, output_ext, replace_underscores=False):
        """
        Генерация имен выходных файлов:
        - TVHub файлы (с task_type='tvhub_encoding'): заменяем .1080p на .400p/.720p с правильными путями
        - AniDub: спец-правила (как было)
        - Обычные файлы: старая логика
        """
        import re
        import os

        base_name = os.path.splitext(os.path.basename(self.input_file))[0]
        
        # 1) Обработка TVHub задач с task_type='tvhub_encoding'
        if self.task_type == 'tvhub_encoding':
            # Определяем реальный суффикс для файла и папку назначения
            if replacement_suffix == 'x264':  # 400p кодирование
                file_suffix = '400p'
                ext = '.mp4'
                output_dir = os.path.join(self.output_base_dir, "x264")
            elif replacement_suffix == '720p':
                file_suffix = '720p'
                ext = '.mkv'
                output_dir = os.path.join(self.output_base_dir, "720")
            elif replacement_suffix == 'xvid':
                file_suffix = 'xvid'
                ext = '.avi'
                output_dir = os.path.join(self.output_base_dir, "Xvid")
            elif replacement_suffix == 'x265':
                file_suffix = 'x265'
                ext = '.mp4'
                output_dir = os.path.join(self.output_base_dir, "x265")
            else:
                file_suffix = replacement_suffix
                ext = output_ext
                output_dir = self.output_base_dir
            
            # Заменяем .1080p на нужный суффикс
            if '.1080p' in base_name:
                output_name = base_name.replace(".1080p", f".{file_suffix}") + ext
            else:
                # Если нет .1080p, добавляем суффикс в конец
                output_name = f"{base_name}.{file_suffix}{ext}"
            
            os.makedirs(output_dir, exist_ok=True)
            log_dir = os.path.join(output_dir, "Logs")
            os.makedirs(log_dir, exist_ok=True)
            
            return {
                "output": os.path.join(output_dir, output_name),
                "log": os.path.join(
                    log_dir,
                    f"{os.path.splitext(output_name)[0]}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
                )
            }
        
        # 2) Оригинальная логика для не-TVHub задач
        if replace_underscores:
            base_name = base_name.replace('_', '.')
        
        # TVHUB — оставляем текущую логику (для старых TVHUB задач без task_type)
        if 'TVHUB' in base_name.upper():
            clean = re.sub(r'(?:\.(?:1080p|720p|400p))+$', '', base_name, flags=re.IGNORECASE)
            
            # ИСПРАВЛЕНИЕ: Для TVHub формата x264 используем суффикс "400p" вместо "x264"
            if replacement_suffix == 'x264':
                actual_suffix = '400p'
            else:
                actual_suffix = replacement_suffix
                
            output_name = f"{clean}.{actual_suffix}{output_ext}"
            output_dir = os.path.join(self.output_base_dir, {
                "xvid":   "Xvid",
                "400p":   "x264",
                "720p":   "720",
                "anidub": "Anidub",
                "tvhub":  "TVHub"
            }.get(self.output_format, "")) or self.output_base_dir
        
        # AniDub — без изменений
        elif re.search(r'AniDub\.com', base_name, re.IGNORECASE):
            name = base_name
            
            if replacement_suffix in ('400p', 'x264') and output_ext == '.mp4':
                # HD1080p убираем, WEBRip → WEBRip.x264
                name = re.sub(r'\bHD1080p\.', '', name, flags=re.IGNORECASE)
                name = re.sub(r'\bWEBRip\.', 'WEBRip.x264.', name, flags=re.IGNORECASE)
                output_name = f"{name}.mp4"
                output_dir = os.path.join(self.output_base_dir, "x264")
            
            elif replacement_suffix == '720p' and output_ext == '.mkv':
                name = re.sub(r'\bHD1080p\b', 'HD720p', name, flags=re.IGNORECASE)
                output_name = f"{name}.mkv"
                output_dir = os.path.join(self.output_base_dir, "720")
            
            else:
                clean = re.sub(r'(?:\.(?:1080p|720p|400p))+$', '', base_name, flags=re.IGNORECASE)
                output_name = f"{clean}.{replacement_suffix}{output_ext}"
                output_dir = os.path.join(self.output_base_dir, {
                    "xvid":   "Xvid",
                    "400p":   "x264",
                    "720p":   "720",
                    "anidub": "Anidub",
                    "tvhub":  "TVHub"
                }.get(self.output_format, "")) or self.output_base_dir
        
        # Обычные файлы (не TVHub и не AniDub)
        else:
            if replacement_suffix in ('400p', 'x264') and output_ext == '.mp4':
                # Совпадает со схемой XviD, только вставляем x264:
                name = base_name.replace("HD1080p.", "")
                name = re.sub(r'\bWEBRip\.', 'WEBRip.x264.', name, flags=re.IGNORECASE)
                output_name = f"{name}.mp4"
                output_dir = os.path.join(self.output_base_dir, "x264")
            
            elif replacement_suffix == '720p' and output_ext == '.mkv':
                name = base_name.replace("HD1080p.", "HD720p.")
                output_name = f"{name}.mkv"
                output_dir = os.path.join(self.output_base_dir, "720")
            
            elif replacement_suffix == 'x265' and output_ext == '.mp4':
                name = base_name.replace("HD1080p.", "").replace("WEBRip.", "WEBRip.x265.")
                output_name = f"{name}.mp4"
                output_dir = os.path.join(self.output_base_dir, "x265")
            
            else:
                clean = re.sub(r'(?:\.(?:1080p|720p|400p))+$', '', base_name, flags=re.IGNORECASE)
                output_name = f"{clean}.{replacement_suffix}{output_ext}"
                output_dir = os.path.join(self.output_base_dir, {
                    "xvid":   "Xvid",
                    "400p":   "x264",
                    "720p":   "720",
                    "anidub": "Anidub",
                    "tvhub":  "TVHub"
                }.get(self.output_format, "")) or self.output_base_dir
        
        os.makedirs(output_dir, exist_ok=True)
        log_dir = os.path.join(output_dir, "Logs")
        os.makedirs(log_dir, exist_ok=True)
        
        return {
            "output": os.path.join(output_dir, output_name),
            "log": os.path.join(
                log_dir,
                f"{os.path.splitext(output_name)[0]}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
            )
        }

    def _handle_error(self, error):
        """Общая обработка ошибок"""
        error_msg = f"{type(error).__name__}: {str(error)}"
        _log("ERR", error_msg)
        self.finished_signal.emit(self.task_id, self.output_format, False, error_msg)

    def _cleanup_failed(self, output_file):
        """Очистка после неудачного выполнения"""
        try:
            if output_file and os.path.exists(output_file):
                os.remove(output_file)
        except Exception as e:
            _log("WARN", f"Failed to clean up output file: {str(e)}")

    def _cleanup_resources(self):
        """Гарантированное освобождение ресурсов"""
        try:
            # Закрываем файловые дескрипторы процесса
            if hasattr(self, 'process') and self.process:
                try:
                    # Закрываем все потоки
                    for stream in [self.process.stdout, self.process.stderr, getattr(self.process, 'stdin', None)]:
                        if stream:
                            try:
                                stream.close()
                            except:
                                pass
                    
                    # Завершаем процесс, если он еще работает
                    if self.process.poll() is None:
                        try:
                            self.process.terminate()
                            self.process.wait(timeout=2)
                        except:
                            try:
                                self.process.kill()
                                self.process.wait(timeout=1)
                            except:
                                pass
                except Exception as e:
                    _log("WARN", f"Resource cleanup error: {str(e)}")
        finally:
            # Гарантируем освобождение ресурсов
            self.process = None
            self.pid = None
            
            # Закрываем лог-файл, если он был открыт
            if hasattr(self, '_log_file_handle') and self._log_file_handle:
                try:
                    self._log_file_handle.close()
                except:
                    pass
                self._log_file_handle = None

    def _terminate_process(self):
        """Принудительное завершение процесса"""
        try:
            if sys.platform == "win32":
                subprocess.run(
                    ['taskkill', '/F', '/T', '/PID', str(self.pid)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=5
                )
            else:
                os.killpg(os.getpgid(self.pid), signal.SIGKILL)
        except Exception as e:
            _log("WARN", f"Process termination failed: {str(e)}")

    def stop(self):
        """Полная остановка кодирования с очисткой"""
        self.running = False
        self._kill_ffmpeg_process()
        
        # Дополнительная очистка для Windows
        if sys.platform == "win32":
            try:
                subprocess.run(['taskkill', '/F', '/IM', 'ffmpeg.exe'],
                              stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL)
            except:
                pass

class PipelineLogger:
    def __init__(
        self,
        textbox=None,
        write_fn: Optional[Callable[[str], None]] = None,
        shorten_keep: int = 28
    ):
        self.textbox = textbox
        self.write_fn = write_fn
        self.shorten_keep = shorten_keep
        self.tasks: Dict[str, Dict] = {}
        self._seq: int = 0
        self._order: Dict[str, int] = {}
        self._created_at: Dict[str, str] = {}  # task_id -> время создания

    # ---------- публичные методы ----------
    def task_init(self, task_id: str, title: str):
        t = self._ensure(task_id)
        if task_id not in self._order:
            self._seq += 1
            self._order[task_id] = self._seq
            self._created_at[task_id] = datetime.now().strftime("%H:%M:%S")
        t["title"] = title or t["title"]
        self.render()

    def source_removed(self, task_id: str, source_name: str):
        t = self._ensure(task_id)
        t["source_removed"] = source_name
        self.render()

    def duration_once(self, task_id: str, duration_str: str):
        t = self._ensure(task_id)
        if duration_str and not t["duration"]:
            t["duration"] = duration_str
            self.render()

    def track(self, task_id: str, fmt: str, message: str):
        t = self._ensure(task_id)
        tr: List[str] = t["tracks"].setdefault(fmt, [])
        if tr and tr[-1] == message:
            return
        tr.append(message)
        self.render()
        
    def _task_state(self, data):
        """Возвращает (done, total, errors, terminals) по дорожкам."""
        tracks = data.get("tracks", {})
        total = len(tracks)
        done = 0
        errors = 0
        terminals = 0
        for evs in tracks.values():
            has_done = any(e.startswith("готово") for e in evs)
            has_err  = any(e.startswith("ошибка") for e in evs)
            if has_done: 
                done += 1
                terminals += 1
            elif has_err:
                errors += 1
                terminals += 1
        return done, total, errors, terminals

    def _is_complete(self, data):
        """Все дорожки завершены (готово/ошибка) — можно сворачивать."""
        done, total, errors, terminals = self._task_state(data)
        return total > 0 and terminals == total

    def clear_task(self, task_id: str):
        if task_id in self.tasks:
            del self.tasks[task_id]
        if task_id in self._order:
            del self._order[task_id]
        if task_id in self._created_at:
            del self._created_at[task_id]
        self.render()

    # ---------- рендер ----------
    def render(self) -> str:
        ordered_ids = sorted(self.tasks.keys(), key=lambda tid: self._order.get(tid, 10**9))
        lines: List[str] = []

        for task_id in ordered_ids:
            data = self.tasks[task_id]
            title = data.get("title") or f"{task_id}"
            num = self._order.get(task_id, 0)
            created = self._created_at.get(task_id, "--:--:--")

            done, total, errors, terminals = self._task_state(data)
            collapsed = self._is_complete(data)

            if collapsed:
                # СВЁРНУТАЯ ШАПКА для полностью завершённой задачи
                if errors:
                    # есть ошибки
                    summary = f"завершено: {done}/{total}, ошибок: {errors}"
                else:
                    summary = f"статус: {done}/{total} дорожек готово"
                lines.append(f"Задача #{num} • {created}: {self._short(title)}  —  {summary}")
                lines.append("")  # разделитель
                continue

            # НЕСВЁРНУТЫЙ ПОЛНЫЙ БЛОК
            lines.append(f"Задача #{num} • {created}: {self._short(title)}")

            # Подготовка
            prep = []
            if data.get("source_removed"):
                prep.append(f"исходник удалён: {self._short(data['source_removed'])}")
            if data.get("duration"):
                prep.append(f"длительность: {data['duration']}")
            tracks = data.get("tracks", {})
            have_prep = bool(prep)
            have_tracks = bool(tracks)

            if have_prep:
                lines.append("  ├─ Подготовка")
                for i, p in enumerate(prep):
                    branch = "  │    " if have_tracks or i < len(prep) - 1 else "       "
                    lines.append(f"{branch}• {p}")

            if have_tracks:
                lines.append("  ├─ Кодирование")
                for fmt, events in tracks.items():
                    chain = " → ".join(events) if events else ""
                    lines.append(f"  │    • {fmt}: {chain}" if chain else f"  │    • {fmt}")

                lines.append("  └─ Итог")
                if errors:
                    lines.append(f"       • завершено: {done}/{total}, ошибок: {errors}")
                else:
                    lines.append(f"       • статус: {done}/{total} дорожек готово")

            lines.append("")

        text = "\n".join(lines).rstrip()
        if self.textbox is not None:
            try:
                self.textbox.setPlainText(text)
                cursor = self.textbox.textCursor()
                cursor.movePosition(cursor.End)
                self.textbox.setTextCursor(cursor)
            except Exception:
                pass
        elif self.write_fn is not None:
            self.write_fn(text)
        return text

    # ---------- вспомогательные ----------
    def _ensure(self, task_id: str) -> Dict:
        if task_id not in self.tasks:
            self.tasks[task_id] = {
                "title": "",
                "source_removed": None,
                "duration": None,
                "tracks": {}
            }
        return self.tasks[task_id]

    def _short(self, s: str) -> str:
        k = self.shorten_keep
        if not s:
            return ""
        return s if len(s) <= k*2+3 else f"{s[:k]}...{s[-k:]}"
                
class FileCopyThread(QThread):
    update_progress = pyqtSignal(int)
    copy_finished = pyqtSignal(str, str, bool)  # from_path, to_path, success
    
    def __init__(self, from_path, to_path):
        super().__init__()
        self.from_path = from_path
        self.to_path = to_path
        self.canceled = False
        
    def run(self):
        try:
            total_size = os.path.getsize(self.from_path)
            copied = 0
            block_size = 1024 * 1024  # 1MB chunks
            
            with open(self.from_path, 'rb') as src, open(self.to_path, 'wb') as dst:
                while not self.canceled:
                    data = src.read(block_size)
                    if not data:
                        break
                    dst.write(data)
                    copied += len(data)
                    progress = int((copied / total_size) * 100)
                    self.update_progress.emit(progress)  # Отправляем прогресс без задержки
            
            if not self.canceled:
                self.copy_finished.emit(self.from_path, self.to_path, True)
            else:
                # Удаляем частично скопированный файл
                try:
                    os.remove(self.to_path)
                except:
                    pass
                self.copy_finished.emit(self.from_path, self.to_path, False)
                
        except Exception as e:
            self.copy_finished.emit(self.from_path, self.to_path, False)
    
    def cancel(self):
        self.canceled = True

class VideoEncoderGUI(QMainWindow):
    encoder_queue_total_changed = pyqtSignal(int)

    def __init__(self, kafka_available=False):
        super().__init__()
        self.setWindowTitle("Advanced Video Encoder")
        self.setGeometry(100, 100, 1000, 800)
        self.setAcceptDrops(True)
        self.check_ffmpeg_availability()
        self._cleanup_zombie_processes()
        
        self.threads = {}
        self.tasks = {}
        self.current_task_id = 0
        self.progress_bars = {}
        # Папка сохранения (выбирается при первом запуске кодирования; по умолчанию ~/Movies/Rip на macOS)
        self.output_base_dir = None

        # Пагинация
        self.current_page = 0
        self.tasks_per_page = 10
        self.total_pages = 1
        
        # Kafka Consumer
        self.kafka_consumer = None
        self.kafka_thread = None
        self.KAFKA_AVAILABLE = kafka_available
        
        # Настройки Kafka
        self.kafka_bootstrap_servers = '192.168.1.223:9092'
        self.kafka_topic = 'video_encoding_tasks'
        self.kafka_producer = None  # Producer для ftp_upload_queue и encoding_ready
        
        # Ограничение одновременных задач
        self.max_concurrent_tasks = 6
        self.active_tasks_count = 0
        self.pending_tasks = []  # Очередь ожидающих задач
        
        self.is_closing = False
        self.eta = {}
        self.eta_sec = {}  # Для хранения времени в секундах
        
        self.max_eta_label = None
        
        self.total_encoding_timer = QTimer()
        self.total_encoding_timer.timeout.connect(self.update_total_encoding_time)
        self.total_encoding_start_time = None
        
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout(main_widget)
        
        top_layout = QHBoxLayout()
        top_layout.setContentsMargins(0, 0, 0, 10)  # Отступ снизу

        # Заголовок слева
        title_label = QLabel("🎬 Video Encoder Pro")
        title_label.setStyleSheet("""
            QLabel {
                font: bold 14pt 'Segoe UI';
                color: #2c3e50;
                padding: 5px;
            }
        """)
        top_layout.addWidget(title_label)

        # Растягивающийся спейсер
        top_layout.addStretch()

        # Максимальный ETA справа
        self.max_eta_label = QLabel("⏱ Макс. ETA: --:--:--")
        self.max_eta_label.setStyleSheet("""
            QLabel {
                font: 10pt 'Segoe UI';
                color: #555;
                padding: 8px 12px;
                background-color: #f8f9fa;
                border-radius: 6px;
                border: 1px solid #dee2e6;
                min-width: 180px;
            }
        """)
        self.max_eta_label.setAlignment(Qt.AlignCenter)
        self.max_eta_label.setToolTip("Максимальное оставшееся время среди всех активных задач")
        top_layout.addWidget(self.max_eta_label)

        # Добавляем верхнюю панель в основной layout
        layout.addLayout(top_layout)
        
        # Queue Section
        queue_group = QGroupBox("Encoding Queue (Right-click for actions | Del to remove)")
        queue_layout = QVBoxLayout()
        queue_layout.setContentsMargins(5, 5, 5, 5)

        self.task_list = QListWidget()
        self.task_list.setMinimumHeight(200)
        self.task_list.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.task_list.setStyleSheet("""
            QListWidget {
                background-color: white;
                border: 1px solid #ddd;
                font: 10pt 'Segoe UI';
            }
            QListWidget::item {
                padding: 8px;
                border-bottom: 1px solid #eee;
                color: black;
            }
            QListWidget::item:hover {
                background-color: #f5f5f5;
            }
            QListWidget::item:selected {
                background-color: #0078d7;
                color: white;
                border: none;
            }
        """)
        self.task_list.setAlternatingRowColors(True)
        self.task_list.setSelectionMode(QListWidget.SingleSelection)
        self.task_list.setVerticalScrollMode(QListWidget.ScrollPerPixel)
        
        # Отключение скроллбаров
        self.task_list.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.task_list.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        
        def task_list_key_press_event(event):
            if event.key() == Qt.Key_Delete:
                selected_items = self.task_list.selectedItems()
                if selected_items:
                    for item in selected_items:
                        task_id = item.data(Qt.UserRole)
                        self.remove_task(task_id)
            else:
                QListWidget.keyPressEvent(self.task_list, event)
        
        self.task_list.keyPressEvent = task_list_key_press_event
        self.task_list.itemSelectionChanged.connect(self.update_visible_progress)
        
        queue_layout.addWidget(self.task_list)
        
        # Pagination controls
        pagination_layout = QHBoxLayout()
        self.prev_page_btn = QPushButton("←")
        self.next_page_btn = QPushButton("→")
        self.page_label = QLabel("Страница 1/1")
        
        # Стилизация кнопок пагинации
        pagination_style = """
            QPushButton {
                min-width: 30px;
                max-width: 30px;
                font: bold 12pt 'Segoe UI';
            }
            QLabel {
                font: 10pt 'Segoe UI';
                padding: 0 10px;
            }
        """
        self.prev_page_btn.setStyleSheet(pagination_style)
        self.next_page_btn.setStyleSheet(pagination_style)
        self.page_label.setStyleSheet(pagination_style)
        
        pagination_layout.addWidget(self.prev_page_btn)
        pagination_layout.addWidget(self.page_label)
        pagination_layout.addWidget(self.next_page_btn)
        pagination_layout.setAlignment(Qt.AlignCenter)
        
        queue_layout.addLayout(pagination_layout)
        queue_group.setLayout(queue_layout)
        layout.addWidget(queue_group)
        
        # Автозапуск Kafka потребителя
        QTimer.singleShot(1000, self.auto_start_kafka)
        
        # Encoding Options
        options_group = QGroupBox("Encoding Options")
        options_layout = QHBoxLayout()
        
        self.xvid_check = QPushButton("Xvid (720x400)")
        self.xvid_check.setCheckable(True)
        self.xvid_check.setChecked(True)
        self.xvid_check.setStyleSheet("QPushButton:checked { background-color: lightgreen; }")
        
        self.p400_check = QPushButton("400p (720x400)")
        self.p400_check.setCheckable(True)
        self.p400_check.setChecked(True)
        self.p400_check.setStyleSheet("QPushButton:checked { background-color: lightgreen; }")
        
        self.p720_check = QPushButton("720p (1280x720)")
        self.p720_check.setCheckable(True)
        self.p720_check.setChecked(True)
        self.p720_check.setStyleSheet("QPushButton:checked { background-color: lightgreen; }")
        
        self.x265_check = QPushButton("x265 (HEVC)")
        self.x265_check.setCheckable(True)
        self.x265_check.setChecked(False)
        self.x265_check.setStyleSheet("QPushButton:checked { background-color: lightgreen; }")
        
        # Add Anidub button
        self.anidub_btn = QPushButton("AniDub")
        self.anidub_btn.setCheckable(False)
        self.anidub_btn.setStyleSheet("""
            QPushButton {
                background-color: #ff9800;
                color: white;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #fb8c00;
            }
        """)
        self.anidub_btn.setAcceptDrops(True)
        self.anidub_btn.clicked.connect(self.browse_file_for_anidub)
        self.anidub_btn.dragEnterEvent = self.dragEnterEvent
        self.anidub_btn.dropEvent = self.anidub_drop_event
        
        # Add TVHub button
        self.tvhub_btn = QPushButton("TVHub")
        self.tvhub_btn.setCheckable(False)
        self.tvhub_btn.setStyleSheet("""
            QPushButton {
                background-color: #673ab7;
                color: white;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #5e35b1;
            }
        """)
        self.tvhub_btn.setAcceptDrops(True)
        self.tvhub_btn.clicked.connect(self.browse_file_for_tvhub)
        self.tvhub_btn.dragEnterEvent = self.dragEnterEvent
        self.tvhub_btn.dropEvent = self.tvhub_drop_event
        
        options_layout.addWidget(self.xvid_check)
        options_layout.addWidget(self.p400_check)
        options_layout.addWidget(self.p720_check)
        options_layout.addWidget(self.x265_check)
        options_layout.addWidget(self.anidub_btn)
        options_layout.addWidget(self.tvhub_btn)
        options_group.setLayout(options_layout)
        layout.addWidget(options_group)
        
        # Progress Bars Container
        self.progress_group = QGroupBox("Current Encoding Progress")
        self.progress_layout = QVBoxLayout()
        
        # Создаем ScrollArea с фиксированным размером
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setStyleSheet("""
            QScrollArea {
                border: 1px solid #ddd;
                border-radius: 4px;
                background: white;
            }
            QScrollBar:vertical {
                width: 12px;
                background: #f0f0f0;
            }
            QScrollBar::handle:vertical {
                background: #c0c0c0;
                min-height: 20px;
                border-radius: 6px;
            }
        """)
        
        # Контейнер для содержимого
        self.scroll_content = QWidget()
        self.scroll_content_layout = QVBoxLayout(self.scroll_content)
        self.scroll_content_layout.setContentsMargins(10, 10, 10, 10)
        self.scroll_content_layout.setSpacing(8)
        
        self.scroll_area.setWidget(self.scroll_content)
        self.progress_layout.addWidget(self.scroll_area)
        self.progress_group.setLayout(self.progress_layout)
        layout.addWidget(self.progress_group)
        
        # Log Output
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.verticalScrollBar().valueChanged.connect(self.log_scroll_changed)
        self.auto_scroll_log = True
        layout.addWidget(QLabel("Log:"))
        layout.addWidget(self.log_output)
        self.logger = PipelineLogger(textbox=self.log_output)
        
        # Control Buttons
        control_layout = QHBoxLayout()
        self.global_start_btn = QPushButton("Start All")
        self.global_start_btn.clicked.connect(self.start_all_tasks)
        self.global_stop_btn = QPushButton("Stop All")
        self.global_stop_btn.clicked.connect(self.stop_all_tasks)
        self.global_stop_btn.setEnabled(False)
        self.clear_btn = QPushButton("Clear Completed")
        self.clear_btn.clicked.connect(self.clear_completed)
        control_layout.addWidget(self.global_start_btn)
        control_layout.addWidget(self.global_stop_btn)
        control_layout.addWidget(self.clear_btn)
        layout.addLayout(control_layout)
        
        # Настройки для плавного скролла
        self.task_list.itemSelectionChanged.connect(self._handle_selection_change)
        self.task_list.setVerticalScrollMode(QListWidget.ScrollPerPixel)
        scroll_bar = self.task_list.verticalScrollBar()
        scroll_bar.setSingleStep(20)
        
        # Настройка контекстного меню для списка задач
        self.task_list.setContextMenuPolicy(Qt.CustomContextMenu)
        self.task_list.customContextMenuRequested.connect(self.show_context_menu)

        self.resource_timer = QTimer()
        self.resource_timer.timeout.connect(self.check_resources)
        self.resource_timer.start(30000)
        
        # Подключение кнопок пагинации
        self.prev_page_btn.clicked.connect(self.prev_page)
        self.next_page_btn.clicked.connect(self.next_page)
        
        self.is_stopping_all = False
        
        # Инициализация пагинации
        self.update_pagination()
        
        QTimer.singleShot(1500, self.auto_start_kafka)
        
        # Таймер для очистки памяти (добавить в конец __init__)
        self.memory_cleanup_timer = QTimer()
        self.memory_cleanup_timer.timeout.connect(self.periodic_memory_cleanup)
        self.memory_cleanup_timer.start(5 * 60 * 1000)  # Каждые 5 минут
        
        # Инициализация других таймеров для отслеживания
        self.other_timers = []
        
        self.encoder_queue_total_changed.emit(0)
        
    def periodic_memory_cleanup(self):
        """Периодическая очистка устаревших данных"""
        try:
            # Очищаем старые записи ETA
            self.cleanup_old_eta_data()
            
            # Очищаем ссылки на удаленные потоки
            self.cleanup_dead_threads()
            
            # Принудительный сбор мусора
            import gc
            gc.collect()
            
            # Логируем использование памяти (для отладки)
            if psutil:
                memory = psutil.virtual_memory()
                self.append_to_log(f"[Memory] Использовано: {memory.percent}%")
        except Exception as e:
            _log("ERR", f"Ошибка очистки памяти: {str(e)}")

    def cleanup_old_eta_data(self, max_age_minutes=60):
        """Очищает старые записи ETA для завершенных задач"""
        # Удаляем ETA для задач, которых нет в self.tasks
        for task_id in list(self.eta.keys()):
            if task_id not in self.tasks:
                self.eta.pop(task_id, None)
                self.eta_sec.pop(task_id, None)

        # Удаляем ETA для завершенных задач, которые не кодируются
        for task_id, task in self.tasks.items():
            if task.get('status') == 'done':
                # Проверяем, есть ли активные потоки для этой задачи
                has_active = any(
                    (task_id, fmt) in self.threads and self.threads[(task_id, fmt)].isRunning()
                    for fmt in task.get('formats', {})
                )
                if not has_active:
                    self.eta.pop(task_id, None)
                    self.eta_sec.pop(task_id, None)

    def cleanup_dead_threads(self):
        """Очищает ссылки на завершенные потоки"""
        threads_to_remove = []
        for key, thread in self.threads.items():
            try:
                if not thread.isRunning():
                    threads_to_remove.append(key)
            except Exception:
                threads_to_remove.append(key)
        
        for key in threads_to_remove:
            try:
                thread = self.threads.pop(key, None)
                if thread:
                    thread.deleteLater()
            except Exception:
                pass

    def set_kafka_available(self, available):
        """Устанавливает доступность Kafka извне"""
        self.KAFKA_AVAILABLE = available
        if available:
            _log("INFO", "Kafka доступен в VideoEncoderGUI")
            # Автозапуск Kafka при установке доступности
            QTimer.singleShot(500, self.auto_start_kafka)
        else:
            _log("WARN", "Kafka недоступен в VideoEncoderGUI")

    def auto_start_kafka(self):
        """Автоматический запуск Kafka потребителя"""
        if not self.KAFKA_AVAILABLE:
            return
            
        # Проверяем, не запущен ли уже потребитель
        if hasattr(self, 'kafka_thread') and self.kafka_thread and self.kafka_thread.isRunning():
            return
            
        try:
            self.kafka_thread = KafkaConsumerThread(
                bootstrap_servers=self.kafka_bootstrap_servers,
                topic=self.kafka_topic,
                kafka_available=self.KAFKA_AVAILABLE
            )
            self.kafka_thread.message_received.connect(self.handle_kafka_message)
            self.kafka_thread.start()
            
            _log("KAFKA", "запущен")
            
        except Exception as e:
            _log("ERR", f"Ошибка автозапуска Kafka: {str(e)}")

    def stop_kafka_consumer(self):
        """Остановка Kafka потребителя и producer (Conv→FTP)."""
        if hasattr(self, 'kafka_producer') and self.kafka_producer:
            try:
                self.kafka_producer.flush(5)
                self.kafka_producer.close()
                _log("KAFKA", "producer: закрыт")
            except Exception as e:
                _log("ERR", f"Ошибка закрытия Kafka Producer: {e}")
            self.kafka_producer = None
        if hasattr(self, 'kafka_thread') and self.kafka_thread:
            try:
                # Вызываем метод stop() потока
                self.kafka_thread.stop()
                
                # Ждем завершения потока
                if not self.kafka_thread.wait(3000):
                    # Если не завершился за 3 секунды - принудительно
                    self.kafka_thread.terminate()
                    self.kafka_thread.wait(1000)
                _log("KAFKA", "остановлен")
            except Exception as e:
                _log("ERR", f"Error stopping Kafka thread: {str(e)}")
            finally:
                self.kafka_thread = None

    def _get_kafka_producer(self):
        """Ленивое создание Kafka producer для отправки в FTP-очередь и encoding_ready."""
        if not self.KAFKA_AVAILABLE:
            return None
        if self.kafka_producer is None:
            try:
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=[self.kafka_bootstrap_servers],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3,
                    request_timeout_ms=30000
                )
                _log("KAFKA", "producer: подключен")
            except Exception as e:
                _log("ERR", f"Kafka Producer (Conv->FTP): {e}")
                self.kafka_producer = None
        return self.kafka_producer

    def _send_ftp_queue_message(self, release_id, source_file):
        """Отправить в топик ftp_upload_queue: добавлена серия (1080p). FTP откроет файл как «добавить файлы» и начнёт загрузку 1080p; остальные форматы придут по encoding_ready."""
        producer = self._get_kafka_producer()
        if not producer:
            return
        try:
            producer.send(KAFKA_TOPIC_FTP_QUEUE, value={
                'release_id': release_id,
                'source_file': source_file,
            }, key=release_id.encode('utf-8'))
            _log("KAFKA", f"ftp_upload_queue: серия {release_id} (1080p)")
        except Exception as e:
            _log("ERR", f"Отправка в {KAFKA_TOPIC_FTP_QUEUE}: {e}")

    def _send_encoding_ready_message(self, release_id, format_name, file_path):
        """Отправить в топик encoding_ready: файл закодирован, готов к загрузке на FTP."""
        producer = self._get_kafka_producer()
        if not producer:
            return
        try:
            producer.send(KAFKA_TOPIC_ENCODING_READY, value={
                'release_id': release_id,
                'format': format_name,
                'file_path': file_path,
            }, key=release_id.encode('utf-8'))
            _log("KAFKA", f"encoding_ready: {os.path.basename(file_path)} ({format_name})")
        except Exception as e:
            _log("ERR", f"Отправка в {KAFKA_TOPIC_ENCODING_READY}: {e}")

    @pyqtSlot(str)
    def handle_kafka_message(self, message):
        """Обработка сообщений от Kafka потребителя"""
        if message.startswith("ERROR:"):
            _log("ERR", f"Kafka: {message}")
            return
            
        if os.path.exists(message):
            # Добавляем файл в очередь кодирования
            _log("KAFKA", f"Получен файл для обработки: {os.path.basename(message)}")
            self.add_files_to_queue([message])
            # Запуск будет через _start_next_task в add_files_to_queue
        else:
            # Игнорируем служебные сообщения Kafka
            if "Kafka потребитель запущен" not in message and "ожидание задач" not in message:
                _log("ERR", f"Kafka: Файл не существует: {message}")
        
    def check_ffmpeg_availability(self):
        """Проверяет доступность ffmpeg в системе"""
        try:
            result = subprocess.run(['ffmpeg', '-version'], 
                                  stdout=subprocess.PIPE, 
                                  stderr=subprocess.PIPE,
                                  text=True)
            if result.returncode != 0:
                QMessageBox.critical(self, "FFmpeg Error", 
                                   "FFmpeg not found or not working. Please install FFmpeg and add it to PATH.")
                sys.exit(1)
        except FileNotFoundError:
            QMessageBox.critical(self, "FFmpeg Error", 
                               "FFmpeg not found. Please install FFmpeg and add it to PATH.")
            sys.exit(1)
        
    def update_pagination(self):
        """Обновляет состояние пагинации"""
        total_tasks = self.task_list.count()
        self.total_pages = max(1, (total_tasks + self.tasks_per_page - 1) // self.tasks_per_page)
        self.current_page = min(self.current_page, self.total_pages - 1)
        
        self.page_label.setText(f"Страница {self.current_page + 1}/{self.total_pages}")
        self.prev_page_btn.setEnabled(self.current_page > 0)
        self.next_page_btn.setEnabled(self.current_page < self.total_pages - 1)
        
        # Скрываем все задачи
        for i in range(total_tasks):
            item = self.task_list.item(i)
            if item:
                item.setHidden(True)
        
        # Показываем только задачи текущей страницы
        start_idx = self.current_page * self.tasks_per_page
        end_idx = min(start_idx + self.tasks_per_page, total_tasks)
        
        for i in range(start_idx, end_idx):
            item = self.task_list.item(i)
            if item:
                item.setHidden(False)

    def next_page(self):
        """Переход на следующую страницу"""
        if self.current_page < self.total_pages - 1:
            self.current_page += 1
            self.update_pagination()

    def prev_page(self):
        """Переход на предыдущую страницу"""
        if self.current_page > 0:
            self.current_page -= 1
            self.update_pagination()
        
    def check_resources(self):
        """Проверка использования памяти и CPU"""
        try:
            import psutil
            mem = psutil.virtual_memory()
            if mem.percent > 90:
                self.append_to_log("WARNING: High memory usage!")
                self.stop_all_tasks()
        except ImportError:
            pass

    def anidub_drop_event(self, event: QDropEvent):
        """Handle file drops on Anidub button"""
        urls = event.mimeData().urls()
        files = [url.toLocalFile() for url in urls if url.isLocalFile()]
        if files:
            self.create_anidub_mkv(files[0])
        event.acceptProposedAction() 

    def browse_file_for_anidub(self):
        """Browse for file to create Anidub MKV"""
        file, _ = QFileDialog.getOpenFileName(self, "Select MP4 File", "", "MP4 Files (*.mp4)")
        if file:
            self.create_anidub_mkv(file)

    def create_anidub_mkv(self, input_file):
        """Создание задачи для Anidub MKV с последующим удалением исходного файла и запуском кодирования (всё GUI)."""
        if not os.path.isfile(input_file):
            QMessageBox.warning(self, "Ошибка", "Файл не найден!")
            return

        if not input_file.lower().endswith('.mp4'):
            QMessageBox.warning(self, "Ошибка", "Файл должен быть в формате MP4!")
            return

        # Создаем уникальный ID задачи
        task_id = str(self.current_task_id)
        self.current_task_id += 1

        # Формируем правильное имя выходного файла
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        file_name = base_name.replace('_', '.')
        output_path = os.path.join(self.get_output_base_dir(), "Anidub", f"{file_name}.mkv")

        # Создаем элементы прогресса
        self.progress_bars[task_id] = {
            'anidub': self._create_progress_bar(),
            'status_label': QLabel("Ожидание...")
        }
        self.progress_bars[task_id]['status_label'].setStyleSheet("""
            QLabel { color: #757575; font: 10pt 'Segoe UI'; margin-top: 5px; font-style: italic; }
        """)

        # Создаем запись задачи
        self.tasks[task_id] = {
            'file': input_file,
            'original_file': input_file,
            'status': 'waiting',
            'type': 'anidub',
            'formats': {
                'anidub': {
                    'status': 'waiting',
                    'progress': 0,
                    'pid': None,
                    'output_path': output_path
                }
            }
        }

        # ЛОГ-«шапка» — ДЕЛАЕМ В GUI-ПОТОКЕ, не в рабочем потоке
        try:
            self.logger.task_init(task_id, os.path.basename(input_file))
        except Exception:
            pass

        # Элемент списка
        item = QListWidgetItem(f"⏸ Ожидание Anidub — {os.path.basename(input_file)}")
        item.setData(Qt.UserRole, task_id)
        item.setData(Qt.UserRole + 1, "waiting")
        item.setBackground(QColor(255, 248, 225))
        self.task_list.insertItem(0, item)
        self.task_list.scrollToItem(item, QListWidget.PositionAtTop)
        self.task_list.setCurrentItem(item)

        # Запуск кодирования
        self.start_encoding_thread(task_id, 'anidub')

        self.update_global_controls()
        self.update_visible_progress()
        self.update_pagination()

    def tvhub_drop_event(self, event: QDropEvent):
        """Handle file drops on TVHub button"""
        urls = event.mimeData().urls()
        files = [url.toLocalFile() for url in urls if url.isLocalFile()]
        if files:
            self.create_tvhub_task(files[0])
        event.acceptProposedAction() 

    def browse_file_for_tvhub(self):
        """Browse for file to create TVHub encodes"""
        file, _ = QFileDialog.getOpenFileName(self, "Select Video File", "", "Video Files (*.mp4 *.mkv *.avi)")
        if file:
            self.create_tvhub_task(file)

    def create_tvhub_task(self, input_file):
        """Create task for TVHub encoding with proper filename handling"""
        if not os.path.isfile(input_file):
            QMessageBox.warning(self, "Ошибка", "Файл не найден!")
            return
        
        tvhub_dir = os.path.join(self.get_output_base_dir(), "TVHub")
        os.makedirs(tvhub_dir, exist_ok=True)
        
        # Генерируем правильное имя файла
        tvhub_base = self.generate_tvhub_filename(input_file)  # Уже содержит .1080p
        tvhub_file = os.path.join(tvhub_dir, f"{tvhub_base}.mkv")
        
        # Если файл уже существует в TVHub
        if os.path.exists(tvhub_file):
            self._process_existing_tvhub_file(tvhub_file)
            return
        
        # Создаем ЗАДАЧУ КОПИРОВАНИЯ (отдельная задача)
        task_id = f"copy_{int(time.time())}_{self.current_task_id}"
        self.current_task_id += 1
        
        # Задача копирования
        self.tasks[task_id] = {
            'file': input_file,
            'status': 'copying',
            'type': 'tvhub_copy',
            'progress': 0,
            'target_file': tvhub_file,
            'original_file': input_file,  # Сохраняем оригинальный путь
            'formats': {}  # Нет форматов для задачи копирования
        }
        
        # Создаем элементы прогресса для копирования
        self.progress_bars[task_id] = {
            'copy': self._create_progress_bar(),
            'status_label': QLabel("Копирование файла...")
        }
        
        # Устанавливаем синий стиль для прогресс-бара копирования
        bar = self.progress_bars[task_id]['copy']
        bar.setStyleSheet(self._get_progress_style('copying'))
        bar.setFormat("0%")
        
        self.progress_bars[task_id]['status_label'].setStyleSheet("""
            QLabel { color: #757575; font: 10pt 'Segoe UI'; margin-top: 5px; font-style: italic; }
        """)
        
        # ЛОГ: «шапка» задачи копирования
        self.logger.task_init(task_id, f"Копирование: {os.path.basename(input_file)}")
        
        # Добавляем в список задач
        item = QListWidgetItem(f"📁 Копирование TVHub — {os.path.basename(input_file)}")
        item.setData(Qt.UserRole, task_id)
        item.setData(Qt.UserRole + 1, "copying")
        item.setBackground(QColor(255, 243, 224))  # Оранжевый для копирования
        item.setToolTip(f"Копирование в TVHub\nИз: {input_file}\nВ: {tvhub_file}")
        self.task_list.insertItem(0, item)
        
        # Запускаем копирование
        self.copy_thread = FileCopyThread(input_file, tvhub_file)
        self.copy_thread.update_progress.connect(
            lambda p: self._update_copy_progress(task_id, p))
        self.copy_thread.copy_finished.connect(
            lambda f, t, s: self._handle_copy_finished(task_id, f, t, s))
        self.copy_thread.start()
        
    def _update_copy_progress(self, task_id, progress):
        """Обновление прогресса копирования"""
        try:
            if self.is_closing or task_id not in self.tasks:
                return
                
            # Обновляем прогресс в данных задачи
            self.tasks[task_id]['progress'] = progress
            
            # Обновляем элемент в списке задач
            for i in range(self.task_list.count()):
                item = self.task_list.item(i)
                if item.data(Qt.UserRole) == task_id:
                    item.setText(f"📁 {os.path.basename(self.tasks[task_id]['file'])} (Копирование TVHub {progress}%)")
                    break
                    
            # Обновляем прогресс-бар, если задача выбрана
            selected_items = self.task_list.selectedItems()
            if selected_items and selected_items[0].data(Qt.UserRole) == task_id:
                if 'copy' in self.progress_bars[task_id]:
                    self.progress_bars[task_id]['copy'].setValue(progress)
                    
        except Exception as e:
            _log("ERR", f"Ошибка обновления прогресса: {str(e)}")

    def _handle_copy_finished(self, task_id, from_path, to_path, success):
        """Обработка завершения копирования - создание ОТДЕЛЬНОЙ задачи кодирования"""
        if task_id not in self.tasks:
            return

        # Удаляем ETA для задачи копирования (если было)
        if task_id in self.eta:
            if 'copy' in self.eta[task_id]:
                del self.eta[task_id]['copy']
            if not self.eta[task_id]:
                del self.eta[task_id]
        
        # Обновляем метку с максимальным ETA
        self.update_max_eta_label()
        
        if success:
            try:
                # 1) Удаляем исходный файл после копирования и логируем это
                if from_path and os.path.exists(from_path):
                    try:
                        os.remove(from_path)
                        self.logger.source_removed(task_id, os.path.basename(from_path))
                        self.append_to_log(f"✅ Исходный файл удален: {os.path.basename(from_path)}")
                    except Exception as e_del:
                        self.append_to_log(f"⚠️ Не удалось удалить исходный файл: {str(e_del)}")
                
                # 2) Обновляем задачу копирования на статус "done"
                self.tasks[task_id]['status'] = 'done'
                self.tasks[task_id]['progress'] = 100
                
                # Обновляем прогресс-бар копирования (меняем на "✓ Готово")
                if task_id in self.progress_bars and 'copy' in self.progress_bars[task_id]:
                    bar = self.progress_bars[task_id]['copy']
                    if not sip.isdeleted(bar):
                        bar.setValue(100)
                        bar.setFormat("✓ Готово")  # Изменено здесь
                        bar.setStyleSheet(self._get_progress_style('done'))
                
                # Обновляем элемент в списке (меняем на "✓ Готово")
                for i in range(self.task_list.count()):
                    item = self.task_list.item(i)
                    if item and item.data(Qt.UserRole) == task_id:
                        item.setText(f"✓ Готово — {os.path.basename(to_path)}")
                        item.setBackground(QColor(232, 245, 233))  # Зеленый для завершенного
                        item.setToolTip(f"Копирование завершено\nФайл готов для кодирования: {to_path}")
                        break
                
                # 3) Создаём ОТДЕЛЬНУЮ задачу кодирования TVHub
                self._create_tvhub_encoding_task(to_path)
                
                # 4) Обновляем лог копирования
                self.logger.track(task_id, 'copy', 'готово')  # Изменено здесь

            except Exception as e:
                # Переводим временную задачу в ошибку
                if task_id in self.tasks:
                    self.tasks[task_id]['status'] = 'error'
                    self.tasks[task_id]['error'] = f"Ошибка: {str(e)}"
                    self._update_task_item_in_list(task_id)
                QMessageBox.warning(self, "Ошибка", f"Копирование завершено, но создание задачи кодирования не удалось: {str(e)}")

        else:
            # Копирование не удалось
            self.tasks[task_id]['status'] = 'error'
            self.tasks[task_id]['error'] = "Ошибка копирования"
            
            # Обновляем прогресс-бар
            if task_id in self.progress_bars and 'copy' in self.progress_bars[task_id]:
                bar = self.progress_bars[task_id]['copy']
                if not sip.isdeleted(bar):
                    bar.setValue(0)
                    bar.setFormat("✗ Ошибка копирования")
                    bar.setStyleSheet(self._get_progress_style('error'))
            
            self._update_task_item_in_list(task_id)
            QMessageBox.warning(self, "Ошибка", "Не удалось скопировать файл!")
        
        # Перерисуем прогресс/кнопки
        self.update_visible_progress()
        self.update_global_controls()
        
    def _create_tvhub_encoding_task(self, tvhub_file):
        """Создает ОТДЕЛЬНУЮ задачу кодирования для TVHub файла"""
        try:
            # Получаем базовое имя (без расширения)
            tvhub_base = os.path.splitext(os.path.basename(tvhub_file))[0]  # "David.TVHUB.FILM.WEB.1080p"
            
            # Создаем ID задачи кодирования
            task_id = str(self.current_task_id)
            self.current_task_id += 1
            
            # Формируем выходные пути, заменяя .1080p на .400p/.720p
            # Если в имени уже есть .1080p
            base_dir = self.get_output_base_dir()
            if tvhub_base.endswith('.1080p'):
                output_400p = os.path.join(base_dir, "x264", tvhub_base.replace(".1080p", ".400p") + ".mp4")
                output_720p = os.path.join(base_dir, "720", tvhub_base.replace(".1080p", ".720p") + ".mkv")
            else:
                output_400p = os.path.join(base_dir, "x264", f"{tvhub_base}.400p.mp4")
                output_720p = os.path.join(base_dir, "720", f"{tvhub_base}.720p.mkv")

            # Создаем элементы интерфейса (прогресс-бары) в правильном порядке
            self.progress_bars[task_id] = {
                '400p': self._create_progress_bar(),
                '720p': self._create_progress_bar(),
                'status_label': QLabel("Ожидание кодирования TVHub...")
            }
            self.progress_bars[task_id]['status_label'].setStyleSheet("""
                QLabel { color: #757575; font: 10pt 'Segoe UI'; margin-top: 5px; font-style: italic; }
            """)

            # Создаем запись задачи кодирования
            self.tasks[task_id] = {
                'file': tvhub_file,
                'status': 'waiting',
                'type': 'tvhub_encoding',
                'formats': {
                    '400p': {'status': 'waiting', 'progress': 0, 'pid': None, 'output_path': output_400p},
                    '720p': {'status': 'waiting', 'progress': 0, 'pid': None, 'output_path': output_720p}
                }
            }

            # ЛОГ: «шапка» задачи кодирования
            self.logger.task_init(task_id, f"Кодирование TVHub: {os.path.basename(tvhub_file)}")

            # Добавляем в список задач
            display_name = os.path.basename(tvhub_file)
            item = QListWidgetItem(f"⏸ Ожидание TVHub — {display_name}")
            item.setData(Qt.UserRole, task_id)
            item.setData(Qt.UserRole + 1, "waiting")
            item.setBackground(QColor(230, 225, 245))  # Фиолетовый для TVHub кодирования
            item.setToolTip(f"Кодирование TVHub\nФайл: {tvhub_file}\n400p → {output_400p}\n720p → {output_720p}")
            self.task_list.insertItem(0, item)

            # Добавляем форматы в очередь ожидания
            for fmt in ['400p', '720p']:
                if (task_id, fmt) not in self.pending_tasks:
                    self.pending_tasks.append((task_id, fmt))
                    _log("INFO", f"TVHub кодирование: формат {fmt} в очередь")

            # Запускаем кодирование, если есть свободные слоты
            QTimer.singleShot(500, lambda: self._start_next_task())

            self.update_global_controls()
            self.update_visible_progress()
            self.update_pagination()

            _log("OK", f"Создана задача кодирования TVHub: {os.path.basename(tvhub_file)}")
            self.append_to_log(f"✅ Создана задача кодирования TVHub: {os.path.basename(tvhub_file)}")

        except Exception as e:
            QMessageBox.warning(self, "Ошибка", f"Ошибка создания задачи кодирования TVHub: {str(e)}")

    def _remove_task_from_list(self, task_id):
        """Удаление задачи из списка по ID"""
        for i in range(self.task_list.count()):
            item = self.task_list.item(i)
            if item.data(Qt.UserRole) == task_id:
                self.task_list.takeItem(i)
                break
        if task_id in self.tasks:
            del self.tasks[task_id]
        if task_id in self.progress_bars:
            del self.progress_bars[task_id]

    def _process_existing_tvhub_file(self, tvhub_file):
        """Обработка случая когда файл уже существует в папке TVHub"""
        reply = QMessageBox.question(
            self,
            "Файл уже существует",
            "Файл уже находится в папке TVHub. Использовать его?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes
        )
        
        if reply == QMessageBox.Yes:
            # Просто создаем задачу с существующим файлом
            self._create_tvhub_task_with_existing_file(tvhub_file)
            
    def _create_tvhub_task_with_existing_file(self, tvhub_file):
        """Создает задачу для существующего TVHub файла"""
        try:
            # Получаем базовое имя (без расширения)
            tvhub_base = os.path.splitext(os.path.basename(tvhub_file))[0]
            
            # Создаем ID задачи
            task_id = str(self.current_task_id)
            self.current_task_id += 1

            base_dir = self.get_output_base_dir()
            if tvhub_base.endswith('.1080p'):
                output_400p = os.path.join(base_dir, "x264", tvhub_base.replace(".1080p", ".400p") + ".mp4")
                output_720p = os.path.join(base_dir, "720", tvhub_base.replace(".1080p", ".720p") + ".mkv")
            else:
                output_400p = os.path.join(base_dir, "x264", f"{tvhub_base}.400p.mp4")
                output_720p = os.path.join(base_dir, "720", f"{tvhub_base}.720p.mkv")

            # Создаем элементы интерфейса (прогресс-бары)
            self.progress_bars[task_id] = {
                '400p': self._create_progress_bar(),
                '720p':  self._create_progress_bar(),
                'status_label': QLabel("Ожидание...")
            }

            # Создаем запись задачи с ПРАВИЛЬНЫМ ТИПОМ
            self.tasks[task_id] = {
                'file': tvhub_file,
                'status': 'waiting',
                'type': 'tvhub_encoding',  # ВАЖНО: правильный тип для TVHub кодирования
                'formats': {
                    '400p': {'status': 'waiting', 'progress': 0, 'pid': None, 'output_path': output_400p},
                    '720p': {'status': 'waiting', 'progress': 0, 'pid': None, 'output_path': output_720p}
                }
            }

            # ЛОГ: «шапка» задачи
            self.logger.task_init(task_id, os.path.basename(tvhub_file))

            # Добавляем в список задач
            display_name = os.path.basename(tvhub_file)
            item = QListWidgetItem(f"⏸ Ожидание TVHub — {display_name}")
            item.setData(Qt.UserRole, task_id)
            item.setData(Qt.UserRole + 1, "waiting")
            item.setBackground(QColor(230, 225, 245))
            self.task_list.insertItem(0, item)

            # Добавляем форматы в очередь ожидания
            for fmt in ['400p', '720p']:
                if (task_id, fmt) not in self.pending_tasks:
                    self.pending_tasks.append((task_id, fmt))
                    _log("INFO", f"TVHub (существующий): формат {fmt} в очередь")

            # Запускаем кодирование, если есть свободные слоты
            QTimer.singleShot(500, lambda: self._start_next_task())

            self.update_global_controls()
            self.update_visible_progress()
            self.update_pagination()

            _log("OK", f"Создана задача TVHub (из существующего): {os.path.basename(tvhub_file)}")
            self.append_to_log(f"✅ Создана задача кодирования TVHub (из существующего файла): {os.path.basename(tvhub_file)}")

        except Exception as e:
            QMessageBox.warning(self, "Ошибка", f"Ошибка создания задачи: {str(e)}")
        
    def generate_tvhub_filename(self, input_filename):
        """
        Нормализация имени для TVHub с раздельной логикой для фильмов и сериалов.
        
        - Фильм: "<Title>.TVHUB.FILM.WEB.1080p"
        - Сериал (если найден sXXeYY/sXeY): "<Title>.sxxeyy.TVHUB.WEB.1080p"
          * S/E всегда строчные и нулевые (s01e05)
        - Отрезаем всё правее первого "TVHUB"/"TVHub"/"tvhub" (без учёта регистра)
        - Удаляем шумовые токены (качество, рипы, языки, хвосты релиз-групп и т.п.)
        - НЕ удаляем .1080p в конце имени
        """
        import os, re

        base = os.path.splitext(os.path.basename(input_filename))[0]

        # Единые разделители
        tokens = base.replace('_', '.').replace(' ', '.').split('.')
        tokens = [t for t in tokens if t]

        # 1) Обрезаем по первому упоминанию TVHUB (любой регистр)
        cut_idx = next((i for i, t in enumerate(tokens) if 'TVHUB' in t.upper()), len(tokens))
        tokens = tokens[:cut_idx]

        # 2) Удаляем шумовые токены, но НЕ удаляем '1080p' и подобные качества в конце
        # Сначала определим, есть ли качество в конце (1080p, 720p и т.д.)
        quality_at_end = None
        if tokens:
            last_token = tokens[-1].upper()
            # Проверяем, является ли последний токен качеством
            if re.match(r'^(?:2160|1080|720|480|400)P?$', last_token):
                quality_at_end = tokens[-1]  # Сохраняем качество
                tokens = tokens[:-1]  # Удаляем качество из основного списка

        # Теперь удаляем остальные шумовые токены
        noise = re.compile(
            r'^(?:HD)?(?:2160p?|1080p?|720p?|480p?|400p?|HD1080|HD720|HD480|HD400)$'
            r'|^(?:WEBRip|WEB-?DL|WEBDL|HDRip|BDRip|DVD(?:Rip)?|CAMRip|TS|HDTV)$'
            r'|^(?:Rus|Eng|Multi|Dub|Sub|VO|TV|Kotik|Alexiym|graywww|Ser)$'
            r'|^(?:x264|x265|HEVC|H264)$',
            re.IGNORECASE
        )
        
        # Удаляем шум, но только если это не последний токен (качество уже удалили)
        filtered_tokens = [t for t in tokens if not noise.match(t)]

        # 3) Также удаляем общие паттерны качества без HD префикса (только если не в конце)
        quality_pattern = re.compile(r'^\d{3,4}p?$', re.IGNORECASE)
        filtered_tokens = [t for t in filtered_tokens if not quality_pattern.match(t)]

        # 4) Ищем эпизод sXXeYY (без учёта регистра). Нормализуем к sxxeyy (нижний регистр, 2-значные числа)
        ep_idx = None
        ep_norm = None
        for i, t in enumerate(filtered_tokens):
            m = re.fullmatch(r'(?i)(s)(\d{1,2})(e)(\d{1,2})', t)
            if m:
                season = int(m.group(2))
                episode = int(m.group(4))
                ep_norm = f"s{season:02d}e{episode:02d}"
                ep_idx = i
                break

        # 5) Сериал или фильм?
        result = ""
        if ep_idx is not None:
            # СЕРИАЛ: оставляем Title до эпизода (не включая мусор), эпизод — нормализованный
            title = '.'.join(filtered_tokens[:ep_idx]).strip('.')
            if title.endswith('.'):
                title = title[:-1]
            result = f"{title}.{ep_norm}.TVHUB.WEB"
        else:
            # ФИЛЬМ: оставляем всё «чистое» (возможно, с годом), добавляем FILM
            title = '.'.join(filtered_tokens).strip('.')
            result = f"{title}.TVHUB.FILM.WEB"

        # 6) Добавляем качество обратно, если оно было
        if quality_at_end:
            result = f"{result}.{quality_at_end}"
        else:
            # Если качества не было, добавляем .1080p по умолчанию для TVHub
            result = f"{result}.1080p"

        return result

    def log_scroll_changed(self, value):
        """Обработчик изменения положения скроллбара лога"""
        scroll_bar = self.log_output.verticalScrollBar()
        self.auto_scroll_log = (value == scroll_bar.maximum())
        
    def _safe_scroll_to_item(self, item):
        """Безопасная прокрутка к элементу без 'подпрыгивания'"""
        # Сохраняем текущую позицию скролла
        scroll_bar = self.task_list.verticalScrollBar()
        old_pos = scroll_bar.value()
        
        # Прокручиваем к элементу
        self.task_list.scrollToItem(item, QListWidget.PositionAtTop)
        
        # Если это последний элемент, корректируем позицию
        if self.task_list.row(item) == self.task_list.count() - 1:
            scroll_bar.setValue(scroll_bar.maximum())
        else:
            # Для других элементов восстанавливаем точную позицию
            scroll_bar.setValue(old_pos)
            
    def _handle_selection_change(self):
        """Обработчик изменения выбора с корректной прокруткой"""
        selected = self.task_list.selectedItems()
        if not selected:
            return
        
        item = selected[0]
        self._safe_scroll_to_item(item)
        self.update_visible_progress()
        
    def _normalize_path(self, path):
        """Нормализует путь для кроссплатформенности"""
        if not path:
            return path
        return os.path.normpath(os.path.abspath(path))

    def append_to_log(self, message):
        """Добавляет сообщение в лог с учетом настроек скроллинга"""
        scroll_bar = self.log_output.verticalScrollBar()
        at_bottom = self.auto_scroll_log
        
        # Сохраняем текущую позицию курсора и скролла
        cursor = self.log_output.textCursor()
        old_pos = cursor.position()
        old_scroll_pos = scroll_bar.value()
        
        # Добавляем текст
        self.log_output.append(message)
        
        # Восстанавливаем позицию или прокручиваем вниз
        if at_bottom:
            scroll_bar.setValue(scroll_bar.maximum())
        else:
            cursor.setPosition(old_pos)
            self.log_output.setTextCursor(cursor)
            scroll_bar.setValue(old_scroll_pos)

    def dragEnterEvent(self, event: QDragEnterEvent):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
    
    def dropEvent(self, event: QDropEvent):
        urls = event.mimeData().urls()
        files = [url.toLocalFile() for url in urls if url.isLocalFile()]
        self.add_files_to_queue(files)
        event.acceptProposedAction()
    
    def add_files_to_queue(self, files):
        """Добавляет файлы в очередь и запускает их при наличии свободных слотови ОБНОВЛЯЕТ GUI"""
        if not files:
            return

        added_count = 0
        self.task_list.setUpdatesEnabled(False)
        try:
            for file in files:
                if not os.path.isfile(file):
                    continue

                # Проверка на дубликаты
                if any(task.get('file') == file for task in self.tasks.values()):
                    _log("INFO", f"Файл уже в очереди: {os.path.basename(file)}")
                    continue

                task_id = str(self.current_task_id)
                self.current_task_id += 1

                # Определяем какие форматы будут использоваться
                active_formats = []
                if self.xvid_check.isChecked(): active_formats.append('xvid')
                if self.p400_check.isChecked(): active_formats.append('400p')
                if self.p720_check.isChecked(): active_formats.append('720p')
                if self.x265_check.isChecked(): active_formats.append('x265')

                if not active_formats:
                    _log("WARN", f"Нет активных форматов для файла: {os.path.basename(file)}")
                    continue

                # Создаем структуру задачи
                self.tasks[task_id] = {
                    'file': file,
                    'status': 'waiting',
                    'formats': {fmt: {'status': 'waiting', 'progress': 0, 'pid': None}
                               for fmt in active_formats}
                }

                # Создаем прогресс-бары для задачи
                self.progress_bars[task_id] = {}
                for fmt in active_formats:
                    self.progress_bars[task_id][fmt] = self._create_progress_bar()

                # ЛОГ: «шапка» задачи
                self.logger.task_init(task_id, os.path.basename(file))

                # Создаем элемент списка
                item = QListWidgetItem(f"⏸ Ожидание — {os.path.basename(file)}")
                item.setData(Qt.UserRole, task_id)
                item.setData(Qt.UserRole + 1, "waiting")
                item.setToolTip(file)
                item.setBackground(QColor(255, 248, 225))  # Желтый для ожидания
                self.task_list.insertItem(0, item)
                
                # Добавляем все форматы в очередь ожидания (FIFO)
                for fmt in active_formats:
                    if (task_id, fmt) not in self.pending_tasks:
                        self.pending_tasks.append((task_id, fmt))
                        _log("INFO", f"Формат {fmt} в очередь")
                
                # Уведомляем FTP один раз: добавлена серия (1080p), FTP откроет файл и начнёт загрузку 1080p; остальные форматы подкинутся по encoding_ready
                if self.KAFKA_AVAILABLE:
                    self._send_ftp_queue_message(task_id, file)
                
                added_count += 1

            if added_count > 0:
                _log("OK", f"Добавлено {added_count} файлов, в очереди: {len(self.pending_tasks)} форматов")
                # СРАЗУ обновляем GUI
                self.update_global_controls()
                # Пытаемся запустить задачи из очереди
                QTimer.singleShot(100, self._start_next_task)

        except Exception as e:
            self.append_to_log(f"Ошибка добавления файлов: {str(e)}")
            _log("ERR", f"Ошибка добавления файлов: {str(e)}")
        finally:
            self.task_list.setUpdatesEnabled(True)
            self.update_visible_progress()
            self.update_pagination()
            # Принудительно обновляем GUI
            QApplication.processEvents()

    def update_visible_progress(self):
        """Обновляет отображение прогресса в фиксированной области прокрутки"""
        try:
            # Очищаем предыдущее содержимое
            while self.scroll_content_layout.count():
                item = self.scroll_content_layout.takeAt(0)
                if item.widget():
                    item.widget().deleteLater()

            selected_items = self.task_list.selectedItems()
            if not selected_items:
                self._show_no_task_message()
                return

            task_id = selected_items[0].data(Qt.UserRole)
            if task_id not in self.tasks:
                return

            task = self.tasks[task_id]
            
            # Основной контейнер
            main_widget = QWidget()
            main_layout = QVBoxLayout(main_widget)
            main_layout.setContentsMargins(0, 0, 0, 0)
            
            # Заголовок в зависимости от типа задачи
            task_type = task.get('type', '')
            if task_type == 'tvhub_copy':
                file_label = QLabel(f"Копирование TVHub: {os.path.basename(task['file'])}")
                file_label.setStyleSheet("""
                    QLabel {
                        font-weight: bold;
                        font: 11pt 'Segoe UI';
                        padding-bottom: 10px;
                        color: #FF9800;
                    }
                """)
            elif task_type == 'tvhub_encoding':
                file_label = QLabel(f"Кодирование TVHub: {os.path.basename(task['file'])}")
                file_label.setStyleSheet("""
                    QLabel {
                        font-weight: bold;
                        font: 11pt 'Segoe UI';
                        padding-bottom: 10px;
                        color: #673AB7;
                    }
                """)
            else:
                file_label = QLabel(f"Файл: {os.path.basename(task['file'])}")
                file_label.setStyleSheet("""
                    QLabel {
                        font-weight: bold;
                        font: 11pt 'Segoe UI';
                        padding-bottom: 10px;
                        color: #333;
                    }
                """)
            main_layout.addWidget(file_label)

            # --- Обработка задачи копирования TVHub ---
            if task_type == 'tvhub_copy':
                if 'copy' in self.progress_bars[task_id]:
                    row = QWidget()
                    row_layout = QHBoxLayout(row)
                    row_layout.setContentsMargins(0, 0, 0, 0)

                    # Метка
                    name_label = QLabel("Копирование:")
                    name_label.setFixedWidth(80)
                    name_label.setStyleSheet("font: 10pt 'Segoe UI';")
                    row_layout.addWidget(name_label)

                    # Прогресс-бар копирования
                    bar = self.progress_bars[task_id]['copy']
                    if sip.isdeleted(bar):
                        bar = self._create_progress_bar()
                        self.progress_bars[task_id]['copy'] = bar
                    
                    bar.setFixedHeight(24)
                    bar.setValue(task.get('progress', 0))
                    bar.setFormat(self._get_progress_text(task.get('progress', 0), task.get('status', 'waiting')))
                    bar.setStyleSheet(self._get_progress_style(task.get('status', 'waiting')))
                    row_layout.addWidget(bar)

                    # Статус
                    status_label = QLabel(self._get_status_text(task.get('status', 'waiting')))
                    status_color = self._get_status_color(task.get('status', 'waiting'))
                    status_label.setStyleSheet(f"color: {status_color}; font: 9pt 'Segoe UI'; margin-left: 10px; min-width: 100px;")
                    row_layout.addWidget(status_label)

                    main_layout.addWidget(row)
            
            # --- Обработка задачи кодирования TVHub и обычных задач ---
            else:
                # Определяем порядок отображения форматов
                format_order = ['xvid', '400p', '720p', 'x265', 'anidub', 'tvhub']
                
                # Добавляем прогресс-бары в правильном порядке
                for fmt in format_order:
                    if task_id in self.tasks and fmt in self.tasks[task_id]['formats']:
                        data = self.tasks[task_id]['formats'][fmt]
                        
                        if task_id in self.progress_bars and fmt in self.progress_bars[task_id]:
                            row = QWidget()
                            row_layout = QHBoxLayout(row)
                            row_layout.setContentsMargins(0, 0, 0, 0)

                            # Метка формата
                            name_label = QLabel({
                                'xvid': 'XviD:',
                                '400p': '400p:',
                                '720p': '720p:',
                                'x265': 'x265:',
                                'anidub': 'AniDub:',
                                'tvhub': 'TVHub:'
                            }.get(fmt, fmt + ':'))
                            name_label.setFixedWidth(60)
                            name_label.setStyleSheet("font: 10pt 'Segoe UI';")
                            row_layout.addWidget(name_label)

                            # Прогресс-бар
                            bar = self.progress_bars[task_id][fmt]
                            if sip.isdeleted(bar):
                                bar = self._create_progress_bar()
                                self.progress_bars[task_id][fmt] = bar
                            
                            bar.setFixedHeight(24)
                            bar.setValue(data['progress'])
                            bar.setFormat(self._get_progress_text(data['progress'], data['status']))
                            bar.setStyleSheet(self._get_progress_style(data['status']))
                            row_layout.addWidget(bar)

                            # Статус
                            status_label = QLabel(self._get_status_text(data['status']))
                            status_color = self._get_status_color(data['status'])
                            status_label.setStyleSheet(f"color: {status_color}; font: 9pt 'Segoe UI'; margin-left: 10px; min-width: 100px;")
                            row_layout.addWidget(status_label)

                            main_layout.addWidget(row)

            # --- Группа "Результаты" для ВСЕХ типов задач ---
            output_paths = []
            
            # Для задач копирования TVHub
            if task_type == 'tvhub_copy' and task.get('target_file'):
                output_paths.append(f"TVHub: {task['target_file']}")
            
            # Для задач кодирования TVHub и обычных задач
            elif task.get('formats'):
                for fmt, data in task.get('formats', {}).items():
                    if data.get('status') == 'done' and 'output_path' in data:
                        # Форматируем пути в зависимости от формата
                        if fmt == '400p':
                            prefix = 'x264:'
                        elif fmt == '720p':
                            prefix = '720p:'
                        elif fmt == 'xvid':
                            prefix = 'XviD:'
                        elif fmt == 'x265':
                            prefix = 'x265:'
                        elif fmt == 'tvhub':
                            prefix = 'TVHub:'
                        else:
                            prefix = f'{fmt.upper()}:'
                        
                        output_paths.append(f"{prefix} {data['output_path']}")
            
            # Добавляем группу "Результаты", если есть что показывать
            if output_paths:
                paths_group = QGroupBox("Результаты:")
                paths_group.setStyleSheet("""
                    QGroupBox {
                        font: 10pt 'Segoe UI';
                        margin-top: 15px;
                        border: 1px solid #ddd;
                        border-radius: 4px;
                        padding-top: 15px;
                    }
                """)
                paths_layout = QVBoxLayout()
                
                for path in output_paths:
                    path_label = QLabel(path)
                    path_label.setStyleSheet("""
                        QLabel {
                            font: 9pt 'Consolas';
                            color: #555;
                            padding: 5px;
                            background-color: #f5f5f5;
                            border-radius: 3px;
                        }
                    """)
                    path_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
                    paths_layout.addWidget(path_label)
                
                paths_group.setLayout(paths_layout)
                main_layout.addWidget(paths_group)

            # Устанавливаем содержимое в scroll area
            content_wrapper = QWidget()
            content_wrapper.setLayout(main_layout)
            self.scroll_content_layout.addWidget(content_wrapper)

        except Exception as e:
            _log("ERR", f"Progress update error: {str(e)}")
            error_label = QLabel(f"Ошибка отображения: {str(e)}")
            error_label.setStyleSheet("""
                QLabel {
                    color: #F44336;
                    font: 10pt 'Segoe UI';
                    padding: 20px;
                }
            """)
            self.scroll_content_layout.addWidget(error_label)
        
    def _get_progress_text(self, progress, status):
        """Возвращает текст для отображения в прогресс-баре"""
        if status == 'done':
            return "✓ Готово"
        elif status == 'error':
            return "✗ Ошибка"
        elif status == 'encoding' or status == 'copying':
            return f"{progress}%"
        return "Ожидание"
            
    def cleanup_progress_bars(self):
        """Очищает ссылки на удаленные прогресс-бары"""
        if hasattr(self, '_progress_bar_refs'):
            self._progress_bar_refs = [bar for bar in self._progress_bar_refs if not sip.isdeleted(bar)]

    def _get_progress_style(self, status):
        """Возвращает стиль для прогресс-бара"""
        colors = {
            'done': ("#4CAF50", "#388E3C"),
            'encoding': ("#2196F3", "#1976D2"),
            'error': ("#F44336", "#D32F2F"),
            'stopped': ("#FF9800", "#F57C00"),
            'waiting': ("#BDBDBD", "#757575"),
            'copying': ("#2196F3", "#1976D2"),  # Синий для копирования
        }.get(status, ("#BDBDBD", "#757575"))
        
        return f"""
            QProgressBar {{
                border: 1px solid {colors[1]};
                border-radius: 3px;
                text-align: center;
            }}
            QProgressBar::chunk {{
                background-color: {colors[0]};
            }}
        """

    def _get_status_text(self, status):
        """Возвращает текст статуса с иконками"""
        return {
            'done': "✓ Готово",
            'encoding': "▶ В процессе",
            'copying': "▶ Копирование",
            'error': "✗ Ошибка",
            'stopped': "⏹ Остановлено",
            'waiting': "⏸ Ожидание"
        }.get(status, "? Неизвестно")

    def _get_status_color(self, status):
        """Возвращает цвет статуса"""
        return {
            'done': "#388E3C",        # Зеленый
            'encoding': "#1976D2",    # Синий
            'error': "#D32F2F",       # Красный
            'stopped': "#F57C00",     # Оранжевый
            'waiting': "#757575",     # Серый
            'copying': "#1976D2",     # Синий для копирования
        }.get(status, "#757575")

    def clear_completed(self):
        """Удаляет только полностью завершенные задачи (все форматы в статусе 'done') и завершенные копирования"""
        _log("INFO", "Очистка завершенных задач и копирований...")
        
        tasks_to_remove = []
        
        # Сначала собираем ID задач, которые нужно удалить
        for task_id, task in list(self.tasks.items()):
            task_type = task.get('type', '')
            
            # --- Задачи копирования TVHub ---
            if task_type == 'tvhub_copy':
                # Удаляем завершенные копирования
                if task.get('status') == 'done':
                    tasks_to_remove.append(task_id)
                continue
                
            # Пропускаем задачи, которые копируются
            if task.get('status') == 'copying':
                continue
                
            # Пропускаем задачи, которые кодируются
            if task.get('status') == 'encoding':
                continue
                
            # Проверяем, есть ли форматы в задаче
            formats = task.get('formats', {})
            if not formats:
                continue
                
            # Проверяем, ВСЕ ли форматы завершены (статус 'done')
            all_formats_done = True
            for fmt_name, fmt_data in formats.items():
                if fmt_data.get('status') != 'done':
                    all_formats_done = False
                    break
            
            # Если все форматы завершены, добавляем задачу в список на удаление
            if all_formats_done:
                tasks_to_remove.append(task_id)
        
        # Теперь удаляем задачи из UI и структур данных
        removed_count = 0
        for task_id in tasks_to_remove:
            if task_id not in self.tasks:
                continue
                
            # Находим и удаляем элемент из списка задач
            for i in range(self.task_list.count() - 1, -1, -1):
                item = self.task_list.item(i)
                if item and item.data(Qt.UserRole) == task_id:
                    self.task_list.takeItem(i)
                    break
            
            # Удаляем прогресс-бары
            if task_id in self.progress_bars:
                for format_name, bar in self.progress_bars[task_id].items():
                    try:
                        bar.setParent(None)
                        bar.deleteLater()
                    except:
                        pass
                del self.progress_bars[task_id]
            
            # Удаляем из задач
            del self.tasks[task_id]
            
            # Удаляем из логгера
            if hasattr(self, 'logger') and task_id in self.logger.tasks:
                del self.logger.tasks[task_id]
            
            removed_count += 1
            _log("INFO", f"Удалена задача {task_id}")
        
        # Обновляем UI
        if removed_count > 0:
            self.update_visible_progress()
            self.update_pagination()
            self.update_global_controls()
            self.logger.render()  # Обновляем иерархический лог
            
            _log("OK", f"Удалено {removed_count} завершенных задач")
            self.append_to_log(f"Очищено: {removed_count} завершенных задач")
        else:
            _log("INFO", "Нет завершенных задач для удаления")
            self.append_to_log("Нет завершенных задач для удаления")

    def browse_file(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Select Video Files", "", "Video Files (*.mp4 *.avi *.mkv *.mov)")
        if files:
            self.add_files_to_queue(files)
    
    def update_task_item(self, item):
        """Безопасное обновление элемента списка задач"""
        if not item:  # Проверка на существование элемента
            return

        try:
            task_id = item.data(Qt.UserRole)
            if not task_id:  # Проверка ID задачи
                return

            task = self.tasks.get(task_id)
            if not task:  # Проверка существования задачи
                return

            # Базовые данные с защитой по умолчанию
            file_path = task.get('file', 'Файл не указан')
            base_name = os.path.basename(str(file_path))
            status = task.get('status', 'unknown')
            file_type = task.get('type', '')

            # Инициализация текста и цвета
            text = f"� {base_name} (Неизвестный статус)"
            color = QColor(240, 240, 240)  # Серый по умолчанию

            # Обработка статуса копирования (без formats)
            if status == 'copying':
                progress = task.get('progress', 0)
                text = f"📁 {base_name} (Копирование)"
                color = QColor(255, 243, 224)  # Оранжевый
                
            # Обработка остальных статусов (с проверкой formats)
            elif 'formats' in task:  # Ключевая проверка!
                formats = task['formats']
                
                if status == 'encoding':
                    active_fmts = [fmt for fmt, data in formats.items() 
                                  if isinstance(data, dict) and data.get('status') == 'encoding']
                    fmt_text = ', '.join(active_fmts) if active_fmts else 'Кодирование'
                    text = f"▶ {base_name} ({fmt_text})"
                    color = QColor(227, 242, 253)  # Голубой
                    
                elif status == 'done':
                    text = f"✓ {base_name} (Готово)"
                    color = QColor(232, 245, 233)  # Зеленый
                    
                elif status == 'error':
                    err_msg = next((data.get('message', 'Ошибка') 
                                 for data in formats.values() 
                                 if isinstance(data, dict) and data.get('status') == 'error'), 'Ошибка')
                    text = f"✗ {base_name} ({err_msg})"
                    color = QColor(255, 235, 238)  # Красный
                    
                elif status == 'stopped':
                    text = f"⏹ {base_name} (Остановлено)"
                    color = QColor(255, 224, 178)  # Оранжевый
                    
                else:  # waiting
                    type_suffix = {
                        'tvhub': ' (TVHub)',
                        'anidub': ' (AniDub)'
                    }.get(file_type, ' (Ожидание)')
                    text = f"⏸ {base_name}{type_suffix}"
                    color = QColor(255, 248, 225)  # Желтый
                    
            # Установка значений с обработкой ошибок
            try:
                item.setText(str(text))
                item.setBackground(color)
                item.setData(Qt.UserRole + 1, status)
                item.setToolTip(f"Файл: {file_path}\nСтатус: {status}")
                item.setFlags(item.flags() | Qt.ItemIsEnabled | Qt.ItemIsSelectable)
            except Exception as ui_err:
                _log("ERR", f"UI update error: {ui_err}")

        except Exception as e:
            _log("ERR", f"Ошибка обновления элемента: {e}")
            item.setText("! Ошибка отображения !")
            item.setBackground(QColor(255, 200, 200))
        
    def _get_display_name_for_tvhub(self, filename):
        """Форматирует имя файла для TVHub задач"""
        # Удаляем лишние части (PROPER, TVHUB и т.д.)
        parts = filename.split('.')
        filtered = []
        skip = False
        for part in parts:
            if part in ['PROPER', 'TVHUB', 'FILM', 'WEB']:
                skip = True
            elif part in ['400p', '720p', '1080p']:
                break
            elif not skip:
                filtered.append(part)
            else:
                skip = False
        return '.'.join(filtered[:5])  # Берем первые 5 значимых частей

    def _get_display_name_for_anidub(self, filename):
        """Форматирует имя файла для AniDub задач"""
        # Удаляем расширения и техническую информацию
        if filename.lower().endswith('.mkv'):
            filename = filename[:-4]
        return filename.replace('_', '.').replace('..', '.')
        
    def _create_progress_bar(self):
        """Создает прогресс-бар с фиксированными параметрами"""
        pb = QProgressBar()
        pb.setRange(0, 100)
        pb.setValue(0)
        pb.setFixedHeight(24)
        pb.setAlignment(Qt.AlignCenter)
        pb.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        pb.setStyleSheet("""
            QProgressBar {
                border: 1px solid #ccc;
                border-radius: 3px;
                text-align: center;
                font: 9pt 'Segoe UI';
            }
            QProgressBar::chunk {
                width: 1px;
            }
        """)
        pb.setFormat("Ожидание")
        return pb
        
    def _show_no_task_message(self):
        """Показывает сообщение при отсутствии выбранной задачи"""
        no_task_label = QLabel("Выберите задачу для просмотра прогресса")
        no_task_label.setAlignment(Qt.AlignCenter)
        no_task_label.setStyleSheet("""
            QLabel {
                color: #666;
                font: 10pt 'Segoe UI';
                padding: 20px;
            }
        """)
        self.scroll_content_layout.addWidget(no_task_label)
        
    def _update_progress_text(self, progress_bar, percent, status, eta=None):
        """Обновляет только текст прогресс-бара без изменения значения"""
        if sip.isdeleted(progress_bar):
            return
            
        if status == 'done':
            text = "✓ Готово"
        elif status == 'error':
            text = "✗ Ошибка"
        elif eta:
            text = f"{percent}% • ETA: {eta}"
        else:
            text = f"{percent}%"
        
        progress_bar.setFormat(text)
        # Принудительное обновление без изменения значения
        progress_bar.repaint()
    
    def show_context_menu(self, position):
        """
        Контекстное меню очереди в едином стиле:
        - Заголовок-файл (некликабельный)
        - Управление по форматам в порядке: XviD, 400p, 720p, x265
          • если формат есть: ▶ Запустить / ⏹ Остановить
          • если формата нет (для x265): ▶ Добавить и запустить
        - Разделитель
        - 🗑 Удалить задачу
        """
        item = self.task_list.itemAt(position)
        if not item:
            return

        task_id = item.data(Qt.UserRole)
        task = self.tasks.get(task_id)
        if not task:
            return

        menu = QMenu(self)

        # Заголовок — единообразно
        info = menu.addAction(f"🗂 Файл: {os.path.basename(task['file'])}")
        info.setEnabled(False)

        menu.addSeparator()

        # Единая фабрика подписей
        def fmt_title(fmt: str) -> str:
            return {
                'xvid': 'XviD (AVI 720×400)',
                '400p': '400p (H.264 720×400)',
                '720p': '720p (H.264 1280×720)',
                'x265': 'x265 (HEVC, 1920×1080)'
            }.get(fmt, fmt.upper())

        def add_run_action(fmt: str):
            """Добавляет пункт для формата в едином стиле."""
            present = fmt in task.get('formats', {})
            status = task['formats'][fmt].get('status') if present else None

            if present and status == 'encoding':
                act = menu.addAction(f"⏹ Остановить — {fmt_title(fmt)}")
                act.triggered.connect(lambda _=False, f=fmt: self.stop_format(task_id, f))
            elif present:
                act = menu.addAction(f"▶ Запустить — {fmt_title(fmt)}")
                act.triggered.connect(lambda _=False, f=fmt: self.start_format(task_id, f))
            else:
                # единый стиль: тоже ▶, чтобы дизайн не «плясал»
                act = menu.addAction(f"▶ Запустить — {fmt_title(fmt)}")
                act.triggered.connect(lambda _=False, f=fmt: self.add_format_and_start(task_id, f))

        # Порядок форматов фиксированный
        for f in ['xvid', '400p', '720p', 'x265']:
            # Для x265 — показываем всегда (добавится и запустится при клике)
            if f == 'x265':
                add_run_action('x265')
            else:
                # Для существующих форматов добавляем пункт, если формат присутствует в задаче
                if f in task.get('formats', {}):
                    add_run_action(f)

        menu.addSeparator()

        # Удаление — единый стиль
        remove_action = menu.addAction("🗑 Удалить задачу")
        remove_action.triggered.connect(lambda: self.remove_task(task_id))

        menu.exec_(self.task_list.viewport().mapToGlobal(position))

    def add_format_and_start(self, task_id: str, fmt: str):
        """
        Добавляет формат в задачу (если его ещё нет) и сразу запускает кодирование.
        Используется для x265 из контекстного меню, но подходит и для других форматов.
        """
        if fmt not in {"xvid", "400p", "720p", "x265"}:
            return

        task = self.tasks.get(task_id)
        if not task:
            return

        # Гарантируем структуру форматов
        task.setdefault('formats', {})

        # Если формата нет — создаём запись и прогресс-бар
        if fmt not in task['formats']:
            task['formats'][fmt] = {
                'status': 'waiting',
                'progress': 0,
                'pid': None
            }
            self.progress_bars.setdefault(task_id, {})
            self.progress_bars[task_id][fmt] = self._create_progress_bar()

        # Разрешаем перезапуск завершённых/ошибочных
        if task['formats'][fmt].get('status') in ('done', 'error', 'stopped'):
            task['formats'][fmt]['status'] = 'waiting'
            task['formats'][fmt]['progress'] = 0

        # Проверяем лимит одновременных задач
        if self.active_tasks_count >= self.max_concurrent_tasks:
            # Добавляем в очередь ожидания
            if (task_id, fmt) not in self.pending_tasks:
                self.pending_tasks.append((task_id, fmt))
                self.append_to_log(f"Задача {task_id} ({fmt}) добавлена в очередь ожидания")
            return

        # Запускаем кодирование
        self.start_format(task_id, fmt)

        # Обновляем UI
        self.update_visible_progress()
        if hasattr(self, '_update_task_item_in_list'):
            self._update_task_item_in_list(task_id)
        
    def start_format(self, task_id, format_name):
        """Запускает конкретный формат (из контекстного меню)."""
        try:
            # 1. Проверка существования задачи
            if task_id not in self.tasks:
                self.append_to_log(f"Ошибка: задача {task_id} не найдена")
                return False

            task = self.tasks[task_id]
            
            # 2. Если формат отсутствует в задаче - добавляем
            if format_name not in task['formats']:
                task['formats'][format_name] = {
                    'status': 'waiting',
                    'progress': 0,
                    'pid': None,
                    'output_path': None
                }

            fmt_data = task['formats'][format_name]

            # 3. Проверяем лимит одновременных задач
            if self.active_tasks_count >= self.max_concurrent_tasks:
                # Добавляем в очередь ожидания вместо немедленного запуска
                if (task_id, format_name) not in self.pending_tasks:
                    self.pending_tasks.append((task_id, format_name))
                    self.append_to_log(f"Задача {task_id} ({format_name}) добавлена в очередь ожидания")
                
                # Обновляем статус
                fmt_data['status'] = 'waiting'
                self._update_task_item_in_list(task_id)
                return False

            # 4. Гарантируем наличие прогресс-бара
            self.ensure_progress_bar(task_id, format_name)
            
            # 5. Если есть старый поток - останавливаем
            if (task_id, format_name) in self.threads:
                old_thread = self.threads[(task_id, format_name)]
                if old_thread.isRunning():
                    self.append_to_log(f"Остановка предыдущего потока для {format_name}...")
                    old_thread.safe_quit()
                    old_thread.wait(2000)
                    if old_thread.isRunning():
                        old_thread.terminate()
                del self.threads[(task_id, format_name)]

            # 6. Проверка состояния
            if fmt_data['status'] == 'encoding':
                self.append_to_log(f"Кодирование {format_name} уже запущено")
                return False

            # 7. Проверка файла
            if not os.path.exists(task['file']):
                error_msg = f"Файл не найден: {task['file']}"
                self.append_to_log(error_msg)
                fmt_data['status'] = 'error'
                fmt_data['message'] = error_msg
                self._update_task_item_in_list(task_id)
                return False

            # 8. Подготовка прогресс-бара
            bar = self.progress_bars[task_id][format_name]
            bar.setValue(0)
            bar.setFormat("0%")
            bar.setStyleSheet(self._get_progress_style('encoding'))

            # 9. Подготовка статусов
            fmt_data.update({
                'status': 'encoding',
                'progress': 0,
                'pid': None,
                'message': None
            })
            task['status'] = 'encoding'

            # 10. Увеличиваем счетчик активных задач
            self.active_tasks_count += 1

            # 11. Создание и запуск потока
            thread = EncodingThread(task_id, task['file'], format_name)
            thread.update_signal.connect(self.update_status, Qt.QueuedConnection)
            thread.progress_signal.connect(self.update_progress, Qt.QueuedConnection)
            thread.finished_signal.connect(
                lambda *args: self.encoding_finished(*args), 
                Qt.QueuedConnection
            )

            self.threads[(task_id, format_name)] = thread
            thread.start()

            self.append_to_log(f"Запущено кодирование {format_name} для {os.path.basename(task['file'])}")
            return True

        except Exception as e:
            error_msg = f"Ошибка запуска формата {format_name}: {str(e)}"
            self.append_to_log(error_msg)
            if task_id in self.tasks and format_name in self.tasks[task_id]['formats']:
                self.tasks[task_id]['formats'][format_name]['status'] = 'error'
                self.tasks[task_id]['formats'][format_name]['message'] = error_msg
            return False
        finally:
            self._update_task_item_in_list(task_id)
            self.update_visible_progress()
            self.update_global_controls()
            
    def _get_active_threads_count(self):
        """Возвращает количество реально запущенных потоков кодирования"""
        active_count = 0
        for key, thread in self.threads.items():
            try:
                if thread.isRunning():
                    active_count += 1
            except:
                pass
        return active_count

    def stop_format(self, task_id, format_name):
        """Безопасная остановка конкретного формата"""
        # 1) Удаляем из очереди ожидания (если есть)
        if (task_id, format_name) in self.pending_tasks:
            self.pending_tasks.remove((task_id, format_name))
            _log("WARN", f"Формат {format_name} удален из очереди ожидания")

        # Удаляем ETA для остановленной задачи
        if task_id in self.eta and format_name in self.eta[task_id]:
            del self.eta[task_id][format_name]
            if not self.eta[task_id]:
                del self.eta[task_id]
        
        if task_id in self.eta_sec and format_name in self.eta_sec[task_id]:
            del self.eta_sec[task_id][format_name]
            if not self.eta_sec[task_id]:
                del self.eta_sec[task_id]
        
        # Обновляем метку с максимальным ETA
        self.update_max_eta_label()
        
        # 2) Проверяем существование потока
        if (task_id, format_name) not in self.threads:
            # Если нет потока, просто обновляем статус
            if task_id in self.tasks and format_name in self.tasks[task_id]['formats']:
                self.tasks[task_id]['formats'][format_name]['status'] = 'stopped'
                self._update_task_item_in_list(task_id)
            return
        
        # 3) Получаем поток
        thread = self.threads[(task_id, format_name)]
        
        # 4) Останавливаем поток
        try:
            if thread.isRunning():
                _log("INFO", f"Останавливаем формат {format_name}...")
                thread.safe_quit()  # Используем наш новый метод
                
                # Ждем завершения
                if not thread.wait(3000):
                    thread.terminate()
                    thread.wait(1000)
        except Exception as e:
            _log("ERR", f"Ошибка при остановке потока: {str(e)}")
        
        # 5) Уменьшаем счетчик активных задач
        self.active_tasks_count = max(0, self.active_tasks_count - 1)
        
        # 6) Обновляем статус
        if task_id in self.tasks and format_name in self.tasks[task_id]['formats']:
            self.tasks[task_id]['formats'][format_name]['status'] = 'stopped'
            self.tasks[task_id]['formats'][format_name]['progress'] = 0
            
            # Обновляем прогресс-бар
            if task_id in self.progress_bars and format_name in self.progress_bars[task_id]:
                bar = self.progress_bars[task_id][format_name]
                try:
                    if not sip.isdeleted(bar):
                        bar.setValue(0)
                        bar.setFormat("⏹ Остановлено")
                        bar.setStyleSheet(self._get_progress_style('stopped'))
                except:
                    pass
        
        # 7) Удаляем поток из словаря
        if (task_id, format_name) in self.threads:
            thr = self.threads[(task_id, format_name)]
            try:
                # Разрываем соединения
                thr.update_signal.disconnect()
                thr.progress_signal.disconnect()
                thr.finished_signal.disconnect()
            except:
                pass
            thr.deleteLater()
            del self.threads[(task_id, format_name)]
        
        # 8) Обновляем общий статус задачи
        self._update_overall_task_status(task_id)
        
        # 9) Обновляем UI
        self._update_task_item_in_list(task_id)
        self.update_visible_progress()
        self.update_global_controls()
        
        # 10) Проверяем, остались ли активные задачи и задачи в очереди
        if not self.check_active_encoding_tasks():
            self.stop_total_encoding_timer()
        
        # 11) Запускаем следующую задачу из очереди
        QTimer.singleShot(100, self._start_next_task)
        
        _log("OK", f"Формат {format_name} остановлен")
    
    def ensure_progress_bar(self, task_id, format_name):
        """
        Гарантирует существование progress bar для (task_id, format_name) и возвращает его (не None).
        Если ранее бар был удалён из Qt (sip.isdeleted == True), создаём новый.
        """
        if not hasattr(self, 'progress_bars'):
            self.progress_bars = {}

        task_bars = self.progress_bars.setdefault(task_id, {})
        bar = task_bars.get(format_name)

        # бар может быть "зомби": объект удалён в Qt, но ссылка осталась
        try:
            import sip
            is_deleted = (bar is not None and hasattr(sip, "isdeleted") and sip.isdeleted(bar))
        except Exception:
            is_deleted = False

        if bar is None or is_deleted:
            bar = QProgressBar()
            bar.setRange(0, 100)
            bar.setValue(0)
            bar.setFormat("0%")
            bar.setTextVisible(True)
            task_bars[format_name] = bar

            try:
                if hasattr(self, '_attach_progress_bar_to_ui'):
                    self._attach_progress_bar_to_ui(task_id, format_name, bar)
            except Exception as e:
                self.append_to_log(f"Не удалось добавить прогресс-бар в UI: {e}")

        return bar
    
    def start_specific_task(self, task_id):
        """Запускает кодирование только для выбранных и активных форматов"""
        if task_id not in self.tasks:
            return

        task = self.tasks[task_id]
        if task.get('status') == 'encoding':
            return

        # Определяем какие форматы нужно запускать
        formats_to_start = []
        if self.xvid_check.isChecked() and 'xvid' in task['formats']:
            formats_to_start.append('xvid')
        if self.p400_check.isChecked() and '400p' in task['formats']:
            formats_to_start.append('400p')
        if self.p720_check.isChecked() and '720p' in task['formats']:
            formats_to_start.append('720p')
        if self.x265_check.isChecked() and 'x265' in task['formats']:
            formats_to_start.append('x265')

        if not formats_to_start:
            return

        task['status'] = 'encoding'
        self._update_task_item_in_list(task_id)

        # Запускаем потоки только для нужных форматов
        for format_name in formats_to_start:
            fmt_data = task['formats'][format_name]
            
            # Проверяем состояние формата
            if fmt_data['status'] in ('waiting', 'stopped', 'error'):
                # Убедимся что прогресс-бар существует
                if task_id not in self.progress_bars or format_name not in self.progress_bars[task_id]:
                    if task_id not in self.progress_bars:
                        self.progress_bars[task_id] = {}
                    self.progress_bars[task_id][format_name] = self._create_progress_bar()
                
                # Сбрасываем состояние если была ошибка/остановка
                if fmt_data['status'] in ('error', 'stopped'):
                    fmt_data.update({
                        'status': 'waiting',
                        'progress': 0,
                        'pid': None
                    })

                # Запускаем кодирование
                self.start_encoding_thread(task_id, format_name)

        self.update_visible_progress()
        self.update_global_controls()
    
    def stop_specific_task(self, task_id):
        """Останавливает все форматы в конкретной задаче"""
        if task_id not in self.tasks:
            return
            
        task = self.tasks[task_id]
        if task.get('status') != 'encoding':
            return
            
        _log("INFO", f"Останавливаем задачу {task_id}...")
        
        # Останавливаем все форматы задачи
        for format_name in list(task.get('formats', {}).keys()):
            self.stop_format(task_id, format_name)
        
        # Обновляем статус задачи
        task['status'] = 'stopped'
        self._update_task_item_in_list(task_id)
    
    def remove_task(self, task_id):
        """Удаляет задачу и связанные ресурсы с проверками + чистит запись в PipelineLogger."""
        # 0) Удаляем все форматы этой задачи из очереди ожидания
        self.pending_tasks = [(tid, fmt) for (tid, fmt) in self.pending_tasks if tid != task_id]

        # 1) Проверяем существование задачи
        if task_id not in self.tasks:
            return

        # 2) Остановить задачу, если она выполняется
        try:
            if self.tasks[task_id].get('status') == 'encoding':
                self.stop_specific_task(task_id)
        except Exception:
            pass

        # 3) Удалить из списка задач (UI)
        try:
            for i in range(self.task_list.count()):
                item = self.task_list.item(i)
                if item and item.data(Qt.UserRole) == task_id:
                    self.task_list.takeItem(i)
                    break
        except Exception:
            pass

        # 4) Очистить потоки (если ещё есть)
        try:
            for format_name in ['xvid', '400p', '720p', 'x265', 'anidub', 'tvhub']:
                key = (task_id, format_name)
                if key in self.threads:
                    thr = self.threads[key]
                    try:
                        if thr.isRunning():
                            thr.safe_quit()
                            thr.wait(1000)
                            if thr.isRunning():
                                thr.terminate()
                                thr.wait(500)
                    except Exception:
                        pass
                    finally:
                        # Разрываем соединения
                        try:
                            thr.update_signal.disconnect()
                            thr.progress_signal.disconnect()
                            thr.finished_signal.disconnect()
                        except:
                            pass
                        thr.deleteLater()
                        self.threads.pop(key, None)
        except Exception:
            pass

        # 5) Безопасно удалить прогресс-бары
        try:
            if task_id in self.progress_bars:
                for format_name, bar in list(self.progress_bars[task_id].items()):
                    # 'status_label' может быть QLabel
                    if format_name == 'status_label':
                        try:
                            if bar and bar.parent():
                                bar.setParent(None)
                                bar.deleteLater()
                        except RuntimeError:
                            pass
                    else:
                        try:
                            if bar and hasattr(bar, 'setParent'):
                                bar.setParent(None)
                            if hasattr(bar, 'deleteLater'):
                                bar.deleteLater()
                        except RuntimeError:
                            pass
                self.progress_bars.pop(task_id, None)
        except Exception:
            pass

        # 6) Удалить из словаря задач
        try:
            self.tasks.pop(task_id, None)
        except Exception:
            pass

        # 7) Почистить запись в иерархическом логере и перерисовать лог
        try:
            if hasattr(self, "logger") and task_id in self.logger.tasks:
                del self.logger.tasks[task_id]
                self.logger.render()
        except Exception:
            pass

        # 8) Удалить ETA для этой задачи
        self.eta.pop(task_id, None)
        self.eta_sec.pop(task_id, None)
        self.update_max_eta_label()

        # 9) Обновить UI
        try:
            self.update_global_controls()
            self.update_visible_progress()
            self.update_pagination()
        except Exception:
            pass
        
        _log("OK", f"Задача {task_id} удалена из очереди")
    
    def start_all_tasks(self):
        """Запускает ВСЕ ожидающие, остановленные и ошибочные задачи"""
        if not self.tasks:
            self.append_to_log("Нет задач в очереди")
            return
        
        _log("INFO", "Запускаем ВСЕ задачи (включая ошибки)...")
        
        # Получаем список активных форматов
        active_formats = []
        if self.xvid_check.isChecked(): active_formats.append('xvid')
        if self.p400_check.isChecked(): active_formats.append('400p')
        if self.p720_check.isChecked(): active_formats.append('720p')
        if self.x265_check.isChecked(): active_formats.append('x265')
        
        if not active_formats:
            self.append_to_log("Не выбрано ни одного формата для кодирования")
            return

        # Очищаем предыдущую очередь
        self.pending_tasks.clear()
        
        # Считаем, сколько задач добавили
        added_tasks = 0
        added_formats = 0
        
        # Проходим по всем задачам в правильном порядке (сначала старые)
        for i in range(self.task_list.count()):
            item = self.task_list.item(i)
            if not item:
                continue
                
            task_id = item.data(Qt.UserRole)
            if task_id not in self.tasks:
                continue
                
            task = self.tasks[task_id]
            
            # Запускаем задачи, которые ожидают, остановлены ИЛИ с ошибками
            if task['status'] in ('waiting', 'stopped', 'error'):
                added_tasks += 1
                
                # Добавляем отсутствующие форматы, если они активированы
                for fmt in active_formats:
                    if fmt not in task['formats']:
                        task['formats'][fmt] = {
                            'status': 'waiting',
                            'progress': 0,
                            'pid': None,
                            'message': None
                        }
                        # Создаем прогресс-бар для нового формата
                        self.ensure_progress_bar(task_id, fmt)
                
                # Добавляем все активированные форматы в очередь
                for fmt in active_formats:
                    if fmt in task['formats']:
                        fmt_data = task['formats'][fmt]
                        
                        # Если формат был остановлен, ожидает или в ошибке, сбрасываем статус и добавляем в очередь
                        if fmt_data.get('status') in ('waiting', 'stopped', 'error'):
                            # Полностью сбрасываем статус, прогресс и сообщение об ошибке
                            fmt_data.update({
                                'status': 'waiting',
                                'progress': 0,
                                'pid': None,
                                'message': None,
                                'output_path': None
                            })
                            
                            # Сбрасываем прогресс-бар
                            if task_id in self.progress_bars and fmt in self.progress_bars[task_id]:
                                bar = self.progress_bars[task_id][fmt]
                                try:
                                    bar.setValue(0)
                                    bar.setFormat("Ожидание")
                                    bar.setStyleSheet(self._get_progress_style('waiting'))
                                except:
                                    pass
                            
                            # Добавляем в очередь
                            self.pending_tasks.append((task_id, fmt))
                            added_formats += 1
                            
                            _log("INFO", f"Добавлен формат {fmt} для {os.path.basename(task['file'])} "
                                  f"(был: {fmt_data.get('status', 'unknown')})")
        
        if added_formats == 0:
            self.append_to_log("Нет задач для запуска")
            _log("INFO", "Нет задач для запуска")
            return
        
        _log("OK", f"Добавлено {added_formats} форматов из {added_tasks} задач в очередь")
        self.append_to_log(f"Добавлено в очередь: {added_formats} форматов из {added_tasks} задач")
        
        # Обновляем статусы задач на 'waiting'
        for task_id in self.tasks:
            task = self.tasks[task_id]
            if task['status'] in ('stopped', 'error'):
                task['status'] = 'waiting'
        
        # Обновляем UI перед запуском
        for i in range(self.task_list.count()):
            item = self.task_list.item(i)
            if item:
                task_id = item.data(Qt.UserRole)
                self._update_task_item_in_list(task_id)
        
        # Сразу запускаем первые задачи из очереди
        self._start_next_task()
        
        # Обновляем GUI
        self.update_global_controls()
        self.update_visible_progress()
        QApplication.processEvents()
        
    def print_task_statuses(self):
        """Выводит статусы всех задач для отладки"""
        _log("INFO", "--- СТАТУСЫ ВСЕХ ЗАДАЧ ---")
        for i, (task_id, task) in enumerate(self.tasks.items(), 1):
            filename = os.path.basename(task.get('file', 'unknown'))
            status = task.get('status', 'unknown')
            formats = task.get('formats', {})
            _log("INFO", f"{i}. {filename} [{status}]")
            for fmt, data in formats.items():
                fmt_status = data.get('status', 'unknown')
                progress = data.get('progress', 0)
                has_error = 'message' in data and data['message']
                err = " (err)" if has_error else ""
                _log("INFO", f"   - {fmt}: {fmt_status} ({progress}%){err}")
    
    def stop_all_tasks(self):
        """Остановка ВСЕХ задач - текущих и в очереди"""
        _log("INFO", "Останавливаем ВСЕ задачи...")
        
        # Очищаем все ETA
        self.eta.clear()
        self.eta_sec.clear()
        
        # Обновляем метку с максимальным ETA
        self.update_max_eta_label()
        
        # Устанавливаем флаг остановки
        self.is_stopping_all = True
        
        # 1. Останавливаем ВСЕ потоки кодирования
        stopped_threads = 0
        for (task_id, format_name), thread in list(self.threads.items()):
            try:
                if thread.isRunning():
                    _log("INFO", f"Останавливаем поток {task_id}/{format_name}")
                    thread.safe_quit()
                    stopped_threads += 1
                    
                    # Ждем завершения до 3 секунд
                    if not thread.wait(3000):
                        # Если не завершился - принудительно
                        thread.terminate()
                        thread.wait(1000)
                    
                    # Обновляем статус в задаче
                    if task_id in self.tasks and format_name in self.tasks[task_id]['formats']:
                        self.tasks[task_id]['formats'][format_name]['status'] = 'stopped'
                        self.tasks[task_id]['formats'][format_name]['progress'] = 0
                        
                        # Обновляем прогресс-бар
                        if task_id in self.progress_bars and format_name in self.progress_bars[task_id]:
                            bar = self.progress_bars[task_id][format_name]
                            try:
                                if not sip.isdeleted(bar):
                                    bar.setValue(0)
                                    bar.setFormat("⏹ Остановлено")
                                    bar.setStyleSheet(self._get_progress_style('stopped'))
                            except:
                                pass
                                
            except Exception as e:
                _log("ERR", f"Ошибка остановки потока {task_id}/{format_name}: {str(e)}")
        
        # 2. Очищаем очередь ожидания ПОЛНОСТЬЮ
        pending_count = len(self.pending_tasks)
        self.pending_tasks.clear()
        
        # 3. Сбрасываем счетчик активных задач
        self.active_tasks_count = 0
        
        # 4. Очищаем словарь потоков
        self.threads.clear()
        
        # 5. Обновляем статусы всех задач
        for task_id, task in self.tasks.items():
            if task['status'] in ('encoding', 'waiting'):
                task['status'] = 'stopped'
                for fmt_data in task['formats'].values():
                    if fmt_data.get('status') in ('encoding', 'waiting'):
                        fmt_data['status'] = 'stopped'
                        fmt_data['progress'] = 0
                        fmt_data['message'] = None
        
        # 6. Обновляем UI
        for i in range(self.task_list.count()):
            item = self.task_list.item(i)
            if item:
                task_id = item.data(Qt.UserRole)
                self._update_task_item_in_list(task_id)
        
        # 7. Обновляем отображение
        self.update_visible_progress()
        self.update_global_controls()
        QApplication.processEvents()
        
        _log("OK", f"Остановлено: {stopped_threads} потоков, очередь: {pending_count} задач")
        self.append_to_log(f"Все задачи остановлены. Остановлено: {stopped_threads} потоков, очищена очередь: {pending_count} задач")
        
        # 8. Дополнительно: добавляем флаг, что остановка в процессе
        self.global_stop_btn.setEnabled(False)
        self.global_start_btn.setEnabled(True)
        
        # 9. Останавливаем таймер общего времени кодирования (если он существует)
        if hasattr(self, 'total_encoding_timer') and self.total_encoding_timer.isActive():
            self.stop_total_encoding_timer()
        
        # 10. Снимаем флаг остановки
        self.is_stopping_all = False
        
    def stop_all_timers(self):
        """Останавливает все таймеры в приложении"""
        # Останавливаем таймер общего времени
        if hasattr(self, 'total_encoding_timer'):
            self.total_encoding_timer.stop()
            self.total_encoding_start_time = None
        
        # Останавливаем таймер очистки памяти
        if hasattr(self, 'memory_cleanup_timer'):
            self.memory_cleanup_timer.stop()
        
        # Останавливаем таймер ресурсов
        if hasattr(self, 'resource_timer'):
            self.resource_timer.stop()
        
        # Останавливаем другие таймеры
        if hasattr(self, 'other_timers'):
            for timer in self.other_timers:
                timer.stop()
        
    def _update_task_ui(self, task_id):
        """Обновляет элемент задачи в списке"""
        if task_id not in self.tasks:
            return
        
        # Находим соответствующий элемент в списке
        for i in range(self.task_list.count()):
            item = self.task_list.item(i)
            if item.data(Qt.UserRole) == task_id:
                self.update_task_item(item)
                break
        
        # Обновляем отображение прогресса, если задача выбрана
        selected_items = self.task_list.selectedItems()
        if selected_items and selected_items[0].data(Qt.UserRole) == task_id:
            self.update_visible_progress()
    
    def get_output_base_dir(self):
        """Базовая папка для сохранения (выбранная пользователем или по умолчанию для macOS)."""
        return self.output_base_dir if self.output_base_dir else DEFAULT_OUTPUT_BASE_MAC

    def start_encoding_thread(self, task_id, format_name):
        """Запуск потока кодирования с ограничением на одновременное выполнение"""
        try:
            # 1) Проверка активированности формата (кроме спец-форматов)
            special_formats = {'anidub', 'tvhub'}
            if format_name not in special_formats:
                format_check_map = {
                    'xvid': self.xvid_check,
                    '400p': self.p400_check,
                    '720p': self.p720_check,
                    'x265': self.x265_check,
                }
                if format_name not in format_check_map or not format_check_map[format_name].isChecked():
                    return False

            # 2) Проверяем лимит одновременных задач
            if self.active_tasks_count >= self.max_concurrent_tasks:
                # Добавляем в очередь ожидания
                if (task_id, format_name) not in self.pending_tasks:
                    self.pending_tasks.append((task_id, format_name))
                return False

            # 3) Валидация задачи и файла
            if task_id not in self.tasks:
                return False
            task = self.tasks[task_id]

            import os
            if not os.path.exists(task['file']):
                msg = f"Файл не найден: {task['file']}"
                task.setdefault('formats', {}).setdefault(format_name, {})['status'] = 'error'
                self._update_task_item_in_list(task_id)
                return False

            # 4) Увеличиваем счетчик активных задач
            self.active_tasks_count += 1

            # 5) Инициализация структуры форматов
            fmts = task.setdefault('formats', {})
            fmts.setdefault(format_name, {
                'status': 'waiting', 'progress': 0, 'pid': None, 'output_path': None, 'message': None
            })

            # 6) Остановка предыдущего потока для этого формата (если есть)
            key = (task_id, format_name)
            if key in self.threads:
                thr = self.threads[key]
                if thr.isRunning():
                    try:
                        thr.safe_quit()
                        thr.wait(2000)
                        if thr.isRunning():
                            thr.terminate()
                    except Exception:
                        pass
                del self.threads[key]

            # 7) Прогресс-бар: первичная инициализация
            bar = self.ensure_progress_bar(task_id, format_name)
            try:
                if bar:
                    bar.setValue(0)
                    bar.setFormat("0%")
                    bar.setStyleSheet(self._get_progress_style('encoding'))
            except Exception:
                pass

            # 8) Статусы
            fmts[format_name].update({'status': 'encoding', 'progress': 0, 'pid': None, 'message': None})
            task['status'] = 'encoding'

            # 9) Поднять задачу в самый верх очереди
            try:
                for i in range(self.task_list.count()):
                    it = self.task_list.item(i)
                    if it and it.data(Qt.UserRole) == task_id:
                        was_selected = (self.task_list.currentItem() is it)
                        it = self.task_list.takeItem(i)
                        self.task_list.insertItem(0, it)
                        if was_selected:
                            self.task_list.setCurrentItem(it)
                        break
            except Exception:
                pass

            # 10) Запускаем таймер общего времени кодирования, если это первый запуск
            if self.check_active_encoding_tasks() and not self.total_encoding_timer.isActive():
                self.start_total_encoding_timer()

            # 11) При первом запуске кодирования — выбор папки сохранения (macOS-ориентировано)
            if self.output_base_dir is None:
                chosen = QFileDialog.getExistingDirectory(
                    self,
                    "Выберите папку для сохранения",
                    DEFAULT_OUTPUT_BASE_MAC,
                    QFileDialog.ShowDirsOnly | QFileDialog.DontResolveSymlinks
                )
                if not chosen or not os.path.isdir(chosen):
                    self.active_tasks_count -= 1
                    fmts[format_name].update({'status': 'waiting', 'progress': 0})
                    task['status'] = 'waiting'
                    self._update_task_item_in_list(task_id)
                    self.update_visible_progress()
                    self.update_global_controls()
                    return False
                self.output_base_dir = chosen

            # 12) Старт рабочего потока
            thread = EncodingThread(
                task_id=task_id,
                input_file=task['file'],
                output_format=format_name,
                logger=getattr(self, 'logger', None),
                task_type=task.get('type', ''),
                output_base_dir=self.output_base_dir
            )
            
            # ДОБАВИМ ОТЛАДОЧНЫЙ ВЫВОД
            _log("INFO", f"start_encoding_thread: task_id={task_id}, task_type={task.get('type', '')}")
            thread.update_signal.connect(self.update_status, Qt.QueuedConnection)
            thread.progress_signal.connect(self.update_progress, Qt.QueuedConnection)
            thread.finished_signal.connect(lambda *args: self.encoding_finished(*args), Qt.QueuedConnection)

            self.threads[key] = thread
            thread.start()
            
            return True

        except Exception as e:
            # Уменьшаем счетчик при ошибке
            self.active_tasks_count = max(0, self.active_tasks_count - 1)
            if task_id in self.tasks:
                self.tasks[task_id].setdefault('formats', {}).setdefault(format_name, {})['status'] = 'error'
            return False
        finally:
            # аккуратное обновление UI
            self._update_task_item_in_list(task_id)
            self.update_visible_progress()
            self.update_global_controls()
            
    def _on_thread_finished(self, task_id, format_name):
        """Обработчик завершения потока"""
        if (task_id, format_name) in self.threads:
            thread = self.threads[(task_id, format_name)]
            thread.deleteLater()
            del self.threads[(task_id, format_name)]
            
    def _cleanup_zombie_processes(self):
        """Очистка зависших процессов ffmpeg"""
        try:
            if sys.platform == "win32":
                result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq ffmpeg.exe'],
                                      capture_output=True, text=True)
                if "ffmpeg.exe" in result.stdout:
                    subprocess.run(['taskkill', '/F', '/IM', 'ffmpeg.exe'],
                                  stdout=subprocess.DEVNULL,
                                  stderr=subprocess.DEVNULL)
        except Exception as e:
            self.append_to_log(f"Ошибка очистки процессов: {str(e)}")
              
    @pyqtSlot(str, str, str)
    def update_status(self, task_id, format_name, message):
        """
        Обрабатывает обновления статуса и ETA без мерцания текста.
        Ключевые статусы отправляем в PipelineLogger (вариант B).
        'Thread finished' подавляем (финал дорисует encoding_finished).
        + Поднятие задачи вверх при старте кодирования (как у TVHub).
        """
        try:
            # Подготавливаем доступ к sip для безопасной проверки удалённости виджета
            try:
                import sip  # локальный импорт, чтобы не падать, если sip не доступен
            except Exception:
                sip = None

            # --- 1) ETA: "ETA HH:MM:SS" ------------------------------------------------
            if isinstance(message, str) and message.startswith("ETA "):
                if not hasattr(self, 'eta'):
                    self.eta = {}
                if not hasattr(self, 'eta_sec'):
                    self.eta_sec = {}

                eta_str = message[4:].strip()
                
                # Инициализируем структуры для хранения ETA
                if task_id not in self.eta:
                    self.eta[task_id] = {}
                if task_id not in self.eta_sec:
                    self.eta_sec[task_id] = {}
                
                # Сохраняем ETA строкой
                self.eta[task_id][format_name] = eta_str

                # Сохраняем ETA в секундах для сравнения
                try:
                    h, m, s = map(int, eta_str.split(':'))
                    eta_seconds = h * 3600 + m * 60 + s
                    self.eta_sec[task_id][format_name] = eta_seconds
                except:
                    pass

                # Обновляем метку с максимальным ETA
                self.update_max_eta_label()

                # безопасное обновление текста на баре
                try:
                    bar = None
                    if (task_id in getattr(self, 'progress_bars', {}) and
                        format_name in self.progress_bars[task_id]):
                        bar = self.progress_bars[task_id][format_name]
                    if bar and not (sip and hasattr(sip, "isdeleted") and sip.isdeleted(bar)):
                        current_value = bar.value()
                        new_text = f"{current_value}% • ETA: {eta_str}"
                        if new_text != bar.format():
                            bar.setFormat(new_text)
                            bar.update()
                except Exception:
                    pass
                return  # ETA обработали и выходим

            # --- 2) Неи ETA: роутинг ключевых статусов в PipelineLogger --------------
            low = (message or "").lower()

            # Поглощаем "thread finished" чтобы не дублировать финал
            if "thread finished" in low:
                return

            # При старте потока/кодирования — лог + поднимаем задачу вверх (как у TVHub)
            if "starting thread" in low or "starting encoding process" in low:
                try:
                    self.logger.track(task_id, format_name, "поток запущен" if "starting thread" in low else "старт")
                except Exception:
                    pass

                # Поднять вверх (без отдельных хелперов, инлайн)
                try:
                    for i in range(self.task_list.count()):
                        it = self.task_list.item(i)
                        if it and it.data(Qt.UserRole) == task_id:
                            was_selected = (self.task_list.currentItem() is it)
                            it = self.task_list.takeItem(i)
                            self.task_list.insertItem(0, it)
                            if was_selected:
                                self.task_list.setCurrentItem(it)
                            break
                except Exception:
                    pass
                return

            if "duration detected" in low:
                try:
                    m = re.search(r'(\d{2}:\d{2}:\d{2}(?:\.\d{2})?)', message)
                    if m:
                        self.logger.duration_once(task_id, m.group(1))
                except Exception:
                    pass
                return

            # --- 3) Прочие сообщения (диагностика/редкие) — оставим в плоском логе
            try:
                import os
                file_name = self.tasks.get(task_id, {}).get('file', 'N/A')
                log_msg = f"{os.path.basename(file_name)} ({format_name}): {message}"
                if len(log_msg) > 120:
                    log_msg = log_msg[:100] + "..." + log_msg[-20:]
                self.append_to_log(log_msg)
            except Exception:
                pass

        except Exception as e:
            _log("ERR", f"Status update error: {str(e)}")
    
    @pyqtSlot(str, str, int)
    def update_progress(self, task_id, format_name, percent):
        """Обновляет прогресс без моргания текста"""
        try:
            if task_id not in self.tasks or format_name not in self.tasks[task_id]['formats']:
                return

            fmt_data = self.tasks[task_id]['formats'][format_name]
            fmt_data['progress'] = percent
            fmt_data['status'] = 'encoding' if percent < 100 else 'done'

            # Получаем ETA (если есть)
            eta = self.eta.get(task_id, {}).get(format_name, "")

            # Обновляем UI только если бар существует
            if task_id in self.progress_bars and format_name in self.progress_bars[task_id]:
                bar = self.progress_bars[task_id][format_name]
                if sip.isdeleted(bar):
                    return

                # Сохраняем текущее значение текста
                current_text = bar.format()

                # Формируем новый текст (используем только "ETA" для единообразия)
                new_text = f"{percent}% • ETA: {eta}" if eta else f"{percent}%"
                if percent >= 100:
                    new_text = "✓ Готово"
                elif fmt_data['status'] == 'error':
                    new_text = "✗ Ошибка"

                # Обновляем только если текст изменился
                if current_text != new_text:
                    bar.setFormat(new_text)

                # Всегда обновляем значение
                bar.setValue(percent)

        except Exception as e:
            _log("ERR", f"Progress update error: {str(e)}")

    @pyqtSlot(str, str)
    def _safe_update_progress(self, task_id, format_name):
        """Безопасное обновление GUI (выполняется в основном потоке)"""
        try:
            # Проверяем, не закрывается ли приложение
            if self.is_closing:
                return
                
            # Проверяем существование задачи и прогресс-бара
            if task_id not in self.progress_bars or task_id not in self.tasks:
                return
            
            if format_name not in self.tasks[task_id]['formats']:
                return
                
            # Получаем текущий прогресс
            progress = self.tasks[task_id]['formats'][format_name]['progress']
            
            # Обновляем прогресс-бар
            if format_name in self.progress_bars[task_id]:
                self.progress_bars[task_id][format_name].setValue(progress)
            
            # Обновляем элемент списка задач
            self.update_task_item_by_id(task_id)
            
            # Обновляем отображение прогресса, если задача выбрана
            self.update_progress_display_if_selected(task_id)
        except Exception as e:
            _log("ERR", f"Error in _safe_update_progress: {str(e)}")

    def update_task_item_by_id(self, task_id):
        """Обновляет элемент списка задач по ID"""
        for i in range(self.task_list.count()):
            item = self.task_list.item(i)
            if item.data(Qt.UserRole) == task_id:
                self.update_task_item(item)
                break

    def update_progress_display_if_selected(self, task_id):
        """Обновляет отображение прогресса, если задача выбрана"""
        selected_items = self.task_list.selectedItems()
        if selected_items and selected_items[0].data(Qt.UserRole) == task_id:
            self.update_visible_progress()
    
    @pyqtSlot(str, str, bool, str)
    def encoding_finished(self, task_id, format_name, success, output_file):
        """
        Финализация кодирования формата с запуском следующей задачи из очереди.
        """
        try:
            # 1) Уменьшаем счетчик активных задач
            self.active_tasks_count = max(0, self.active_tasks_count - 1)

            # Удаляем ETA для завершенной задачи
            if task_id in self.eta and format_name in self.eta[task_id]:
                del self.eta[task_id][format_name]
                # Если в задаче больше нет ETA, удаляем всю задачу
                if not self.eta[task_id]:
                    del self.eta[task_id]
            
            if task_id in self.eta_sec and format_name in self.eta_sec[task_id]:
                del self.eta_sec[task_id][format_name]
                if not self.eta_sec[task_id]:
                    del self.eta_sec[task_id]
            
            # Обновляем метку с максимальным ETA
            self.update_max_eta_label()
            
            if task_id not in self.tasks:
                return

            task = self.tasks[task_id]
            fmts = task.setdefault('formats', {})
            fmt_entry = fmts.setdefault(format_name, {})

            # --- Прогресс-бар (может быть удалён из UI) ---------------------------
            try:
                bar = self.ensure_progress_bar(task_id, format_name)
            except Exception:
                bar = None

            # Проверка на "зомби"-бар
            try:
                import sip
                bar_deleted = (bar is not None and hasattr(sip, "isdeleted") and sip.isdeleted(bar))
            except Exception:
                bar_deleted = False

            # --- Обновление статуса формата ---------------------------------------
            if success:
                fmt_entry.update({'status': 'done', 'progress': 100, 'output_path': output_file})
                if bar and not bar_deleted:
                    try:
                        bar.setValue(100)
                        bar.setFormat("✓ Готово")
                        bar.setStyleSheet(self._get_progress_style('done'))
                    except Exception:
                        pass
                self.logger.track(task_id, format_name, "готово")
                # Уведомляем FTP: файл готов к загрузке
                if self.KAFKA_AVAILABLE and output_file and os.path.exists(output_file):
                    self._send_encoding_ready_message(task_id, format_name, output_file)
                
            else:
                # Проверяем, не была ли ошибка из-за остановки пользователем
                error_message = output_file if isinstance(output_file, str) else "Unknown error"
                
                if error_message != "Stopped by user":
                    # НЕ предлагаем перезапуск, если пользователь сам остановил
                    
                    # Обновляем статус на ошибку
                    fmt_entry.update({'status': 'error', 'message': error_message})
                    if bar and not bar_deleted:
                        try:
                            bar.setValue(0)
                            bar.setFormat("✗ Ошибка")
                            bar.setStyleSheet(self._get_progress_style('error'))
                        except Exception:
                            pass
                    self.logger.track(task_id, format_name, "ошибка")
                    
                    # Показываем диалоговое окно с предложением перезапуска
                    QTimer.singleShot(100, lambda: self._show_retry_dialog(task_id, format_name, error_message))
                    
                else:
                    # Это остановка пользователем - не показываем диалог
                    fmt_entry.update({'status': 'stopped', 'message': error_message})
                    if bar and not bar_deleted:
                        try:
                            bar.setValue(0)
                            bar.setFormat("⏹ Остановлено")
                            bar.setStyleSheet(self._get_progress_style('stopped'))
                        except Exception:
                            pass
                    self.logger.track(task_id, format_name, "остановлено")

            # --- Закрываем и вычищаем поток --------------------------------------
            key = (task_id, format_name)
            if key in self.threads:
                thr = self.threads[key]
                try:
                    if thr.isRunning():
                        thr.quit()
                        thr.wait(1000)
                        if thr.isRunning():
                            thr.terminate()
                            thr.wait(500)
                except Exception:
                    pass
                finally:
                    # Разрываем все соединения
                    try:
                        thr.update_signal.disconnect()
                        thr.progress_signal.disconnect()
                        thr.finished_signal.disconnect()
                    except:
                        pass
                    
                    thr.deleteLater()
                    self.threads.pop(key, None)

            # --- Спец-логика для AniDub / TVHub ----------------------------------
            if success and format_name in {'anidub', 'tvhub'}:
                try:
                    # 1) Удаляем исходный файл и фиксируем в "Подготовка"
                    original_file = task.get('original_file')
                    if original_file and os.path.exists(original_file):
                        try:
                            os.remove(original_file)
                            self.logger.source_removed(task_id, os.path.basename(original_file))
                        except Exception as e_del:
                            self.append_to_log(f"Не удалось удалить исходный файл: {e_del}")

                    # 2) Ставим готовый MKV как НОВУЮ задачу
                    if output_file and os.path.exists(output_file):
                        self._add_mkv_to_queue(output_file)
                    else:
                        self.append_to_log("Готовый MKV не найден для постановки в очередь")

                except Exception as e_post:
                    self.append_to_log(f"Post-{format_name} обработка не удалась: {e_post}")

            # --- Пересчёт общего статуса задачи ----------------------------------
            try:
                self._update_overall_task_status(task_id)
            except Exception:
                pass

        except Exception as e:
            self.append_to_log(f"Ошибка при завершении задачи: {e}")
            _log("ERR", "encoding_finished error")
        finally:
            # --- Проверяем, остались ли активные задачи и останавливаем таймер общего времени если нет ---
            if not self.check_active_encoding_tasks():
                self.stop_total_encoding_timer()
            
            # --- Сначала обновляем GUI текущей задачи ----------------------------
            try:
                self._update_task_item_in_list(task_id)
                self.update_visible_progress()
            except Exception:
                pass
                
            # --- Затем обновляем весь GUI ----------------------------------------
            try:
                self.update_global_controls()
                QApplication.processEvents()
            except Exception:
                pass
                
            # --- ГАРАНТИРОВАННО запускаем следующую задачу через 100мс ----------
            QTimer.singleShot(100, self._start_next_task)
            
    def _show_retry_dialog(self, task_id, format_name, error_message):
        """Показывает диалоговое окно с предложением перезапустить задачу с ошибкой"""
        try:
            # Проверяем, существует ли еще задача
            if task_id not in self.tasks or format_name not in self.tasks[task_id]['formats']:
                return
                
            task = self.tasks[task_id]
            filename = os.path.basename(task['file'])
            
            # Создаем информативное сообщение
            short_error = error_message
            if len(error_message) > 200:
                short_error = error_message[:200] + "..."
                
            message = (
                f"Произошла ошибка при кодировании {format_name} для файла:\n\n"
                f"📁 {filename}\n\n"
                f"❌ Ошибка: {short_error}\n\n"
                f"Перезапустить этот формат? Он будет поставлен в начало очереди."
            )
            
            # Показываем диалоговое окно
            reply = QMessageBox.question(
                self,
                "Ошибка кодирования",
                message,
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.Yes
            )
            
            if reply == QMessageBox.Yes:
                _log("INFO", f"Пользователь согласился перезапустить {format_name} для {filename}")
                
                # Полностью сбрасываем статус формата
                fmt_data = self.tasks[task_id]['formats'][format_name]
                fmt_data.update({
                    'status': 'waiting',
                    'progress': 0,
                    'message': None,
                    'pid': None,
                    'output_path': None
                })
                
                # Обновляем прогресс-бар
                if task_id in self.progress_bars and format_name in self.progress_bars[task_id]:
                    bar = self.progress_bars[task_id][format_name]
                    try:
                        if not sip.isdeleted(bar):
                            bar.setValue(0)
                            bar.setFormat("Ожидание")
                            bar.setStyleSheet(self._get_progress_style('waiting'))
                    except Exception:
                        pass
                
                # Удаляем задачу из очереди, если она там уже есть
                self.pending_tasks = [(tid, fmt) for (tid, fmt) in self.pending_tasks 
                                      if not (tid == task_id and fmt == format_name)]
                
                # Ставим задачу В САМЫЙ НАЧАЛО очереди
                self.pending_tasks.insert(0, (task_id, format_name))
                
                # Обновляем общий статус задачи
                self._update_overall_task_status(task_id)
                
                # Обновляем элемент в списке
                self._update_task_item_in_list(task_id)
                
                # Запускаем следующую задачу (эта теперь первая в очереди)
                QTimer.singleShot(200, self._start_next_task)
                
                self.append_to_log(f"🔄 Формат {format_name} для {filename} перезапущен и поставлен в начало очереди")
            else:
                _log("INFO", f"Пользователь отказался перезапускать {format_name} для {filename}")
                self.append_to_log(f"❌ Формат {format_name} для {filename} остался с ошибкой")
                
        except Exception as e:
            _log("ERR", f"Ошибка при показе диалога перезапуска: {str(e)}")
                
    def _start_next_task(self):
        """Запускает следующую задачу из очереди по одной (FIFO)"""
        # Если выполняется остановка всех задач, не запускаем новые
        if hasattr(self, 'is_stopping_all') and self.is_stopping_all:
            return
            
        # Проверяем, есть ли свободные слоты
        if self.active_tasks_count >= self.max_concurrent_tasks:
            return
            
        # Проверяем, есть ли задачи в очереди
        if not self.pending_tasks:
            return
            
        # Берем первую задачу из очереди (FIFO)
        task_id, format_name = self.pending_tasks[0]
        
        # Проверяем, что задача еще существует
        if task_id not in self.tasks:
            # Задача была удалена, убираем из очереди и пробуем следующую
            self.pending_tasks.pop(0)
            QTimer.singleShot(50, self._start_next_task)
            return
            
        task = self.tasks[task_id]
        
        # Проверяем, что формат существует
        if format_name not in task['formats']:
            # Формат не найден, убираем из очереди
            self.pending_tasks.pop(0)
            QTimer.singleShot(50, self._start_next_task)
            return
            
        fmt_data = task['formats'][format_name]
        
        # Проверяем, что формат можно запустить (ожидание, остановлен или ошибка)
        if fmt_data.get('status') not in ('waiting', 'stopped', 'error'):
            # Формат уже запущен или завершен, убираем из очереди
            self.pending_tasks.pop(0)
            QTimer.singleShot(50, self._start_next_task)
            return
        
        # Убираем задачу из очереди перед запуском
        self.pending_tasks.pop(0)
        
        # Если формат был в ошибке, сбрасываем сообщение об ошибке
        if fmt_data.get('status') == 'error':
            fmt_data['message'] = None
        
        # Запускаем формат
        success = self._start_encoding_task_direct(task_id, format_name)
        
        if success:
            _log("INFO", f"Запущен формат {format_name} для {os.path.basename(task['file'])} (активных: {self.active_tasks_count}/{self.max_concurrent_tasks})")
            # СРАЗУ обновляем GUI
            self.update_global_controls()
            QApplication.processEvents()
            
            # Обновляем статус задачи
            if task['status'] in ('waiting', 'stopped', 'error'):
                task['status'] = 'encoding'
            self._update_task_item_in_list(task_id)
        else:
            # Не удалось запустить, возвращаем в конец очереди
            self.pending_tasks.append((task_id, format_name))
            _log("WARN", f"Не удалось запустить {format_name} для {os.path.basename(task['file'])} - в очередь")
            
        # Немедленно пытаемся запустить следующую задачу (если есть свободные слоты)
        if self.active_tasks_count < self.max_concurrent_tasks and self.pending_tasks:
            QTimer.singleShot(50, self._start_next_task)
        
    def print_queue_status(self):
        """Выводит статус очереди в консоль (для отладки)"""
        _log("INFO", "--- Статус очереди ---")
        _log("INFO", f"Активных: {self.active_tasks_count}/{self.max_concurrent_tasks}, в очереди: {len(self.pending_tasks)}, всего задач: {len(self.tasks)}")
        if self.pending_tasks:
            for i, (task_id, fmt) in enumerate(self.pending_tasks[:5], 1):
                if task_id in self.tasks:
                    filename = os.path.basename(self.tasks[task_id]['file'])
                    _log("INFO", f"  {i}. {filename} ({fmt})")
                else:
                    _log("INFO", f"  {i}. [УДАЛЕНО] ({fmt})")
            if len(self.pending_tasks) > 5:
                _log("INFO", f"  ... и еще {len(self.pending_tasks) - 5}")
            
    def _update_all_task_items(self):
        """Обновляет все элементы списка задач"""
        try:
            for i in range(self.task_list.count()):
                item = self.task_list.item(i)
                if item:
                    task_id = item.data(Qt.UserRole)
                    if task_id in self.tasks:
                        self._update_task_item_in_list(task_id)
            
            # Принудительная перерисовка
            self.task_list.viewport().update()
            QApplication.processEvents()
            
        except Exception as e:
            _log("ERR", f"Ошибка обновления всех элементов списка: {str(e)}")
                
    def _start_encoding_task_direct(self, task_id, format_name):
        """Прямой запуск задачи без проверки очереди"""
        try:
            # 1) Валидация задачи и файла
            if task_id not in self.tasks:
                return False
            task = self.tasks[task_id]

            import os
            if not os.path.exists(task['file']):
                error_msg = f"Файл не найден: {task['file']}"
                task['formats'][format_name]['status'] = 'error'
                task['formats'][format_name]['message'] = error_msg
                self.append_to_log(error_msg)
                return False

            # 2) Инициализация структуры форматов
            fmts = task.setdefault('formats', {})
            fmt_data = fmts.setdefault(format_name, {
                'status': 'waiting', 
                'progress': 0, 
                'pid': None, 
                'output_path': None, 
                'message': None
            })
            
            # 3) Остановка предыдущего потока (если есть)
            key = (task_id, format_name)
            if key in self.threads:
                thr = self.threads[key]
                if thr.isRunning():
                    try:
                        thr.safe_quit()
                        thr.wait(1000)
                    except:
                        pass
                del self.threads[key]

            # 4) Прогресс-бар
            bar = self.ensure_progress_bar(task_id, format_name)
            try:
                if bar:
                    bar.setValue(0)
                    bar.setFormat("0%")
                    bar.setStyleSheet(self._get_progress_style('encoding'))
            except:
                pass

            # 5) Статусы
            fmt_data.update({
                'status': 'encoding',
                'progress': 0,
                'pid': None,
                'message': None
            })
            
            if task['status'] == 'waiting':
                task['status'] = 'encoding'

            # 6) Увеличиваем счетчик активных задач
            self.active_tasks_count += 1

            # 7) Создание и запуск потока
            thread = EncodingThread(
                task_id=task_id,
                input_file=task['file'],
                output_format=format_name,
                logger=getattr(self, 'logger', None),
                task_type=task.get('type', '')  # НОВОЕ: передаем тип задачи
            )
            thread.update_signal.connect(self.update_status, Qt.QueuedConnection)
            thread.progress_signal.connect(self.update_progress, Qt.QueuedConnection)
            thread.finished_signal.connect(lambda *args: self.encoding_finished(*args), Qt.QueuedConnection)

            self.threads[key] = thread
            thread.start()
            
            return True

        except Exception as e:
            # Уменьшаем счетчик при ошибке
            self.active_tasks_count = max(0, self.active_tasks_count - 1)
            error_msg = f"Ошибка запуска {format_name}: {str(e)}"
            self.append_to_log(error_msg)
            return False
            
    def _update_overall_task_status(self, task_id):
        """Определяет общий статус задачи на основе статусов форматов."""
        if task_id not in self.tasks:
            return

        task = self.tasks[task_id]
        formats = task.get('formats', {})

        # Проверяем, есть ли активные треды для этой задачи
        has_active_threads = any(
            (task_id, fmt) in self.threads and self.threads[(task_id, fmt)].isRunning()
            for fmt in formats
        )
        
        if has_active_threads:
            task['status'] = 'encoding'
            return

        # Если нет форматов - это копирование или особая задача
        if not formats:
            # Для задач без форматов сохраняем текущий статус
            return

        # Получаем все форматы
        considered = list(formats.values())

        if not considered:
            # Нет форматов
            task['status'] = 'waiting'
            return

        # Проверяем, есть ли хотя бы один формат в процессе кодирования
        if any(f.get('status') == 'encoding' for f in considered):
            task['status'] = 'encoding'
        # Если есть ошибки - показываем ошибку
        elif any(f.get('status') == 'error' for f in considered):
            task['status'] = 'error'
        # Если все форматы завершены - показываем готово
        elif all(f.get('status') == 'done' for f in considered):
            task['status'] = 'done'
        # Если есть остановленные - показываем остановлено
        elif any(f.get('status') == 'stopped' for f in considered):
            task['status'] = 'stopped'
        # Иначе - ожидание
        else:
            task['status'] = 'waiting'
            
    def _is_format_active(self, format_name):
        """Проверяет, является ли формат активным (теперь просто проверяет допустимость формата)"""
        return format_name in {'xvid', '400p', '720p', 'x265', 'anidub', 'tvhub'}
        
    def _update_task_item_in_list(self, task_id):
        """Обновляет элемент списка задач"""
        if task_id not in self.tasks:
            return

        task = self.tasks[task_id]
        filename = os.path.basename(task['file'])
        status = task.get('status', 'waiting')
        task_type = task.get('type', '')

        # Определяем статус и цвет
        if task_type == 'tvhub_copy':
            # Задача копирования TVHub
            if status == 'copying':
                progress = task.get('progress', 0)
                status_text = f"📁 Копирование TVHub ({progress}%) — {filename}"
                color = QColor(255, 243, 224)  # Оранжевый
            elif status == 'done':
                # Изменено здесь: убираем "Копирование завершено" на "✓ Готово"
                status_text = f"✓ Готово — {filename}"
                color = QColor(232, 245, 233)  # Зеленый
            elif status == 'error':
                error_msg = task.get('error', 'Ошибка')
                status_text = f"✗ Ошибка копирования — {filename}"
                color = QColor(255, 235, 238)  # Красный
            else:
                status_text = f"⏸ Ожидание копирования — {filename}"
                color = QColor(255, 248, 225)  # Желтый
                
        elif task_type == 'tvhub_encoding':
            # Задача кодирования TVHub
            if status == 'encoding':
                # Показываем активные форматы
                active_fmts = []
                for fmt, data in task.get('formats', {}).items():
                    if data.get('status') == 'encoding':
                        active_fmts.append(fmt)
                if active_fmts:
                    status_text = f"▶ Кодирование TVHub ({', '.join(active_fmts)}) — {filename}"
                else:
                    status_text = f"▶ Кодирование TVHub — {filename}"
                color = QColor(230, 225, 245)  # Фиолетовый
            elif status == 'done':
                # Изменено здесь: убираем "TVHub готово" на "✓ Готово"
                status_text = f"✓ Готово — {filename}"
                color = QColor(232, 245, 233)  # Зеленый
            elif status == 'error':
                status_text = f"✗ Ошибка TVHub — {filename}"
                color = QColor(255, 235, 238)  # Красный
            elif status == 'stopped':
                status_text = f"⏹ Остановлено TVHub — {filename}"
                color = QColor(255, 224, 178)  # Оранжевый
            else:
                status_text = f"⏸ Ожидание TVHub — {filename}"
                color = QColor(230, 225, 245)  # Фиолетовый (светлый)
                
        else:
            # Обычная задача
            status_info = {
                'done': ("✓ Готово", QColor(232, 245, 233)),
                'encoding': ("▶ В процессе", QColor(227, 242, 253)),
                'error': ("✗ Ошибка", QColor(255, 235, 238)),
                'stopped': ("⏹ Остановлено", QColor(255, 224, 178)),
                'waiting': ("⏸ Ожидание", QColor(255, 248, 225)),
                'copying': ("📁 Копирование", QColor(255, 243, 224)),
            }.get(status, ("? Неизвестно", QColor(240, 240, 240)))
            
            if status == 'copying':
                progress = task.get('progress', 0)
                status_text = f"📁 Копирование ({progress}%) — {filename}"
            elif status == 'encoding':
                active_fmts = []
                for fmt, data in task.get('formats', {}).items():
                    if data.get('status') == 'encoding':
                        active_fmts.append(fmt)
                if active_fmts:
                    status_text = f"▶ Кодирование ({', '.join(active_fmts)}) — {filename}"
                else:
                    status_text = f"▶ В процессе — {filename}"
            elif status == 'done':
                # Оставляем просто "✓ Готово" без счетчика
                status_text = f"✓ Готово — {filename}"
            else:
                status_text = f"{status_info[0]} — {filename}"
            
            color = status_info[1]

        # Находим и обновляем элемент в списке
        for i in range(self.task_list.count()):
            item = self.task_list.item(i)
            if item and item.data(Qt.UserRole) == task_id:
                item.setText(status_text)
                item.setData(Qt.UserRole + 1, status)
                item.setBackground(color)
                
                # Обновляем тултип
                tooltip = [f"Файл: {task['file']}", f"Статус: {status}"]
                
                if task_type == 'tvhub_copy':
                    if 'target_file' in task:
                        tooltip.append(f"TVHub: {task['target_file']}")
                    if 'progress' in task:
                        tooltip.append(f"Прогресс: {task['progress']}%")
                        
                elif task.get('formats'):
                    for fmt, data in task['formats'].items():
                        if data.get('status') != 'waiting':
                            tooltip.append(f"{fmt}: {data.get('status')} ({data.get('progress', 0)}%)")
                
                item.setToolTip("\n".join(tooltip))
                break
                
    def _offer_retry(self, task_id, format_name, filename):
        """Предлагает перезапустить ошибочное кодирование"""
        if self.is_closing or task_id not in self.tasks:
            return
            
        if self.tasks[task_id]['formats'][format_name]['status'] != 'error':
            return
            
        reply = QMessageBox.question(
            self,
            "Encoding Error",
            f"{filename} ({format_name}) failed. Retry encoding?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.Yes
        )
        
        if reply == QMessageBox.Yes:
            self.start_format(task_id, format_name)

    def _determine_output_path(self, task_id, format_name):
        """Определяет путь к выходному файлу на основе стандартных шаблонов"""
        try:
            input_file = self.tasks[task_id]['file']
            base_name = os.path.splitext(os.path.basename(input_file))[0]
            
            # Удаляем .mkv если уже есть в имени
            if base_name.lower().endswith('.mkv'):
                base_name = base_name[:-4]
            
            if format_name == 'anidub':
                output_name = base_name.replace('_', '.') + '.mkv'
                return os.path.normpath(os.path.join("C:/Stax/Rip/Anidub", output_name))
            elif format_name == 'xvid':
                output_name = base_name + '.avi'
                return os.path.normpath(os.path.join("C:/Stax/Rip/Xvid", output_name))
            elif format_name == '400p':
                output_name = base_name + '.mp4'
                return os.path.normpath(os.path.join("C:/Stax/Rip/x264", output_name))
            elif format_name == '720p':
                output_name = base_name + '.mp4'
                return os.path.normpath(os.path.join("C:/Stax/Rip/720", output_name))
            elif format_name == 'tvhub':
                # Для TVHub используем формат как для обычных форматов
                if format_name == '400p':
                    output_name = base_name + '.mp4'
                    return os.path.normpath(os.path.join("C:/Stax/Rip/x264", output_name))
                elif format_name == '720p':
                    output_name = base_name + '.mp4'
                    return os.path.normpath(os.path.join("C:/Stax/Rip/720", output_name))
        except Exception as e:
            _log("ERR", f"Ошибка определения пути: {str(e)}")
        return None
            
    def _add_mkv_to_queue(self, mkv_path):
        """
        Добавляет готовый MKV в очередь как НОВУЮ задачу и сразу запускает активированные форматы.
        Возвращает task_id или None. Не зависит от _create_task.
        """
        import os, uuid
        from PyQt5.QtCore import Qt
        try:
            from PyQt5.QtWidgets import QListWidgetItem
        except Exception:
            QListWidgetItem = None

        if not mkv_path or not os.path.exists(mkv_path):
            self.append_to_log(f"MKV не найден: {mkv_path}")
            return None

        if not hasattr(self, "tasks"):
            self.tasks = {}

        # если задача уже есть для этого файла — переиспользуем
        for tid, t in self.tasks.items():
            if t.get("file") == mkv_path:
                self.logger.task_init(tid, os.path.basename(mkv_path))
                return tid

        # создаём новую задачу
        task_id = str(uuid.uuid4())
        self.tasks[task_id] = {
            "file": mkv_path,
            "formats": {},
            "status": "waiting",
            "original_file": None,
        }

        # строка в списке задач
        try:
            if QListWidgetItem is not None and hasattr(self, "task_list") and self.task_list is not None:
                item = QListWidgetItem(f"⏸ Ожидание — {os.path.basename(mkv_path)}")
                item.setData(Qt.UserRole, task_id)
                item.setData(Qt.UserRole + 1, "waiting")
                self.task_list.addItem(item)
        except Exception as e:
            self.append_to_log(f"Не удалось добавить задачу в список: {e}")

        # шапка иерархического лога
        self.logger.task_init(task_id, os.path.basename(mkv_path))

        # какие форматы включены
        enabled = []
        try:
            if getattr(self, 'xvid_check', None) and self.xvid_check.isChecked(): enabled.append('xvid')
            if getattr(self, 'p400_check', None) and self.p400_check.isChecked(): enabled.append('400p')
            if getattr(self, 'p720_check', None) and self.p720_check.isChecked(): enabled.append('720p')
            if getattr(self, 'x265_check', None) and self.x265_check.isChecked(): enabled.append('x265')
        except Exception:
            pass

        # Определяем порядок создания прогресс-баров
        format_order = ['xvid', '400p', '720p', 'x265']
        
        # подготовка баров в правильном порядке
        for fmt in format_order:
            if fmt in enabled:
                self.tasks[task_id]["formats"].setdefault(fmt, {
                    "status": "waiting",
                    "progress": 0,
                    "pid": None,
                    "output_path": None
                })
                try:
                    self.ensure_progress_bar(task_id, fmt)
                except Exception:
                    pass

        # UI обновление
        try:
            self._update_task_item_in_list(task_id)
            self.update_visible_progress()
            self.update_global_controls()
        except Exception:
            pass

        # Уведомляем FTP: добавлена серия (исходник = mkv_path), как при «добавить файлы»
        if self.KAFKA_AVAILABLE and enabled:
            self._send_ftp_queue_message(task_id, mkv_path)

        # запуск
        for fmt in enabled:
            try:
                self.start_encoding_thread(task_id, fmt)
            except Exception as e:
                self.append_to_log(f"Ошибка старта {fmt}: {e}")

        return task_id
            
    def _update_task_status(self, task_id):
        """Обновляет общий статус задачи (игнорируя форматы, которые не запускались)."""
        if task_id not in self.tasks:
            return

        task = self.tasks[task_id]
        formats = task.get('formats', {})

        considered = [f for f in formats.values() if f.get('status') != 'waiting']

        if not considered:
            task['status'] = 'waiting'
            return

        if any(f.get('status') == 'error' for f in considered):
            task['status'] = 'error'
        elif any(f.get('status') == 'encoding' for f in considered):
            task['status'] = 'encoding'
        elif all(f.get('status') == 'done' for f in considered):
            task['status'] = 'done'
        elif any(f.get('status') == 'stopped' for f in considered):
            task['status'] = 'stopped'
        else:
            task['status'] = 'waiting'
            
    def _handle_successful_completion(self, task_id, format_name, output_path):
        """Обработка успешного завершения"""
        self.tasks[task_id]['formats'][format_name].update({
            'status': 'done',
            'progress': 100,
            'output_path': output_path
        })
        
        if task_id in self.progress_bars and format_name in self.progress_bars[task_id]:
            self.progress_bars[task_id][format_name].setValue(100)
            
    def _process_created_mkv(self, mkv_path):
        """Обработка созданного MKV файла"""
        if not any(self._is_same_file(task['file'], mkv_path) for task in self.tasks.values()):
            self._add_new_mkv_task(mkv_path)

    def _is_same_file(self, path1, path2):
        """Проверка, что пути ведут к одному файлу"""
        try:
            return os.path.normcase(os.path.abspath(path1)) == os.path.normcase(os.path.abspath(path2))
        except Exception:
            return False

    def _add_new_mkv_task(self, mkv_path):
        """Добавление новой задачи для MKV"""
        task_id = str(self.current_task_id)
        self.current_task_id += 1

        # Создание структуры задачи
        self.tasks[task_id] = {
            'file': mkv_path,
            'status': 'waiting',
            'formats': {
                'xvid': {'status': 'waiting', 'progress': 0, 'pid': None},
                '400p': {'status': 'waiting', 'progress': 0, 'pid': None},
                '720p': {'status': 'waiting', 'progress': 0, 'pid': None}
            }
        }

        # Создание элементов UI
        self.progress_bars[task_id] = {
            'xvid': self._create_progress_bar(),
            '400p': self._create_progress_bar(),
            '720p': self._create_progress_bar(),
            'x265': self._create_progress_bar()
        }

        item = QListWidgetItem(f"⏸ {os.path.basename(mkv_path)} (MKV)")
        item.setData(Qt.UserRole, task_id)
        item.setData(Qt.UserRole + 1, "waiting")
        self.task_list.addItem(item)

        # Автозапуск если нужно
        if any([self.xvid_check.isChecked(), self.p400_check.isChecked(), self.p720_check.isChecked()]):
            self.start_specific_task(task_id)

        self.append_to_log(f"Добавлен в очередь: {os.path.basename(mkv_path)}")
    
    def update_global_controls(self):
        """Обновление глобальных элементов управления с информацией об очереди"""
        # Считаем РЕАЛЬНОЕ количество запущенных потоков
        real_active_count = len([t for t in self.threads.values() if t.isRunning()])
        
        # Для отладки - если счетчик расходится с реальностью, корректируем его
        if real_active_count != self.active_tasks_count:
            _log("WARN", f"Расхождение: счетчик={self.active_tasks_count}, реально={real_active_count}")
            self.active_tasks_count = real_active_count
        
        # Enable/disable global buttons based on current state
        has_waiting_or_stopped = any(
            task.get('status') in ('waiting', 'stopped') 
            for task in self.tasks.values()
        )
        has_encoding = any(task.get('status') == 'encoding' for task in self.tasks.values())
        
        self.global_start_btn.setEnabled(has_waiting_or_stopped and real_active_count < self.max_concurrent_tasks)
        self.global_stop_btn.setEnabled(has_encoding or real_active_count > 0 or len(self.pending_tasks) > 0)
        
        # Добавляем информацию об очереди и активных задачах
        queue_info = f"Активных: {real_active_count}/{self.max_concurrent_tasks} | Очередь: {len(self.pending_tasks)}"
        total = real_active_count + len(self.pending_tasks)
        self.encoder_queue_total_changed.emit(total)
        
        # Обновляем статусную строку или заголовок окна
        try:
            self.statusBar().showMessage(queue_info)
        except:
            # Если статусной строки нет, добавляем в заголовок окна
            base_title = "Advanced Video Encoder"
            self.setWindowTitle(f"{base_title} - {queue_info}")
        
    def print_queue_debug(self):
        """Отладочная информация о состоянии очереди"""
        _log("INFO", "--- ОТЛАДКА ОЧЕРЕДИ ---")
        _log("INFO", f"Активных (счетчик): {self.active_tasks_count}, потоков запущено: {len([t for t in self.threads.values() if t.isRunning()])}, всего потоков: {len(self.threads)}")
        _log("INFO", f"pending_tasks: {len(self.pending_tasks)}, tasks: {len(self.tasks)}")
        
        # Считаем статусы задач
        status_counts = {}
        for task in self.tasks.values():
            status = task.get('status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1
        
        for status, count in status_counts.items():
            _log("INFO", f"  {status}: {count}")
    
    def closeEvent(self, event):
        """Обработчик закрытия окна с точной проверкой активных задач"""
        # Проверяем, есть ли действительно выполняющиеся задачи
        has_active_tasks = any(
            thread.isRunning() for (_, _), thread in self.threads.items()
        ) or any(
            task['status'] == 'encoding' for task in self.tasks.values()
        )

        if has_active_tasks:
            reply = QMessageBox.question(
                self, 'Encoding in Progress',
                "Tasks are running. Close and stop encoding?",
                QMessageBox.Yes | QMessageBox.No, 
                QMessageBox.No
            )
            
            if reply == QMessageBox.Yes:
                self.cleanup()
                event.accept()
            else:
                event.ignore()
        else:
            # Останавливаем Kafka потребитель перед закрытием
            self.stop_kafka_consumer()
            event.accept()

    def cleanup(self):
        """Гарантированная очистка всех ресурсов при закрытии приложения"""
        self.is_closing = True
        
        # Очищаем ETA
        self.eta.clear()
        self.eta_sec.clear()
        
        # Обновляем метку с максимальным ETA
        if hasattr(self, 'max_eta_label') and self.max_eta_label:
            self.max_eta_label.setText("⏱ Макс. ETA: --:--:--")
            self.max_eta_label.setStyleSheet("""
                QLabel {
                    font: 10pt 'Segoe UI';
                    color: #555;
                    padding: 8px 12px;
                    background-color: #f8f9fa;
                    border-radius: 6px;
                    border: 1px solid #dee2e6;
                    min-width: 180px;
                }
            """)
        
        # Останавливаем все таймеры
        if hasattr(self, 'memory_cleanup_timer'):
            self.memory_cleanup_timer.stop()
        
        if hasattr(self, 'total_encoding_timer') and self.total_encoding_timer.isActive():
            self.total_encoding_timer.stop()
            self.total_encoding_start_time = None
        
        if hasattr(self, 'resource_timer'):
            self.resource_timer.stop()
        
        if hasattr(self, 'other_timers'):
            for timer in self.other_timers:
                timer.stop()
        
        try:
            # 1. Останавливаем Kafka потребитель
            self.stop_kafka_consumer()

            # 2. Останавливаем все потоки кодирования
            for (task_id, format_name), thread in list(self.threads.items()):
                try:
                    if thread.isRunning():
                        # Сначала мягкое завершение
                        thread.safe_quit()
                        
                        # Ждем завершения до 3 секунд
                        if not thread.wait(3000):
                            # Если не завершился - принудительно
                            thread.terminate()
                            thread.wait(1000)
                        
                        # Разрываем соединения
                        try:
                            thread.update_signal.disconnect()
                            thread.progress_signal.disconnect()
                            thread.finished_signal.disconnect()
                        except:
                            pass
                        
                        # Безопасное удаление объекта потока
                        thread.deleteLater()
                        
                except Exception as e:
                    _log("ERR", f"Error stopping encoding thread ({task_id}, {format_name}): {str(e)}")
                finally:
                    # Удаляем ссылку из словаря
                    del self.threads[(task_id, format_name)]

            # 3. Останавливаем поток копирования файлов (если есть)
            if hasattr(self, 'copy_thread') and self.copy_thread and self.copy_thread.isRunning():
                try:
                    self.copy_thread.cancel()
                    if not self.copy_thread.wait(2000):
                        self.copy_thread.terminate()
                    
                    # Удаляем частично скопированный файл
                    if hasattr(self.copy_thread, 'to_path') and os.path.exists(self.copy_thread.to_path):
                        try:
                            os.remove(self.copy_thread.to_path)
                        except Exception as e:
                            _log("ERR", f"Failed to remove partial copy: {str(e)}")
                            
                except Exception as e:
                    _log("ERR", f"Error stopping copy thread: {str(e)}")
                finally:
                    self.copy_thread.deleteLater()

            # 4. Очищаем все структуры данных
            self.threads.clear()
            self.tasks.clear()
            self.pending_tasks.clear()
            self.active_tasks_count = 0
            
            # 5. Удаляем все прогресс-бары
            for task_id, bars in list(self.progress_bars.items()):
                for bar_name, bar in bars.items():
                    try:
                        if bar and not sip.isdeleted(bar):
                            bar.setParent(None)
                            bar.deleteLater()
                    except Exception as e:
                        _log("ERR", f"Error deleting progress bar: {str(e)}")
            self.progress_bars.clear()

            # 6. Закрываем Kafka consumer (дополнительная защита)
            if hasattr(self, 'kafka_consumer') and self.kafka_consumer:
                try:
                    self.kafka_consumer.close()
                except Exception as e:
                    _log("ERR", f"Error closing Kafka consumer: {str(e)}")
                finally:
                    self.kafka_consumer = None

            # 7. Принудительная сборка мусора
            try:
                import gc
                gc.collect()
            except Exception as e:
                _log("ERR", f"GC collection error: {str(e)}")

            # 8. Очищаем ссылки на UI элементы
            try:
                if hasattr(self, 'task_list'):
                    self.task_list.clear()
                if hasattr(self, 'log_output'):
                    self.log_output.clear()
            except Exception as e:
                _log("ERR", f"Error clearing UI elements: {str(e)}")

        except Exception as e:
            _log("ERR", f"Critical error during cleanup: {str(e)}")
            self.append_to_log(f"Ошибка при очистке: {str(e)}")
        finally:
            # 10. Логирование завершения очистки
            _log("INFO", "Application cleanup completed")
            self.append_to_log("Очистка ресурсов завершена")
            
            # 11. Гарантируем, что все потоки завершены
            try:
                # Ждем завершения всех потоков
                for thread_name in ['kafka_thread', 'copy_thread']:
                    if hasattr(self, thread_name):
                        thread = getattr(self, thread_name)
                        if thread and thread.isRunning():
                            thread.wait(1000)
            except Exception as e:
                _log("ERR", f"Error in final thread cleanup: {str(e)}")
                
    def start_total_encoding_timer(self):
        """Запускает таймер общего времени кодирования"""
        if not hasattr(self, 'total_encoding_start_time') or self.total_encoding_start_time is None:
            self.total_encoding_start_time = datetime.now()
            if hasattr(self, 'total_encoding_timer'):
                self.total_encoding_timer.start(1000)
            _log("INFO", "Таймер общего времени запущен")

    def stop_total_encoding_timer(self):
        """Останавливает таймер общего времени кодирования"""
        if hasattr(self, 'total_encoding_start_time') and self.total_encoding_start_time is not None:
            if hasattr(self, 'total_encoding_timer'):
                self.total_encoding_timer.stop()
            
            # Выводим общее время только один раз при остановке
            elapsed = datetime.now() - self.total_encoding_start_time
            total_seconds = int(elapsed.total_seconds())
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            
            if total_seconds > 60:  # Выводим только если кодирование шло больше минуты
                self.append_to_log(f"⏱ Общее время кодирования: {hours:02d}:{minutes:02d}:{seconds:02d}")
            
            self.total_encoding_start_time = None
            _log("INFO", "Таймер общего времени остановлен")

    def update_total_encoding_time(self):
        """Обновляет отображение общего времени кодирования"""
        if not hasattr(self, 'total_encoding_start_time') or self.total_encoding_start_time is None:
            return
        
        elapsed = datetime.now() - self.total_encoding_start_time
        total_seconds = int(elapsed.total_seconds())
        
        # Форматируем время
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        
        if hasattr(self, 'statusBar') and self.statusBar():
            self.statusBar().showMessage(f"Общее время кодирования: {hours:02d}:{minutes:02d}:{seconds:02d}")

    def check_active_encoding_tasks(self):
        """Проверяет, есть ли активные задачи кодирования или задачи в очереди"""
        # Проверяем реально запущенные потоки
        if any(thread.isRunning() for thread in self.threads.values()):
            return True
        
        # Проверяем задачи с форматами
        for task in self.tasks.values():
            # Проверяем статус задачи
            if task.get('status') == 'encoding':
                return True
            # Проверяем форматы внутри задачи
            for fmt_data in task.get('formats', {}).values():
                if fmt_data.get('status') == 'encoding':
                    return True
            # Проверяем задачи копирования
            if task.get('type') == 'tvhub_copy' and task.get('status') == 'copying':
                return True
        
        # Проверяем задачи в очереди
        if self.pending_tasks:
            return True
        
        # Проверяем активные задачи по счетчику
        if self.active_tasks_count > 0:
            return True
        
        return False
        
    def update_max_eta_label(self):
        """Обновляет метку с максимальным ETA среди всех активных задач"""
        max_eta_seconds = 0
        max_eta_str = "--:--:--"
        
        # Ищем максимальное ETA среди всех активных задач
        for task_id, task_eta in self.eta.items():
            for format_name, eta_str in task_eta.items():
                if eta_str and isinstance(eta_str, str) and eta_str != "--:--:--":
                    try:
                        # Парсим время в формате HH:MM:SS
                        if ':' in eta_str:
                            parts = eta_str.split(':')
                            if len(parts) == 3:
                                h, m, s = map(int, parts)
                                eta_seconds = h * 3600 + m * 60 + s
                                
                                # Сохраняем максимальное значение
                                if eta_seconds > max_eta_seconds:
                                    max_eta_seconds = eta_seconds
                                    max_eta_str = eta_str
                    except Exception as e:
                        _log("WARN", f"Ошибка парсинга ETA: {eta_str} - {e}")
        
        # Формируем текст для метки
        if max_eta_seconds > 0:
            text = f"⏱ Макс. ETA: {max_eta_str}"
            
            # Меняем цвет в зависимости от времени
            hours = max_eta_seconds // 3600
            minutes = (max_eta_seconds % 3600) // 60
            
            if hours >= 1:
                # Более 1 часа - красный
                style = """
                    QLabel {
                        font: bold 10pt 'Segoe UI';
                        color: #dc3545;
                        padding: 8px 12px;
                        background-color: #f8d7da;
                        border-radius: 6px;
                        border: 1px solid #f5c6cb;
                        min-width: 180px;
                    }
                """
            elif minutes >= 30:
                # Более 30 минут - оранжевый
                style = """
                    QLabel {
                        font: bold 10pt 'Segoe UI';
                        color: #fd7e14;
                        padding: 8px 12px;
                        background-color: #fff3cd;
                        border-radius: 6px;
                        border: 1px solid #ffeaa7;
                        min-width: 180px;
                    }
                """
            else:
                # Менее 30 минут - синий
                style = """
                    QLabel {
                        font: bold 10pt 'Segoe UI';
                        color: #0d6efd;
                        padding: 8px 12px;
                        background-color: #e8f4fd;
                        border-radius: 6px;
                        border: 1px solid #b6d4fe;
                        min-width: 180px;
                    }
                """
        else:
            # Нет активных ETA
            text = "⏱ Макс. ETA: --:--:--"
            style = """
                QLabel {
                    font: 10pt 'Segoe UI';
                    color: #555;
                    padding: 8px 12px;
                    background-color: #f8f9fa;
                    border-radius: 6px;
                    border: 1px solid #dee2e6;
                    min-width: 180px;
                }
            """
        
        # Обновляем метку
        if self.max_eta_label:
            self.max_eta_label.setText(text)
            self.max_eta_label.setStyleSheet(style)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = VideoEncoderGUI()
    window.show()
    sys.exit(app.exec_())