import time
import queue
import threading


_VERBOSE = True
_LOG_QUEUE = queue.Queue(maxsize=4096)
_LOG_THREAD = None
_DROPPED = 0


def _log_worker() -> None:
    global _DROPPED
    while True:
        line = _LOG_QUEUE.get()
        try:
            if line is None:
                return
            print(line, flush=True)
            if _DROPPED:
                count = _DROPPED
                _DROPPED = 0
                print(f'[{_ts()}] [WARN] Slow log output: dropped {count} log lines', flush=True)
        except (OSError, ValueError):
            pass
        finally:
            _LOG_QUEUE.task_done()


def start_async() -> None:
    global _LOG_THREAD
    if _LOG_THREAD is None:
        _LOG_THREAD = threading.Thread(target=_log_worker, name='relay-logger', daemon=True)
        _LOG_THREAD.start()


def stop_async() -> None:
    global _LOG_THREAD
    if _LOG_THREAD is not None:
        try:
            _LOG_QUEUE.put(None, timeout=0.2)
        except queue.Full:
            return
        _LOG_THREAD.join(timeout=0.5)
        if not _LOG_THREAD.is_alive():
            _LOG_THREAD = None


def _emit(line: str) -> None:
    global _DROPPED
    if _LOG_THREAD is None:
        print(line, flush=True)
        return
    try:
        _LOG_QUEUE.put_nowait(line)
    except queue.Full:
        _DROPPED += 1


def set_verbose(enabled: bool) -> None:
    global _VERBOSE
    _VERBOSE = bool(enabled)


def _ts() -> str:
    now = time.time()
    return time.strftime("%H:%M:%S", time.localtime(now)) + f".{int(now * 1000) % 1000:03d}"


def info(msg: str) -> None:
    _emit(f"[{_ts()}] [INFO] {msg}")


def warn(msg: str) -> None:
    _emit(f"[{_ts()}] [WARN] {msg}")


def error(msg: str) -> None:
    _emit(f"[{_ts()}] [ERROR] {msg}")


def debug(msg: str) -> None:
    if _VERBOSE:
        _emit(f"[{_ts()}] [DEBUG] {msg}")


def packet(direction: str, msg: str) -> None:
    if _VERBOSE:
        _emit(f"[{_ts()}] [PKT] {direction} | {msg}")
