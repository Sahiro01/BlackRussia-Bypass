import random
import socket
import threading
from datetime import datetime, timedelta

from core import logger

DEFAULT_HTTP_PORT = 80
DEFAULT_HTTP_TIMEOUT = 5.0

HTTP_USER_AGENT_ANDROID_VERSIONS = (12, 13, 14, 15)
HTTP_USER_AGENT_LOCALES = ("ru-RU", "en-US", "uk-UA", "kk-KZ")
HTTP_USER_AGENT_LOCALE_CHANCE = 0.85


def _generate_android_device_model() -> str:
    family = random.choices(
        (
            "samsung_a",
            "samsung_s",
            "pixel",
            "xiaomi",
            "redmi",
            "poco",
            "oneplus",
            "realme",
            "oppo",
            "vivo",
        ),
        weights=(24, 15, 10, 10, 10, 8, 7, 6, 5, 5),
        k=1,
    )[0]

    if family == "samsung_a":
        series = random.choice((13, 14, 15, 23, 24, 25, 33, 34, 35, 53, 54, 55))
        generation = random.choice((5, 6))
        region = random.choice(("B", "F", "M", "N"))
        return f"SM-A{series}{generation}{region}"

    if family == "samsung_s":
        generation = random.choice((90, 91, 92, 93))
        tier = random.choice((1, 6, 8))
        region = random.choice(("B", "U", "N"))
        return f"SM-S{generation}{tier}{region}"

    if family == "pixel":
        generation = random.randint(6, 9)
        variant = random.choice(("", "a", " Pro", " Pro XL"))
        if generation == 6 and variant == " Pro XL":
            variant = " Pro"
        return f"Pixel {generation}{variant}"

    if family == "xiaomi":
        return (
            f"{random.randint(2100000, 2509999)}"
            f"{random.choice(('G', 'I', 'C', 'Y'))}"
        )

    if family == "redmi":
        year = random.randint(22, 25)
        month = random.randint(1, 12)
        revision = random.randint(1, 99)
        return (
            f"{year:02d}{month:02d}{random.randint(10, 99):02d}"
            f"RN{revision:02d}{random.choice(('A', 'G', 'I'))}"
        )

    if family == "poco":
        series = random.choice(("M", "X", "F", "C"))
        generation = random.randint(3, 7)
        suffix = random.choice(("", " Pro", " GT"))
        return f"POCO {series}{generation}{suffix}"

    if family in {"oneplus", "oppo"}:
        return f"CPH{random.randint(2300, 2699)}"

    if family == "realme":
        return f"RMX{random.randint(3300, 3999)}"

    return f"V{random.randint(2100, 2399)}"


def _generate_android_build_id(android_version: int) -> str:
    build_profiles = {
        12: (("SP1A", "SQ3A"), datetime(2021, 8, 1), datetime(2023, 3, 31)),
        13: (("TP1A", "TQ3A"), datetime(2022, 6, 1), datetime(2024, 6, 30)),
        14: (("UP1A", "UQ1A"), datetime(2023, 10, 1), datetime(2025, 9, 30)),
        15: (("AP3A", "BP1A"), datetime(2024, 8, 1), datetime(2026, 6, 30)),
    }

    prefixes, date_from, date_to = build_profiles[int(android_version)]
    days = max(0, (date_to - date_from).days)
    build_date = date_from + timedelta(days=random.randint(0, days))
    patch = random.randint(1, 99)

    return f"{random.choice(prefixes)}.{build_date:%y%m%d}.{patch:03d}"


def generate_http_user_agent() -> str:
    android_version = random.choice(HTTP_USER_AGENT_ANDROID_VERSIONS)
    device_model = _generate_android_device_model()
    build_id = _generate_android_build_id(android_version)

    parts = [
        "Linux",
        "U",
        f"Android {android_version}",
    ]

    if random.random() < float(HTTP_USER_AGENT_LOCALE_CHANCE):
        parts.append(random.choice(HTTP_USER_AGENT_LOCALES))

    parts.append(f"{device_model} Build/{build_id}")
    return f"Dalvik/2.1.0 ({'; '.join(parts)})"


class HttpPrePing:

    def __init__(
        self,
        host: str,
        *,
        enabled: bool = True,
        port: int = DEFAULT_HTTP_PORT,
        timeout: float = DEFAULT_HTTP_TIMEOUT,
        http_host: str = "",
    ):
        self.host = str(host or "").strip()
        self.enabled = bool(enabled)
        self.port = int(port)
        self.timeout = max(0.1, float(timeout))


        self.http_host = str(http_host or "").strip()
        self._user_agent: str | None = None
        self._lock = threading.Lock()

    def user_agent(self) -> str:
        with self._lock:
            if self._user_agent is None:
                self._user_agent = generate_http_user_agent()
                logger.info(f"HTTP UA generated: {self._user_agent}")

            return self._user_agent

    def _build_request(self) -> bytes:
        host_header = self.http_host or self.host

        return (
            "GET / HTTP/1.1\r\n"
            f"User-Agent: {self.user_agent()}\r\n"
            f"Host: {host_header}\r\n"
            "Connection: Keep-Alive\r\n"
            "Accept-Encoding: gzip\r\n"
            "\r\n"
        ).encode("ascii", errors="ignore")

    def ping(self, reason: str = "startup") -> bool:
        if not self.enabled:
            return True

        if not self.host:
            logger.warn("HTTP pre-ping skipped: empty host")
            return False

        request = self._build_request()

        logger.info(
            f"HTTP pre-ping start | reason={reason} | "
            f"target={self.host}:{self.port} | "
            f"Host={self.http_host or self.host}"
        )

        try:
            with socket.create_connection(
                (self.host, self.port),
                timeout=self.timeout,
            ) as sock:
                sock.settimeout(self.timeout)
                sock.sendall(request)
                response = sock.recv(1024)

            if not response:
                logger.warn(
                    f"HTTP pre-ping failed: empty response | "
                    f"target={self.host}:{self.port}"
                )
                return False

            status_line = response.split(b"\r\n", 1)[0].decode(
                "ascii", errors="ignore"
            )

            logger.info(
                f"HTTP pre-ping done | target={self.host}:{self.port} | "
                f"bytes={len(response)} | {status_line}"
            )
            return True

        except Exception as exc:
            logger.warn(
                f"HTTP pre-ping failed | target={self.host}:{self.port} | "
                f"{exc}"
            )
            return False
