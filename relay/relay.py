import os
import heapq
import select
import socket
import struct
import threading
import time
from collections import deque
from core import logger
from core.http_ping import HttpPrePing
from core.events import EventBus
from core.api import ScriptApi
from core.loader import load_scripts
from samp_side import parsers as samp_parsers
from samp_side.constants import (
    PacketID as SampID,
    PacketReliability as SampRel,
    RPC as SampRpc,
    PacketPriority as SampPriority,
    FIXED_SAMP_AUTH_KEY,
)
from samp_side.packets import SampDatagramBuilder, SampBodyBuilder, SampRpcBuilder
from br_side import parsers as br_parsers
from br_side.transport import BrReceiveState
from br_side.constants import (
    OutgoingPacketID as BrOutID,
    IncomingPacketID as BrInID,
    PacketReliability as BrRel,
    RPC as BrRpc,
)
from br_side.packets import (
    BrDatagramBuilder,
    BrBodyBuilder,
    BrRpcBuilder,
    InterfaceSyncPayloadBuilder,
    open_connection_request,
)


REL_SAMP_TO_BR = {
    SampRel.UNRELIABLE: BrRel.UNRELIABLE,
    SampRel.UNRELIABLE_SEQUENCED: BrRel.UNRELIABLE_SEQUENCED,
    SampRel.RELIABLE: BrRel.RELIABLE,
    SampRel.RELIABLE_ORDERED: BrRel.RELIABLE_ORDERED,
    SampRel.RELIABLE_SEQUENCED: BrRel.RELIABLE_SEQUENCED,
}

REL_BR_TO_SAMP = {v: k for k, v in REL_SAMP_TO_BR.items()}

PACKET_ID_SAMP_TO_BR = {
    SampID.CONNECTION_REQUEST: BrOutID.CONNECTION_REQUEST,
    SampID.PLAYER_SYNC: 0x1E,
    SampID.VEHICLE_SYNC: 0x22,
    SampID.PASSENGER_SYNC: 33,
    SampID.AIM_SYNC: 0x26,
    SampID.BULLET_SYNC: 0x0C,
    SampID.AUTH_KEY_RESPONSE: BrOutID.AUTH_KEY_RESPONSE,
    SampID.NEW_INCOMING_CONNECTION: BrOutID.NEW_INCOMING_CONNECTION,
    SampID.RECEIVED_STATIC_DATA: BrOutID.RECEIVED_STATIC_DATA,
    SampID.INTERNAL_PING: BrOutID.INTERNAL_PING,
    SampID.CONNECTED_PONG: BrOutID.CONNECTED_PONG,
    SampID.DISCONNECTION_NOTIFICATION: BrOutID.DISCONNECTION_NOTIFICATION,
    SampID.RPC: BrOutID.RPC,
}

PACKET_ID_BR_TO_SAMP = {
    BrInID.AUTH_KEY: SampID.AUTH_KEY,
    0x1E: SampID.PLAYER_SYNC,
    0x22: SampID.VEHICLE_SYNC,
    33: SampID.PASSENGER_SYNC,
    0x26: SampID.AIM_SYNC,
    0x0D: SampID.BULLET_SYNC,
    BrInID.CONNECTION_REQUEST_ACCEPTED: SampID.CONNECTION_REQUEST_ACCEPTED,
    BrInID.INTERNAL_PING: SampID.INTERNAL_PING,
    BrInID.CONNECTED_PONG: SampID.CONNECTED_PONG,
    BrInID.DISCONNECTION_NOTIFICATION: SampID.DISCONNECTION_NOTIFICATION,
    BrInID.RPC: SampID.RPC,
    BrInID.CONNECTION_LOST: SampID.CONNECTION_LOST,
    BrInID.CONNECTION_ATTEMPT_FAILED: SampID.CONNECTION_ATTEMPT_FAILED,
    BrInID.CONNECTION_BANNED: SampID.CONNECTION_BANNED,
    BrInID.INVALID_PASSWORD: SampID.INVALPASSWORD,
}


RPC_ID_SAMP_TO_BR = {
    getattr(SampRpc, name): getattr(BrRpc, name) for name in (
        'CLIENT_JOIN', 'REQUEST_SPAWN', 'SPAWN', 'CHAT',
        'ENTER_VEHICLE', 'EXIT_VEHICLE', 'UPDATE_SCORES_PINGS_IPS',
        'SERVER_COMMAND', 'PICKED_UP_PICKUP', 'DEATH',
    )
}


RPC_ID_SAMP_TO_BR[SampRpc.TOGGLE_SELECT_TEXT_DRAW] = BrRpc.CICK_TEXTDRAW
RPC_ID_BR_TO_SAMP = {
    getattr(BrRpc, name): getattr(SampRpc, name) for name in (
        'SERVER_JOIN', 'SERVER_QUIT', 'INIT_GAME',
        'REQUEST_SPAWN', 'CHAT', 'CLIENT_MESSAGE', 'UPDATE_SCORES_PINGS_IPS',
        'CONNECTION_REJECTED', 'WORLD_PLAYER_ADD', 'WORLD_PLAYER_REMOVE',
        'WORLD_VEHICLE_ADD', 'WORLD_VEHICLE_REMOVE', 'ENTER_VEHICLE',
        'EXIT_VEHICLE', 'SCR_DIALOG_BOX', 'SET_SPAWN_INFO', 'CHAT_BUBBLE',
    )
}

_RPC_BR_TO_SAMP_EXPLICIT = {
    BrRpc.CREATE_PICKUP: SampRpc.CREATE_PICKUP,
    BrRpc.DESTROY_PICKUP: SampRpc.DESTROY_PICKUP,
    BrRpc.SCR_CREATE_3D_TEXT_LABEL: SampRpc.CREATE_3D_TEXT_LABEL,
    BrRpc.SCR_DELETE_3D_TEXT_LABEL: SampRpc.DELETE_3D_TEXT_LABEL,
    BrRpc.SCR_SET_MAP_ICON: SampRpc.SET_MAP_ICON,
    BrRpc.SCR_DISABLE_MAP_ICON: SampRpc.REMOVE_MAP_ICON,
}


_MISSING_EXPLICIT = {
    BrRpc.SCR_SET_PLAYER_POS: SampRpc.SET_PLAYER_POS,
    BrRpc.SCR_SET_PLAYER_POS_FIND_Z: SampRpc.SET_PLAYER_POS_FIND_Z,
    BrRpc.SCR_PUT_PLAYER_IN_VEHICLE: SampRpc.PUT_PLAYER_IN_VEHICLE,
    BrRpc.SCR_SET_PLAYER_HEALTH: SampRpc.SET_PLAYER_HEALTH,
    BrRpc.SCR_SET_PLAYER_ARMOUR: SampRpc.SET_PLAYER_ARMOUR,
    BrRpc.SCR_SET_MAP_ICON: SampRpc.SET_MAP_ICON,
    BrRpc.SCR_DISABLE_MAP_ICON: SampRpc.REMOVE_MAP_ICON,
    BrRpc.SCR_SET_INTERIOR: SampRpc.SET_INTERIOR,
    BrRpc.SCR_VEHICLE_PARAMS: SampRpc.SET_VEHICLE_PARAMS_EX,
    BrRpc.SCR_SET_PLAYER_VELOCITY: SampRpc.SET_PLAYER_VELOCITY,
    BrRpc.SCR_SET_VEHICLE_VELOCITY: SampRpc.SET_VEHICLE_VELOCITY,
    BrRpc.SCR_SET_WEAPON_AMMO: SampRpc.SET_WEAPON_AMMO,
    BrRpc.SET_CHECKPOINT: SampRpc.SET_CHECKPOINT,
    BrRpc.SET_RACE_CHECKPOINT: SampRpc.SET_RACE_CHECKPOINT,
    BrRpc.DISABLE_RACE_CHECKPOINT: SampRpc.DISABLE_RACE_CHECKPOINT,
    BrRpc.SCR_DISPLAY_GAME_TEXT: SampRpc.SHOW_GAME_TEXT,
    BrRpc.SCR_TOGGLE_SELECT_TEXTDRAW: SampRpc.TOGGLE_SELECT_TEXT_DRAW,
    BrRpc.SCR_SET_CAMERA_POS: SampRpc.SET_CAMERA_POS,
    BrRpc.SCR_TOGGLE_PLAYER_CONTROLLABLE: SampRpc.TOGGLE_PLAYER_CONTROLLABLE,
}
RPC_ID_BR_TO_SAMP.update(_MISSING_EXPLICIT)
RPC_ID_BR_TO_SAMP.update(_RPC_BR_TO_SAMP_EXPLICIT)


_ACK_DEFAULT_PING_MS = 100
_ACK_MIN_PING_MS = 30
_ACK_MAX_PING_MS = 500
_ACK_PING_MULTIPLIER = 3.0
_ACK_EWMA_ALPHA = 0.125

MAX_DATAGRAMS_PER_TICK = 64
AUTH_FLOW_PACKET_DELAY = 0.5
SAMP_BATCH_DELAY = 0.005
SAMP_RESEND_INTERVAL = 2.0
BR_RESEND_INTERVAL = 2.0


class BypassRelay:

    def __init__(
        self,
        listen_host: str,
        listen_port: int,
        br_host: str,
        br_port: int,
        *,
        password: str = "",
        auth_auto: bool = True,
        reg_auto: bool = False,
        email: str = "",
        referral: str = "",
        gender: int = 0,
        skin: int = 78,
        relay_toggle_controllable: bool = True,
        damage_ignore: bool = False,
    ):
        self.listen_host = listen_host
        self.listen_port = int(listen_port)
        self.br_host = br_host
        self.br_port = int(br_port)


        self.password = str(password or "")
        self.auth_auto = bool(auth_auto)
        self.reg_auto = bool(reg_auto)
        self.email = str(email or "")
        self.referral = str(referral or "")
        self.gender = int(gender) if int(gender) in (0, 1) else 0
        self.skin = int(skin) if int(skin) >= 0 else 78

        self.relay_toggle_controllable = bool(relay_toggle_controllable)

        self.damage_ignore = bool(damage_ignore)

        self._br_textdraws: set[int] = set()
        self._br_labels: set[int] = set()

        self._br_dialog_styles: dict[int, int] = {}
        self._last_br_dialog = None

        self._samp_sock: socket.socket | None = None
        self._br_sock: socket.socket | None = None

        self._client_addr: tuple[str, int] | None = None


        self._br_server_auth_key: str | None = None


        self._br_splits: dict = {}
        self._samp_splits: dict = {}


        self._auth_state = {
            "auth_mode": None,
            "pending_step": None,
            "auth_allowed": False,
            "registration_allowed": False,
            "registration_continue_required": False,
            "auth_password_confirmed": False,
            "auth_completed": False,
            "reg_password_confirmed": False,
            "reg_referral_confirmed": False,
            "reg_gender_confirmed": False,
            "reg_skin_confirmed": False,
            "registration_completed": False,
            "_reg_after_password_queued": False,
            "_reg_after_referral_queued": False,
            "_reg_continue_gender_queued": False,
            "_reg_after_gender_queued": False,
            "_registration_finish_queued": False,
            "_auth_password_queued": False,
        }

        self._samp_builder = SampDatagramBuilder()
        self._br_builder = BrDatagramBuilder()
        self._br_receive = BrReceiveState()
        self._samp_receive = BrReceiveState({SampRel.RELIABLE, SampRel.RELIABLE_ORDERED,
                                            SampRel.RELIABLE_SEQUENCED})
        self._br_resends = {}
        self._br_auth_queue = deque()
        self._br_auth_next = 0.0
        self._auth_pending_sent = False
        self._auth_wait_since = None


        self._samp_ack_pending: set[int] = set()
        self._br_ack_pending: set[int] = set()
        self._samp_ping_ms: float = _ACK_DEFAULT_PING_MS
        self._br_ping_ms: float = _ACK_DEFAULT_PING_MS
        self._samp_has_ping: bool = False
        self._br_has_ping: bool = False
        self._samp_next_ack_time: float = time.monotonic()
        self._br_next_ack_time: float = time.monotonic()
        self._samp_pending_out: dict[int, float] = {}
        self._br_pending_out: dict[int, float] = {}
        self._samp_send_queue = []
        self._samp_queue_next = 0.0
        self._samp_queue_bytes = 0
        self._samp_resends = {}
        self._samp_queue_sequence = 0
        self._spawn_info = SampRpcBuilder.spawn_info_payload(skin=self.skin)
        self._init_game_request_queued: bool = False
        self._last_keys_1024: bool = False

        self._br_deferred_queue: deque = deque()
        self._br_deferred_next: float = 0.0


        self.events = EventBus()
        self.api = ScriptApi(self)
        load_scripts(self.api, self.events)


        self._http_pre_ping = HttpPrePing(
            self.br_host,
            enabled=True,
            port=80,
            timeout=5.0,
            http_host="",
        )

        self._running = False


    def start(self) -> None:
        self._samp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._samp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._samp_sock.bind((self.listen_host, self.listen_port))
        self._samp_sock.setblocking(False)

        self._br_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._br_sock.setblocking(False)
        self._br_sock.connect((self.br_host, self.br_port))

        self._running = True

        logger.info(
            f"BRBYPASS listening {self.listen_host}:{self.listen_port} "
            f"-> BlackRussia {self.br_host}:{self.br_port}"
        )


        threading.Thread(
            target=self._http_pre_ping.ping,
            kwargs={"reason": "bypass startup"},
            name="http-pre-ping",
            daemon=True,
        ).start()

        try:
            while self._running:
                now = time.monotonic()
                if self._samp_ack_pending and now >= self._samp_next_ack_time:
                    self._flush_samp_acks()
                if self._br_ack_pending and now >= self._br_next_ack_time:
                    self._flush_br_acks()
                self._service_outgoing()

                readable, _, _ = select.select(
                    [self._samp_sock, self._br_sock],
                    [],
                    [],
                    self._next_poll_timeout(),
                )

                for sock in readable:
                    if sock is self._samp_sock:
                        drained = self._pump_samp()
                        if drained and self._samp_ack_pending:
                            self._flush_samp_acks(force=True)
                    elif sock is self._br_sock:
                        drained = self._pump_br()
                        if drained and self._br_ack_pending:
                            self._flush_br_acks(force=True)

                self._service_outgoing()
                now = time.monotonic()
                if self._samp_ack_pending and now >= self._samp_next_ack_time:
                    self._flush_samp_acks()
                if self._br_ack_pending and now >= self._br_next_ack_time:
                    self._flush_br_acks()

        finally:
            try:
                if self._samp_ack_pending:
                    self._flush_samp_acks(force=True)
                if self._br_ack_pending:
                    self._flush_br_acks(force=True)
            except Exception:
                pass
            self.stop()

    def _service_outgoing(self) -> None:
        self._process_br_resends()
        self._process_br_deferred()
        self._process_br_auth()
        if self._auth_pending_sent and self._auth_wait_since is not None:
            if time.monotonic() - self._auth_wait_since >= 15:
                logger.warn(f'[AUTH] Waiting 15s for application response: step={self._auth_state["pending_step"]}, '
                            f'unacknowledged_transport_packets={len(self._br_resends)}')
                self._auth_wait_since = time.monotonic()
        self._process_samp_queue(force=False)
        self._process_samp_resends()
        self._flush_samp_acks()
        self._flush_br_acks()

    def _next_poll_timeout(self) -> float:
        now = time.monotonic()
        deadlines = [now + 0.02]
        if self._samp_send_queue:
            deadlines.append(self._samp_queue_next)
        if self._samp_resends:
            deadlines.append(min(item['next'] for item in self._samp_resends.values()))
        if self._br_deferred_queue:
            deadlines.append(self._br_deferred_next)
        if self._br_auth_queue:
            deadlines.append(self._br_auth_next)
        if self._br_resends:
            deadlines.append(min(item['next'] for item in self._br_resends.values()))
        if self._samp_ack_pending:
            deadlines.append(self._samp_next_ack_time)
        if self._br_ack_pending:
            deadlines.append(self._br_next_ack_time)
        return max(0.0, min(deadlines) - now)

    def stop(self) -> None:
        self._running = False

        for sock_attr in ("_samp_sock", "_br_sock"):
            sock = getattr(self, sock_attr)

            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

            setattr(self, sock_attr, None)

    def _pump_samp(self) -> bool:
        assert self._samp_sock is not None

        pump_started = time.perf_counter()
        for _ in range(MAX_DATAGRAMS_PER_TICK):
            if time.perf_counter() - pump_started >= 0.003:
                return False
            try:
                data, addr = self._samp_sock.recvfrom(4096)
            except BlockingIOError:
                return True
            except ConnectionResetError:


                if self._client_addr is not None:
                    logger.warn(
                        f"SAMP client {self._client_addr[0]}:{self._client_addr[1]} "
                        f"disconnected (connection reset), waiting for new client"
                    )
                    self._client_addr = None
                continue

            if not data:
                continue

            if addr != self._client_addr:
                logger.info(f"SAMP client connected: {addr[0]}:{addr[1]}")
                self._client_addr = addr
                self._reset_counters()

            try:
                self._handle_samp_datagram(data)
                self._service_outgoing()
            except Exception as exc:
                logger.error(f"samp datagram handling failed: {exc}")
        return False

    def _pump_br(self) -> bool:
        assert self._br_sock is not None

        pump_started = time.perf_counter()
        for _ in range(MAX_DATAGRAMS_PER_TICK):
            if time.perf_counter() - pump_started >= 0.003:
                return False
            try:
                data = self._br_sock.recv(4096)
            except BlockingIOError:
                return True
            except ConnectionResetError:
                continue

            if not data:
                continue

            try:
                self._handle_br_datagram(data)
                self._service_outgoing()
            except Exception as exc:
                logger.error(f"br datagram handling failed: {exc}")
        return False

    def _emit(self, name, *args) -> bool:

        bus = getattr(self, 'events', None)
        if bus is None:
            return False
        try:
            return bool(bus.emit(name, *args))
        except Exception as exc:
            logger.warn(f'script event {name} failed: {exc}')
            return False

    def _send_to_samp(self, datagram: bytes) -> None:
        if self._samp_sock is None or self._client_addr is None:
            return

        self._samp_sock.sendto(datagram, self._client_addr)

    def _send_to_br(self, datagram: bytes) -> None:
        if self._br_sock is None:
            return

        self._br_sock.send(datagram)

    def _reset_counters(self) -> None:
        self._samp_builder.message_number = 0
        self._br_builder.message_number = 0
        self._br_builder.ordering_indices = [0] * 16
        self._br_receive = BrReceiveState()
        self._samp_receive = BrReceiveState({SampRel.RELIABLE, SampRel.RELIABLE_ORDERED,
                                            SampRel.RELIABLE_SEQUENCED})
        self._br_resends.clear()
        self._br_auth_queue.clear()
        self._br_auth_next = 0.0
        self._auth_pending_sent = False
        self._auth_wait_since = None
        self._samp_builder.split_packet_id = 0
        self._br_builder.split_packet_id = 0
        self._br_server_auth_key = None
        self._br_splits.clear()
        self._samp_splits.clear()
        self._samp_ack_pending.clear()
        self._br_ack_pending.clear()
        self._samp_pending_out.clear()
        self._br_pending_out.clear()
        self._samp_ping_ms = _ACK_DEFAULT_PING_MS
        self._br_ping_ms = _ACK_DEFAULT_PING_MS
        self._samp_has_ping = False
        self._br_has_ping = False
        now = time.monotonic()
        self._samp_next_ack_time = now
        self._br_next_ack_time = now
        self._samp_send_queue.clear()
        self._samp_queue_next = 0.0
        self._samp_queue_bytes = 0
        self._samp_resends.clear()
        self._samp_builder.ordering_indices = [0] * 32
        self._samp_queue_sequence = 0
        self._spawn_info = SampRpcBuilder.spawn_info_payload(skin=self.skin)
        self._init_game_request_queued = False
        self._last_keys_1024 = False
        self._br_deferred_queue.clear()
        self._br_deferred_next = 0.0
        self._auth_reset_state()


    @staticmethod
    def _compressed_uint16_bits(value: int) -> int:
        value &= 0xFFFF
        if value >= 0x100:
            return 17
        if value >= 0x10:
            return 10
        return 6

    @staticmethod
    def _build_ranges(ids: list[int]) -> list[tuple[int, int]]:
        if not ids:
            return []
        ranges: list[tuple[int, int]] = []
        start = end = ids[0]
        for n in ids[1:]:
            if n == end + 1:
                end = n
            else:
                ranges.append((start, end))
                start = end = n
        ranges.append((start, end))
        return ranges

    @classmethod
    def _take_ranges_for_ack_datagram(
        cls,
        ranges: list[tuple[int, int]],
        max_bytes: int = 500,
    ) -> list[tuple[int, int]]:
        selected: list[tuple[int, int]] = []
        range_bits = 0
        for rmin, rmax in ranges:
            candidate_count = len(selected) + 1
            candidate_range_bits = range_bits + (17 if rmin == rmax else 33)
            total_bits = (
                1
                + cls._compressed_uint16_bits(candidate_count)
                + candidate_range_bits
            )
            if selected and (total_bits + 7) // 8 > max_bytes:
                break
            selected.append((rmin, rmax))
            range_bits = candidate_range_bits
        return selected

    def _ack_delay(self, ping_ms: float) -> float:
        return max(_ACK_MIN_PING_MS, ping_ms) * (_ACK_PING_MULTIPLIER / 4.0) / 1000.0

    def _update_ping_from_rtt(self, side: str, sample_ms: float) -> None:
        if sample_ms <= 0:
            return
        sample_ms = max(_ACK_MIN_PING_MS, min(_ACK_MAX_PING_MS, sample_ms))
        if side == "samp":
            if not self._samp_has_ping:
                self._samp_ping_ms = sample_ms
                self._samp_has_ping = True
            else:
                self._samp_ping_ms += (sample_ms - self._samp_ping_ms) * _ACK_EWMA_ALPHA
        else:
            if not self._br_has_ping:
                self._br_ping_ms = sample_ms
                self._br_has_ping = True
            else:
                self._br_ping_ms += (sample_ms - self._br_ping_ms) * _ACK_EWMA_ALPHA

    def _queue_samp_ack(self, msg_num: int) -> None:
        self._samp_ack_pending.add(int(msg_num) & 0xFFFF)

    def _queue_br_ack(self, msg_num: int) -> None:
        self._br_ack_pending.add(int(msg_num) & 0xFFFF)

    def _flush_samp_acks(self, force: bool = False) -> None:
        if not self._samp_ack_pending:
            return
        now = time.monotonic()
        if not force and now < self._samp_next_ack_time:
            return
        ids = sorted(self._samp_ack_pending)
        ranges = self._build_ranges(ids)
        selected_ranges = self._take_ranges_for_ack_datagram(ranges, max_bytes=500)
        if not selected_ranges:
            return
        last_selected_id = selected_ranges[-1][1]
        selected_ids = [n for n in ids if n <= last_selected_id]
        covered = sum(r[1] - r[0] + 1 for r in selected_ranges)
        if len(selected_ids) != covered:
            selected_ids = ids[:covered]
        self._samp_ack_pending.difference_update(selected_ids)
        try:
            datagram = self._samp_builder.build_ack(selected_ids)
        except ValueError as exc:
            logger.warn(f"samp ack build failed: {exc}")
            return
        self._send_to_samp(datagram)
        now = time.monotonic()
        delay = self._ack_delay(self._samp_ping_ms)
        if self._samp_ack_pending:
            self._samp_next_ack_time = now
        else:
            self._samp_next_ack_time = now + delay

    def _flush_br_acks(self, force: bool = False) -> None:
        if not self._br_ack_pending:
            return
        now = time.monotonic()
        if not force and now < self._br_next_ack_time:
            return
        ids = sorted(self._br_ack_pending)
        ranges = self._build_ranges(ids)
        selected_ranges = self._take_ranges_for_ack_datagram(ranges, max_bytes=500)
        if not selected_ranges:
            return
        last_selected_id = selected_ranges[-1][1]
        selected_ids = [n for n in ids if n <= last_selected_id]
        covered = sum(r[1] - r[0] + 1 for r in selected_ranges)
        if len(selected_ids) != covered:
            selected_ids = ids[:covered]
        self._br_ack_pending.difference_update(selected_ids)
        try:
            datagram = self._br_builder.build_ack(selected_ids)
        except ValueError as exc:
            logger.warn(f"br ack build failed: {exc}")
            return
        self._send_to_br(datagram)
        now = time.monotonic()
        delay = self._ack_delay(self._br_ping_ms)
        if self._br_ack_pending:
            self._br_next_ack_time = now
        else:
            self._br_next_ack_time = now + delay


    def _schedule_br_deferred(self, entries: list[dict], delay: float = 0.05) -> None:
        now = time.monotonic()
        was_empty = not self._br_deferred_queue
        self._br_deferred_queue.append((entries, delay))
        if was_empty:
            self._br_deferred_next = now + delay
        logger.info(f"[DEFERRED] Scheduled {len(entries)} entries, queue={len(self._br_deferred_queue)} delay={delay}")

    def _process_br_deferred(self) -> None:
        if not self._br_deferred_queue:
            return
        now = time.monotonic()
        if now < self._br_deferred_next:
            return
        entries, _ = self._br_deferred_queue[0]
        late_ms = max(0.0, (now - self._br_deferred_next) * 1000)
        try:
            if not self._send_entries_to_br(entries):
                self._br_deferred_next = now + 0.02
                return
            self._br_deferred_queue.popleft()
            logger.debug(f'[DEFERRED] Sent {len(entries)} entries; scheduler_late_ms={late_ms:.1f}')
        except Exception as exc:

            self._br_deferred_queue.popleft()
            logger.warn(f"deferred send failed: {exc}")
        if self._br_deferred_queue:
            _, next_delay = self._br_deferred_queue[0]
            self._br_deferred_next = now + next_delay
        else:
            self._br_deferred_next = 0.0


    def _auth_reset_state(self) -> None:
        s = self._auth_state
        s.update({
            "auth_mode": None,
            "pending_step": None,
            "auth_allowed": False,
            "registration_allowed": False,
            "registration_continue_required": False,
            "auth_password_confirmed": False,
            "auth_completed": False,
            "reg_password_confirmed": False,
            "reg_referral_confirmed": False,
            "reg_gender_confirmed": False,
            "reg_skin_confirmed": False,
            "registration_completed": False,
            "_reg_after_password_queued": False,
            "_reg_after_referral_queued": False,
            "_reg_continue_gender_queued": False,
            "_reg_after_gender_queued": False,
            "_registration_finish_queued": False,
            "_auth_password_queued": False,
        })

    def _auth_set_pending(self, step: str) -> None:
        self._auth_state["pending_step"] = step
        self._auth_pending_sent = False
        logger.info(f"[AUTH] Pending step: {step}")

    def _auth_confirm_pending(self) -> str | None:
        s = self._auth_state
        step = s.get("pending_step")
        if step is None or not self._auth_pending_sent:
            logger.info('[AUTH] Ignoring confirmation: no password/registration step sent yet')
            return None
        self._auth_pending_sent = False
        self._auth_wait_since = None

        s["pending_step"] = None

        if step == "auth_password":
            s["auth_password_confirmed"] = True
            s["auth_completed"] = True
            logger.info("[AUTH] Auth password confirmed вЂ” completed")
        elif step == "reg_password":
            s["reg_password_confirmed"] = True
            logger.info("[AUTH] Registration password confirmed")
        elif step == "reg_referral":
            s["reg_referral_confirmed"] = True
            logger.info("[AUTH] Referral confirmed")
        elif step == "reg_gender":
            s["reg_gender_confirmed"] = True
            logger.info("[AUTH] Gender confirmed")
        elif step == "reg_skin":
            s["reg_skin_confirmed"] = True
            logger.info("[AUTH] Skin confirmed вЂ” need finish {\"c\":1}")
        return step

    def _send_br_interface(self, packet_name: str, builder_fn, *args, **kwargs) -> None:
        try:
            inner = builder_fn(*args, **kwargs)
            body = BrBodyBuilder.create(BrOutID.USER_INTERFACE_SYNC, inner)
        except (ValueError, TypeError) as exc:
            logger.warn(f'[AUTH] {packet_name} build failed: {exc}')
            raise
        step = {
            'interface_auth_password': 'auth_password',
            'interface_register_password': 'reg_password',
            'interface_register_referral': 'reg_referral',
            'interface_register_gender': 'reg_gender',
        }.get(packet_name)
        was_empty = not self._br_auth_queue
        self._br_auth_queue.append((packet_name, step, {
            'body': body, 'reliability': BrRel.RELIABLE_ORDERED, 'ordering_channel': 0,
        }))
        if was_empty:
            self._br_auth_next = time.monotonic() + AUTH_FLOW_PACKET_DELAY
        logger.info(f'[AUTH] Queued {packet_name}; spacing={AUTH_FLOW_PACKET_DELAY}s')

    def _process_br_auth(self) -> None:
        if not self._br_auth_queue or time.monotonic() < self._br_auth_next:
            return
        name, step, entry = self._br_auth_queue[0]
        if not self._send_entries_to_br([entry]):
            self._br_auth_next = time.monotonic() + AUTH_FLOW_PACKET_DELAY
            return
        self._br_auth_queue.popleft()
        if step is not None and self._auth_state['pending_step'] == step:
            self._auth_pending_sent = True
            self._auth_wait_since = time.monotonic()
        if name == 'interface_register_skin':
            self._auth_state['reg_skin_confirmed'] = True
        elif name == 'interface_register_finish':
            self._auth_state.update(registration_completed=True, auth_completed=True,
                                    auth_allowed=False, registration_allowed=False,
                                    registration_continue_required=False)
        logger.info(f'[AUTH] Submitted {name} to reliable transport')
        self._br_auth_next = (time.monotonic() + AUTH_FLOW_PACKET_DELAY
                              if self._br_auth_queue else 0.0)

    def _queue_samp_entry(self, entry: dict, priority: int) -> None:
        if not self._samp_send_queue:
            self._samp_queue_next = time.monotonic() + SAMP_BATCH_DELAY
        self._samp_queue_sequence += 1
        self._samp_queue_bytes += len(entry['body']) + 12
        heapq.heappush(self._samp_send_queue,
                       (priority, self._samp_queue_sequence, entry))

    def _process_samp_queue(self, force: bool = True) -> None:
        if not self._samp_send_queue:
            return
        if not force and self._samp_queue_bytes < 500 and time.monotonic() < self._samp_queue_next:
            return
        entries = []
        for _ in range(min(256, len(self._samp_send_queue))):
            _, _, entry = heapq.heappop(self._samp_send_queue)
            self._samp_queue_bytes -= len(entry['body']) + 12
            entries.append(entry)
        self._send_entries_to_samp(entries)
        if not self._samp_send_queue:
            self._samp_queue_bytes = 0
        self._samp_queue_next = time.monotonic() if self._samp_send_queue else 0.0

    def _queue_samp_class_response(self) -> None:

        self._queue_samp_entry({
            'body': SampRpcBuilder.rpc_body(SampRpcBuilder.create(
                SampRpc.REQUEST_CLASS, b'\x01' + self._spawn_info)),
            'reliability': SampRel.RELIABLE,
            'ordering_channel': 0,
            'local_class_response': True,
        }, SampPriority.MEDIUM_PRIORITY)

    def _queue_init_game_response(self) -> None:

        if self._init_game_request_queued:
            return
        payload, bitlen = BrRpcBuilder.request_class_payload(0)
        self._schedule_br_deferred([{
            'body': BrRpcBuilder.rpc_body(BrRpcBuilder.create(
                BrRpc.REQUEST_CLASS, payload, bitlen)),
            'reliability': BrRel.RELIABLE,
            'ordering_channel': 0,
        }, {
            'body': BrBodyBuilder.create(BrOutID.USER_INTERFACE_SYNC,
                                         InterfaceSyncPayloadBuilder.appmetrica()),
            'reliability': BrRel.RELIABLE,
            'ordering_channel': 0,
        }], delay=0.0)
        self._init_game_request_queued = True
        logger.info('[AUTH] INIT_GAME received -> queued BR RequestClass(0) + AppMetrica')

    def _auth_handle_interface_sync(self, parsed: dict) -> None:
        state = parsed.get("state")
        s = self._auth_state

        if state == "auth_allowed":
            if s["auth_mode"] == "auth" and s["_auth_password_queued"]:
                return
            s.update({
                "auth_mode": "auth",
                "auth_allowed": True,
                "registration_allowed": False,
                "registration_continue_required": False,
                "pending_step": None,
            })
            logger.info("[AUTH] Server: auth_allowed")
            if self.auth_auto and self.password and not s["_auth_password_queued"]:
                s["_auth_password_queued"] = True
                self._auth_set_pending("auth_password")
                self._send_br_interface(
                    "interface_auth_password",
                    InterfaceSyncPayloadBuilder.auth_password,
                    self.password,
                )
            elif not self.auth_auto or not self.password:
                logger.warn('[AUTH] Login not started: auth_auto disabled or password empty')
            return

        if state == "registration_allowed":
            if s["auth_mode"] == "registration" and (
                s["pending_step"] is not None or s["reg_password_confirmed"]
            ):
                return
            s.update({
                "auth_mode": "registration",
                "registration_allowed": True,
                "auth_allowed": False,
                "registration_continue_required": False,
                "pending_step": None,
            })
            logger.info("[AUTH] Server: registration_allowed")
            if self.reg_auto and self.password and not s["reg_password_confirmed"]:
                self._auth_set_pending("reg_password")
                self._send_br_interface(
                    "interface_register_password",
                    InterfaceSyncPayloadBuilder.register_password,
                    self.password,
                    self.email,
                )
            elif not self.reg_auto or not self.password:
                logger.warn('[AUTH] Registration not started: reg_auto disabled or password empty')
            return

        if state == "ack_auth":
            confirmed = self._auth_confirm_pending()
            logger.info(f"[AUTH] Server ACK t:0 confirmed={confirmed}")
            self._auth_maybe_next_registration_step()
            return

        if state == "registration_continue_required":
            if s['registration_continue_required']:
                return
            if s['pending_step'] == 'auth_password' and not self._auth_pending_sent:
                logger.warn('[AUTH] Ignoring t:3 before queued password was sent')
                return
            self._auth_pending_sent = False
            s.update({
                "auth_mode": "registration_continue",
                "auth_allowed": False,
                "registration_allowed": False,
                "registration_continue_required": True,
            })
            if s["pending_step"] == "auth_password":
                s["auth_password_confirmed"] = True
            s["pending_step"] = None
            logger.info("[AUTH] Server: registration_continue_required (t:3)")
            self._auth_maybe_next_registration_step()
            return

    def _auth_maybe_next_registration_step(self) -> None:
        s = self._auth_state
        if not self.reg_auto:
            return
        if s["registration_completed"]:
            return
        if s["pending_step"] is not None:
            return


        if s["registration_allowed"] and s["reg_password_confirmed"] and not s["reg_referral_confirmed"]:
            if s["_reg_after_password_queued"]:
                return
            s["_reg_after_password_queued"] = True
            self._send_br_interface(
                "interface_register_unknown_after_password",
                InterfaceSyncPayloadBuilder.register_unknown_after_password,
            )

            self._auth_set_pending("reg_referral")
            self._send_br_interface(
                "interface_register_referral",
                InterfaceSyncPayloadBuilder.register_referral,
                self.referral,
            )
            return


        if s["registration_allowed"] and s["reg_referral_confirmed"] and not s["reg_gender_confirmed"]:
            if s["_reg_after_referral_queued"]:
                return
            s["_reg_after_referral_queued"] = True
            self._auth_set_pending("reg_gender")
            self._send_br_interface(
                "interface_register_gender",
                InterfaceSyncPayloadBuilder.register_gender,
                self.gender,
            )
            return


        if s["registration_continue_required"] and not s["reg_gender_confirmed"]:
            if s["_reg_continue_gender_queued"]:
                return
            s["_reg_continue_gender_queued"] = True
            self._auth_set_pending("reg_gender")
            self._send_br_interface(
                "interface_register_gender",
                InterfaceSyncPayloadBuilder.register_gender,
                self.gender,
            )
            return


        if (s["registration_allowed"] or s["registration_continue_required"]) and s["reg_gender_confirmed"] and not s["reg_skin_confirmed"]:
            if s["_reg_after_gender_queued"]:
                return
            s["_reg_after_gender_queued"] = True

            self._send_br_interface(
                "interface_register_skin",
                lambda: InterfaceSyncPayloadBuilder.register_skin(self.skin),
            )

            if not s["_registration_finish_queued"]:
                s["_registration_finish_queued"] = True
                self._send_br_interface(
                    "interface_register_finish",
                    InterfaceSyncPayloadBuilder.register_finish,
                )
                logger.info("[AUTH] Registration skin and finish queued")
            return


        if (s["registration_allowed"] or s["registration_continue_required"]) and s["reg_skin_confirmed"] and not s["registration_completed"] and not s["_registration_finish_queued"]:
            s["_registration_finish_queued"] = True
            self._send_br_interface(
                "interface_register_finish",
                InterfaceSyncPayloadBuilder.register_finish,
            )
            logger.info("[AUTH] Registration finish queued")
            return


    def _handle_samp_datagram(self, data: bytes) -> None:


        if data[:4] == b'SAMP':
            logger.packet("SMP>BR", f"SAMP-magic passthrough ({len(data)}B raw)")
            self._send_to_br(data)
            return


        events = samp_parsers.parse_datagram(
            data,
            decrypt_port=self.listen_port,
            split_store=self._samp_splits,
            receive_state=self._samp_receive,
        )

        for event in events:
            if event.get("kind") in ("packet", "fragment", "duplicate") and event.get("reliability") in (
                SampRel.RELIABLE,
                SampRel.RELIABLE_ORDERED,
                SampRel.RELIABLE_SEQUENCED,
            ):
                self._queue_samp_ack(event["message_number"])

        for event in events:
            kind = event.get("kind")

            if kind == 'ack':
                self._handle_samp_ack(event['ranges'])
            elif kind == "offline":
                self._samp_offline_to_br(event)

            elif kind == "fragment":
                pass

            elif kind == "packet":
                if self._emit("OnPacketSAMPReceive", event):
                    continue
                translated = self._translate_samp_packet(event)

                if translated and not self._emit("OnSendPacketBR", translated):
                    self._send_entries_to_br(translated)

            elif kind == "raw":
                logger.debug(
                    f"SMP>BR raw {len(data)}B dropped: {data[:16].hex()}"
                )

    def _samp_offline_to_br(self, event: dict) -> None:
        packet_id = event["packet_id"]

        if packet_id == SampID.OPEN_CONNECTION_REQUEST:
            parsed = samp_parsers.parse_open_connection_request(
                event["body"]
            )
            cookie = parsed["cookie"] if parsed else None

            logger.info(
                "OPEN_CONNECTION_REQUEST "
                + (f"cookie={cookie}" if cookie is not None else "(short)")
                + " -> sending BR OPEN_CONNECTION_REQUEST"
            )

            self._reset_counters()
            self._send_to_br(open_connection_request())
            return

        logger.debug(
            f"samp offline id={packet_id} ignored "
            f"(only OPEN_CONNECTION_REQUEST expected from client)"
        )

    def _translate_samp_packet(self, event: dict) -> list[dict]:

        body = event["body"]

        if not body:
            return []

        packet_id = body[0]
        payload = body[1:]
        name = samp_parsers.PACKET_BODY_NAMES.get(packet_id, f"id_{packet_id}")

        br_reliability = REL_SAMP_TO_BR.get(
            event["reliability"],
            BrRel.RELIABLE,
        )
        br_channel = (event.get("ordering_channel") or 0) & 0x0F


        if packet_id == SampID.OPEN_CONNECTION_REQUEST:
            return []

        if packet_id == SampID.CONNECTION_REQUEST:
            logger.info("CONNECTION_REQUEST -> BR CONNECTION_REQUEST")
            return [{
                "body": BrBodyBuilder.connection_request(),
                "reliability": BrRel.RELIABLE,
                "ordering_channel": 0,
            }]

        if packet_id == SampID.AUTH_KEY_RESPONSE:
            parsed = samp_parsers.parse_auth_key_response(payload)

            if parsed is None:
                logger.warn("bad samp AUTH_KEY_RESPONSE body")
                return []

            br_key = self._br_server_auth_key

            if not br_key:
                logger.warn(
                    "AUTH_KEY_RESPONSE from client before BR AUTH_KEY вЂ” "
                    "dropped"
                )
                return []

            logger.info(
                f"AUTH_KEY_RESPONSE <- client "
                f"(key={parsed['key'][:12]}..., ignored) -> "
                f"BR response derived from server key {br_key[:12]}..."
            )

            try:


                br_body = BrBodyBuilder.auth_key_response(br_key)
            except ValueError as exc:
                logger.error(f"auth key derivation failed: {exc}")
                return []

            return [{
                "body": br_body,
                "reliability": BrRel.RELIABLE,
                "ordering_channel": 0,
            }]

        if packet_id == SampID.NEW_INCOMING_CONNECTION:
            logger.info(
                "NEW_INCOMING_CONNECTION -> BR "
                f"(rewritten to {self.br_host}:{self.br_port})"
            )

            return [{
                "body": BrBodyBuilder.new_incoming_connection(
                    self.br_host,
                    self.br_port,
                ),
                "reliability": BrRel.RELIABLE,
                "ordering_channel": 0,
            }]

        if packet_id == SampID.RPC:
            return self._translate_samp_rpc(
                payload,
                br_reliability,
                br_channel,
            )

        if packet_id == SampID.DETECT_LOST_CONNECTIONS:
            logger.debug("DETECT_LOST_CONNECTIONS dropped (no BR analog)")
            return []

        if packet_id == SampID.RECEIVED_STATIC_DATA:

            return [{
                "body": bytes([BrOutID.RECEIVED_STATIC_DATA]),
                "reliability": BrRel.UNRELIABLE,
                "encrypt": True,
                "ordering_channel": 0,
            }]


        if packet_id in (SampID.PLAYER_SYNC, SampID.VEHICLE_SYNC, SampID.PASSENGER_SYNC, SampID.AIM_SYNC, SampID.BULLET_SYNC):
            try:
                from core.bitstream import BitStream as _BSS
                br_payload = payload
                if packet_id == SampID.PLAYER_SYNC:
                    _bs = _BSS(payload)
                    _lr = _bs.read_int16(); _ud = _bs.read_int16(); _keys = _bs.read_uint16()
                    _x = _bs.read_float(); _y = _bs.read_float(); _z = _bs.read_float()
                    _qw = _bs.read_float(); _qx = _bs.read_float(); _qy = _bs.read_float(); _qz = _bs.read_float()
                    _h = _bs.read_uint8(); _a = _bs.read_uint8()
                    _add = _bs.read_bits(2); _wp = _bs.read_bits(6)
                    _spec = _bs.read_uint8()
                    _bs.read_uint8(); _bs.read_uint8()
                    _vx = _bs.read_float(); _vy = _bs.read_float(); _vz = _bs.read_float()
                    _sx = _bs.read_float(); _sy = _bs.read_float(); _sz = _bs.read_float()
                    _surf = _bs.read_uint16()
                    _anim = _bs.read_int32() if _bs.remaining_bits >= 32 else 0
                    _anim2 = _bs.read_int16() if _bs.remaining_bits >= 16 else 0
                    _bs2 = _BSS()
                    _bs2.write_int16(_lr); _bs2.write_int16(_ud); _bs2.write_uint16(_keys)
                    _bs2.write_float(_x); _bs2.write_float(_y); _bs2.write_float(_z)
                    _bs2.write_float(_qw); _bs2.write_float(_qx); _bs2.write_float(_qy); _bs2.write_float(_qz)
                    _bs2.write_uint16(_h); _bs2.write_uint16(_a)
                    _bs2.write_uint8(_wp & 0x3F); _bs2.write_uint8(_spec)
                    _bs2.write_float(_vx); _bs2.write_float(_vy); _bs2.write_float(_vz)
                    _bs2.write_float(_sx); _bs2.write_float(_sy); _bs2.write_float(_sz)
                    _bs2.write_uint16(_surf)
                    _bs2.write_int32(_anim)
                    _bs2.write_int16(_anim2)
                    br_payload = _bs2.get_bytes()
                elif packet_id == SampID.VEHICLE_SYNC:
                    try:
                        _bs = _BSS(payload)
                        _vid = _bs.read_uint16(); _lr = _bs.read_uint16(); _ud = _bs.read_uint16(); _keys = _bs.read_uint16()
                        _qw = _bs.read_float(); _qx = _bs.read_float(); _qy = _bs.read_float(); _qz = _bs.read_float()
                        _x = _bs.read_float(); _y = _bs.read_float(); _z = _bs.read_float()
                        _vx = _bs.read_float(); _vy = _bs.read_float(); _vz = _bs.read_float()
                        _vh = _bs.read_uint16()
                        _ph = _bs.read_uint8(); _pa = _bs.read_uint8()
                        _rest = _bs.read_bytes(_bs.remaining_bits//8)
                        _bs2 = _BSS()
                        _bs2.write_uint16(_vid); _bs2.write_uint16(_lr); _bs2.write_uint16(_ud); _bs2.write_uint16(_keys)
                        _bs2.write_float(_qw); _bs2.write_float(_qx); _bs2.write_float(_qy); _bs2.write_float(_qz)
                        _bs2.write_float(_x); _bs2.write_float(_y); _bs2.write_float(_z)
                        _bs2.write_float(_vx); _bs2.write_float(_vy); _bs2.write_float(_vz)
                        _bs2.write_uint16(_vh)
                        _bs2.write_uint16(_ph); _bs2.write_uint16(_pa)
                        _bs2.write_bytes(_rest)
                        br_payload = _bs2.get_bytes()
                    except:
                        br_payload = payload
                elif packet_id == SampID.PASSENGER_SYNC:

                    _bs = _BSS(payload)
                    _vid = _bs.read_uint16()
                    _db = _bs.read_bits(2)
                    _seat = _bs.read_bits(6)
                    _add = _bs.read_bits(2)
                    _wp = _bs.read_bits(6)
                    _h = _bs.read_uint8()
                    _a = _bs.read_uint8()
                    _lr = _bs.read_uint16()
                    _ud = _bs.read_uint16()
                    _keys = _bs.read_uint16()
                    _x = _bs.read_float()
                    _y = _bs.read_float()
                    _z = _bs.read_float()
                    _bs2 = _BSS()
                    _bs2.write_uint16(_vid)
                    _bs2.write_bits(_db, 2)
                    _bs2.write_bits(_seat, 6)
                    _bs2.write_bits(_add, 2)
                    _bs2.write_bits(_wp, 6)
                    _bs2.write_uint16(_h)
                    _bs2.write_uint16(_a)
                    _bs2.write_uint16(_lr)
                    _bs2.write_uint16(_ud)
                    _bs2.write_uint16(_keys)
                    _bs2.write_float(_x)
                    _bs2.write_float(_y)
                    _bs2.write_float(_z)
                    br_payload = _bs2.get_bytes()
                br_packet_id = PACKET_ID_SAMP_TO_BR.get(packet_id)
                entries = [{"body": bytes([br_packet_id]) + br_payload, "reliability": br_reliability, "ordering_channel": br_channel}]

                if packet_id == SampID.PLAYER_SYNC and len(payload) >= 6:
                    try:
                        _keys = struct.unpack_from('<H', payload, 4)[0]
                        if _keys & 1024:
                            if not getattr(self, '_last_keys_1024', False):
                                self._last_keys_1024 = True
                                _interact = BrRpcBuilder.create(BrRpc.PICKED_UP_INTERACT, struct.pack('<i', 18))
                                _body_i = BrBodyBuilder.create(BrOutID.RPC, _interact)
                                entries.append({"body": _body_i, "reliability": BrRel.RELIABLE, "ordering_channel": 0})
                                logger.info(f"SMP>BR key 1024 (keys={_keys}) -> {BrRpc.PICKED_UP_INTERACT} pickup=18")
                        else:
                            if getattr(self, '_last_keys_1024', False):
                                self._last_keys_1024 = False
                    except Exception as _ke:
                        logger.debug(f"key 1024 check failed: {_ke}")
                return entries
            except Exception as _e:
                logger.warn(f"sync convert SAMP->BR failed {packet_id}: {_e}")
        br_packet_id = PACKET_ID_SAMP_TO_BR.get(packet_id)

        if br_packet_id is None:
            logger.packet(
                "SMP>BR",
                f"{name}: no BR mapping, dropped",
            )
            return []

        logger.packet(
            "SMP>BR",
            f"{name} ({len(payload)}B payload)",
        )

        return [{
            "body": bytes([br_packet_id]) + payload,
            "reliability": br_reliability,
            "ordering_channel": br_channel,
        }]

    def _translate_samp_rpc(
        self,
        rpc_frame: bytes,
        br_reliability: int,
        br_channel: int,
    ) -> list[dict]:
        parsed = samp_parsers.read_rpc(rpc_frame)

        if parsed is None:
            logger.warn("bad samp RPC frame")
            return []

        samp_rpc_id = parsed["rpc_id"]
        rpc_payload = parsed["payload"]
        samp_name = self._samp_rpc_name(samp_rpc_id)

        if self._emit("OnRpcSAMPReceive", samp_rpc_id, rpc_payload):
            return []

        if samp_rpc_id == SampRpc.REQUEST_CLASS:
            self._queue_samp_class_response()
            logger.debug('SAMP RequestClass -> queued local server response')
            return []

        payload_bitlen = parsed['payload_bitlen']
        try:
            if samp_rpc_id in (SampRpc.SPAWN, SampRpc.REQUEST_SPAWN,
                                  SampRpc.UPDATE_SCORES_PINGS_IPS):
                if payload_bitlen:
                    raise ValueError('client RPC must have an empty payload')
                if samp_rpc_id == SampRpc.SPAWN:
                    rpc_payload = BrRpcBuilder.spawn_payload()
                elif samp_rpc_id == SampRpc.REQUEST_SPAWN:
                    rpc_payload = BrRpcBuilder.request_spawn_payload()
            elif samp_rpc_id in (SampRpc.CHAT, SampRpc.SERVER_COMMAND):
                prefix = 1 if samp_rpc_id == SampRpc.CHAT else 4
                if len(rpc_payload) < prefix or payload_bitlen != len(rpc_payload) * 8:
                    raise ValueError('truncated text RPC')
                length = int.from_bytes(rpc_payload[:prefix], 'little')
                if len(rpc_payload) != prefix + length:
                    raise ValueError('invalid text RPC length')
                text = rpc_payload[prefix:].decode('cp1251')
                if text.startswith('/'):
                    _cmd = text[1:].split(' ', 1)[0].strip().lower() if len(text) > 1 else ''
                    if self._emit("OnCommand", _cmd, text):
                        return []
                if text.strip().lower() == '/y':
                    _call = getattr(self, '_last_call', None)
                    if _call is not None:
                        import json as _js_y
                        _jb_y = _js_y.dumps({'t': int(_call.get('t', 2)), 'b': 1}, separators=(',', ':')).encode('utf-8')
                        from core.bitstream import BitStream as _BSy
                        from br_side.packets import BrBodyBuilder as _BBBy
                        from br_side.constants import OutgoingPacketID as _BOy
                        _bsy = _BSy()
                        _bsy.write_uint16(65)
                        _bsy.write_uint32(len(_jb_y))
                        _bsy.write_bytes(_jb_y)
                        _body_y = _BBBy.create(_BOy.USER_INTERFACE_SYNC, _bsy.get_bytes())
                        logger.info("SMP>BR /y -> call accept")
                        self._last_call = None
                        return [{"body": _body_y, "reliability": br_reliability, "ordering_channel": br_channel}]
                build = (BrRpcBuilder.chat_payload if prefix == 1
                         else BrRpcBuilder.server_command_payload)
                rpc_payload = build(text)
                payload_bitlen = len(rpc_payload) * 8
            elif samp_rpc_id in (SampRpc.DEATH, SampRpc.PICKED_UP_PICKUP,
                                 SampRpc.ENTER_VEHICLE, SampRpc.EXIT_VEHICLE):
                size = {SampRpc.DEATH: 3, SampRpc.PICKED_UP_PICKUP: 4,
                        SampRpc.ENTER_VEHICLE: 3, SampRpc.EXIT_VEHICLE: 2}[samp_rpc_id]
                if payload_bitlen != size * 8:
                    raise ValueError('invalid client RPC size')
        except (ValueError, UnicodeError) as exc:
            logger.warn(f'invalid SAMP {samp_name}: {exc}')
            return []


        if samp_rpc_id == SampRpc.DIALOG_RESPONSE:

            try:
                from core.bitstream import BitStream as _BSs2
                _tmp = _BSs2(rpc_payload)
                _tmp_id = _tmp.read_uint16()
                if _tmp_id == 1002 and getattr(self, '_last_spawn_select', None):
                    _tmp_resp = _tmp.read_uint8()
                    _tmp_item = _tmp.read_uint16()
                    if _tmp_resp == 0:
                        _spawns = self._last_spawn_select
                        _info = "\n".join([s["place_name"] for s in _spawns])
                        samp_payload_s = SampRpcBuilder.dialog_box_payload(1002, 2, "Выберите место спавна", "Выбрать", "", _info)
                        self._queue_samp_entry({'body': SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.SCR_DIALOG_BOX, samp_payload_s)), 'reliability': SampRel.RELIABLE, 'ordering_channel': 0}, SampPriority.MEDIUM_PRIORITY)
                        return []
                    _spawns = self._last_spawn_select
                    if not (0 <= _tmp_item < len(_spawns)):
                        return []
                    _tval = int(_spawns[_tmp_item].get('t', _tmp_item))
                    import json as _js_sp
                    _jb_sp = _js_sp.dumps({'t': _tval}, separators=(',', ':')).encode('utf-8')
                    from core.bitstream import BitStream as _BSsp
                    _bssp = _BSsp()
                    _bssp.write_uint16(50)
                    _bssp.write_uint32(len(_jb_sp))
                    _bssp.write_bytes(_jb_sp)
                    from br_side.packets import BrBodyBuilder as _BBBsp
                    from br_side.constants import OutgoingPacketID as _BOutsp
                    _body_sp = _BBBsp.create(_BOutsp.USER_INTERFACE_SYNC, _bssp.get_bytes())
                    logger.info(f"SMP>BR spawn select item={_tmp_item} t={_tval}")
                    self._last_spawn_select = None
                    return [{"body": _body_sp, "reliability": br_reliability, "ordering_channel": br_channel}]
            except Exception as _es:
                logger.warn(f"spawn response failed {_es}")
            try:
                from core.bitstream import BitStream as _BS2
                _bs2 = _BS2(rpc_payload)
                _dlg_id = _bs2.read_uint16()
                _resp = _bs2.read_uint8()
                _item = _bs2.read_uint16()
                if _item == 0xFFFF:
                    _item = -1
                _tlen = _bs2.read_uint8()
                _txt = _bs2.read_string(_tlen, 'cp1251', errors='ignore') if _tlen else ''
                if getattr(self, '_last_npc_dialog', None) and _dlg_id == 1001:
                    try:
                        _dlg = self._last_npc_dialog
                        _btns = _dlg.get('buttons', [])
                        if _dlg.get('single'):
                            _b0 = _btns[0] if _btns else None
                            _bk = int(_b0.get('bk', 0)) if isinstance(_b0, dict) else 0
                        else:
                            _idx = int(_item) - int(_dlg.get('offset', 0))
                            if not (isinstance(_btns, list) and 0 <= _idx < len(_btns)):
                                return []
                            _bx = _btns[_idx]
                            _bk = int(_bx.get('bk', 0)) if isinstance(_bx, dict) else 0
                        import json as _json_npc
                        _j_npc = {"bk": _bk}
                        _jb_npc = _json_npc.dumps(_j_npc, separators=(',', ':')).encode('utf-8')
                        from core.bitstream import BitStream as _BSn
                        _bsn = _BSn()
                        _bsn.write_uint16(63)
                        _bsn.write_uint32(len(_jb_npc))
                        _bsn.write_bytes(_jb_npc)
                        from br_side.packets import BrBodyBuilder as _BBB_n
                        from br_side.constants import OutgoingPacketID as _BOut_n
                        _body_n = _BBB_n.create(_BOut_n.USER_INTERFACE_SYNC, _bsn.get_bytes())
                        logger.info(f"SMP>BR NPC dialog response item={_item} bk={_bk}")
                        self._last_npc_dialog = None
                        return [{"body": _body_n, "reliability": br_reliability, "ordering_channel": br_channel}]
                    except Exception as _en:
                        logger.warn(f"npc response failed {_en}")
                import json as _json2


                _dlg_style = None
                try:
                    _styles = getattr(self, '_br_dialog_styles', None)
                    if isinstance(_styles, dict) and int(_dlg_id) in _styles:
                        _dlg_style = int(_styles[int(_dlg_id)])
                    else:
                        _last = getattr(self, '_last_br_dialog', None)
                        if isinstance(_last, dict) and 'style' in _last:
                            _dlg_style = int(_last['style'])
                except (TypeError, ValueError):
                    _dlg_style = None


                _j = {"r": int(_resp), "i": str(_txt), "l": int(_item)}
                _jbytes = _json2.dumps(_j, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
                from br_side.packets import BrBodyBuilder as _BBB2
                from br_side.constants import OutgoingPacketID as _BOut2
                _inner = _jbytes

                from core.bitstream import BitStream as _BS3
                _bs3 = _BS3()
                _bs3.write_uint16(10)
                _bs3.write_uint32(len(_jbytes))
                _bs3.write_bytes(_jbytes)
                _body2 = _BBB2.create(_BOut2.USER_INTERFACE_SYNC, _bs3.get_bytes())
                logger.packet("SMP>BR", f"rpc DIALOG_RESPONSE id={_dlg_id} style={_dlg_style} resp={_resp} item={_item} txt={_txt[:20]!r} -> BR JSON")
                return [{"body": _body2, "reliability": br_reliability, "ordering_channel": br_channel}]
            except Exception as exc2:
                logger.warn(f"dialog response convert failed {exc2} payload={rpc_payload[:32].hex()}")
        if samp_rpc_id == SampRpc.CLIENT_JOIN:
            joined = samp_parsers.parse_client_join(rpc_payload)
            nickname = (
                joined["nickname"] if joined else "Player"
            )

            br_payload, _bitlen = BrRpcBuilder.client_join_payload(nickname)

            logger.info(
                f"RPC CLIENT_JOIN nick='{nickname}' -> BR CLIENT_JOIN "
                f"(id {BrRpc.CLIENT_JOIN})"
            )

            return [{
                "body": BrRpcBuilder.rpc_body(
                    BrRpcBuilder.create(BrRpc.CLIENT_JOIN, br_payload)
                ),
                "reliability": BrRel.RELIABLE_ORDERED,
                "ordering_channel": 0,
            }]

        br_rpc_id = RPC_ID_SAMP_TO_BR.get(samp_rpc_id)

        if br_rpc_id is None:
            logger.packet(
                "SMP>BR",
                f"rpc {samp_name}({samp_rpc_id}): no mapping, dropped",
            )
            return []

        logger.packet(
            "SMP>BR",
            f"rpc {samp_name} -> br id {br_rpc_id} "
            f"({len(rpc_payload)}B payload passed through)",
        )

        br_reliability = (BrRel.RELIABLE_ORDERED if samp_rpc_id == SampRpc.SPAWN else
                          BrRel.RELIABLE_SEQUENCED if samp_rpc_id in (SampRpc.ENTER_VEHICLE, SampRpc.EXIT_VEHICLE)
                          else BrRel.RELIABLE)
        br_channel = 0
        return [{
            "body": BrRpcBuilder.rpc_body(
                BrRpcBuilder.create(br_rpc_id, rpc_payload, payload_bitlen)
            ),
            "reliability": br_reliability,
            "ordering_channel": br_channel,
        }]

    @staticmethod
    def _samp_rpc_name(rpc_id: int) -> str:
        for name, value in vars(SampRpc).items():
            if name.isupper() and value == rpc_id:
                return name
        return f"rpc_{rpc_id}"

    @staticmethod
    def _br_rpc_name(rpc_id: int) -> str:
        for name, value in vars(BrRpc).items():
            if name.isupper() and value == rpc_id:
                return name
        return f"rpc_{rpc_id}"

    def _send_entries_to_br(self, entries: list[dict]) -> bool:
        if self._br_sock is None:
            return False

        for entry in entries:
            start_number = self._br_builder.message_number
            datagrams = self._br_builder.build_datagrams([entry])
            reliable = entry.get('reliability', BrRel.RELIABLE) in (
                BrRel.RELIABLE, BrRel.RELIABLE_ORDERED, BrRel.RELIABLE_SEQUENCED)
            for offset, datagram in enumerate(datagrams):
                number = (start_number + offset) & 0xffff
                if reliable:
                    if number in self._br_resends:
                        raise RuntimeError('BR reliable message number still unacknowledged; reconnect required')
                    self._br_resends[number] = {
                        'data': datagram, 'next': time.monotonic() + BR_RESEND_INTERVAL,
                        'sent': time.monotonic(), 'retries': 0,
                    }
                try:
                    self._send_to_br(datagram)
                except OSError as exc:
                    if not reliable:
                        raise
                    logger.warn(f'BR reliable send retained for retry: {exc}')
        return True

    def _process_br_resends(self) -> None:
        if self._br_sock is None:
            return
        now = time.monotonic()
        for number, item in list(self._br_resends.items()):
            if now < item['next']:
                continue
            try:
                self._send_to_br(item['data'])
            except OSError as exc:
                logger.warn(f'BR resend failed msg={number}: {exc}')
            item['retries'] += 1
            item['next'] = now + BR_RESEND_INTERVAL
            if item['retries'] in (1, 5, 15):
                logger.warn(f'BR waiting for transport ACK msg={number}, retries={item["retries"]}')

    def _handle_br_ack(self, ranges) -> None:
        now = time.monotonic()
        for number in list(self._br_resends):
            if any(first <= number <= last for first, last in ranges):
                item = self._br_resends.pop(number)
                if not item['retries']:
                    self._update_ping_from_rtt('br', (now - item['sent']) * 1000)


    def _handle_br_datagram(self, data: bytes) -> None:

        if data[:4] == b'SAMP':
            logger.packet("BR>SMP", f"SAMP-magic passthrough ({len(data)}B raw)")
            self._send_to_samp(data)
            return
        events = br_parsers.parse_datagram(
            data,
            split_store=self._br_splits,
            receive_state=self._br_receive,
        )

        for event in events:
            if event.get("kind") in ("packet", "fragment", "duplicate") and event.get("reliability") in (
                BrRel.RELIABLE,
                BrRel.RELIABLE_ORDERED,
                BrRel.RELIABLE_SEQUENCED,
            ):
                self._queue_br_ack(event["message_number"])

        for event in events:
            kind = event.get("kind")

            if kind == "ack":
                self._handle_br_ack(event['ranges'])
            elif kind == "offline":
                self._br_offline_to_samp(event)

            elif kind == "fragment":
                pass

            elif kind == "packet":
                for delivered in self._br_receive.deliver(event):
                    if self._emit("OnPacketBRReceive", delivered):
                        continue
                    translated = self._translate_br_packet(delivered)
                    self._process_br_deferred()
                    if translated and not self._emit("OnSendPacketSAMP", translated):
                        for entry in translated:
                            self._queue_samp_entry(entry, SampPriority.HIGH_PRIORITY)

            elif kind == "raw":
                logger.debug(
                    f"BR>SMP raw {len(data)}B dropped: {data[:16].hex()}"
                )

    def _br_offline_to_samp(self, event: dict) -> None:
        packet_id = event["packet_id"]

        if packet_id == BrInID.OPEN_CONNECTION_REPLY:
            logger.info("OPEN_CONNECTION_REPLY <- BR, -> samp client")

            self._send_to_samp(bytes([SampID.OPEN_CONNECTION_REPLY]))
            return

        logger.debug(f"br offline id={packet_id} ignored")

    def _translate_br_packet(self, event: dict) -> list[dict]:
        body = event["body"]

        if not body:
            return []

        packet_id = body[0]
        payload = body[1:]
        name = br_parsers.PACKET_BODY_NAMES.get(packet_id, f"id_{packet_id}")

        if packet_id in (BrInID.DISCONNECTION_NOTIFICATION, BrInID.CONNECTION_LOST,
                          BrInID.CONNECTION_ATTEMPT_FAILED, BrInID.CONNECTION_BANNED,
                          BrInID.INVALID_PASSWORD):
            self._br_resends.clear()
            self._br_auth_queue.clear()
            self._br_deferred_queue.clear()
            self._auth_pending_sent = False
            self._auth_wait_since = None
            logger.warn(f'[BR] Connection ended/rejected: packet_id={packet_id}; auth sends cancelled')

        samp_reliability = REL_BR_TO_SAMP.get(
            event["reliability"],
            SampRel.RELIABLE,
        )
        samp_channel = (event.get("ordering_channel") or 0) & 0x1F


        if packet_id == BrInID.USER_INTERFACE_SYNC:

            try:
                from core.bitstream import BitStream as _BS
                import json as _json
                _bs = _BS(payload)
                if _bs.remaining_bits >= 48:
                    _gid = _bs.read_uint16()
                    _jlen = _bs.read_uint32()
                    if _jlen <= _bs.remaining_bits // 8:
                        _jbytes = _bs.read_bytes(_jlen)
                        try:
                            _j = _json.loads(_jbytes.decode('utf-8', errors='ignore'))
                            if isinstance(_j, dict) and _gid == 10 and 'c' in _j:
                                try:
                                    style = int(_j.get('i', 0))
                                except (TypeError, ValueError):
                                    style = 0
                                title = str(_j.get('c', ''))
                                info = str(_j.get('s', ''))
                                btn1 = str(_j.get('l', ''))
                                btn2 = str(_j.get('r', ''))
                                dlg_id = 0
                                self._last_br_dialog = {'id': dlg_id, 'style': style}
                                try:
                                    self._br_dialog_styles[dlg_id] = style
                                except (TypeError, ValueError):
                                    pass
                                try:
                                    samp_payload = SampRpcBuilder.dialog_box_payload(dlg_id, style, title, btn1, btn2, info)
                                except Exception as _be:
                                    logger.warn(f"DIALOG build failed style={style} title={title[:30]!r} info_len={len(info)}: {_be}")
                                    return []
                                logger.packet("BR>SMP", f"rpc DIALOG JSON->SAMP id={dlg_id} style={style} title={title[:20]!r} keys={sorted(_j.keys())} raw={_jbytes[:200]!r}")
                                return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.SCR_DIALOG_BOX, samp_payload)), "reliability": samp_reliability, "ordering_channel": samp_channel}]
                            elif isinstance(_j, dict) and _gid == 10:
                                logger.warn(f"DIALOG gui=10 unexpected shape keys={sorted(_j.keys())} raw={_jbytes[:160]!r}")
                        except Exception as _e:
                            logger.warn(f"DIALOG gui=10 parse failed: {_e}")
            except Exception:
                pass

            try:
                from core.bitstream import BitStream as _BSi
                import json as _jsi
                _bsi = _BSi(payload)
                if _bsi.remaining_bits >= 48:
                    _gid2 = _bsi.read_uint16()
                    _jlen2 = _bsi.read_uint32()
                    if _jlen2 <= _bsi.remaining_bits // 8:
                        _jb2 = _bsi.read_bytes(_jlen2)
                        try:
                            _jd = _jsi.loads(_jb2.decode('utf-8', errors='ignore'))
                            if isinstance(_jd, dict):

                                if _gid2 == 76 and _jd.get('o') == 1:
                                    name = str(_jd.get('ur', 'unknown'))
                                    msg = f"Запустилась катсцена {name} введите /skip чтобы скипнуть ее"
                                    samp_payload = SampRpcBuilder.client_message_payload(0xFF0000FF, msg)
                                    self._queue_samp_entry({'body': SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.CLIENT_MESSAGE, samp_payload)), 'reliability': SampRel.RELIABLE, 'ordering_channel': 0}, SampPriority.MEDIUM_PRIORITY)

                                    self._last_cinematic = name
                                    logger.info(f"Cinematic started {name}")
                                    return []

                                if _gid2 == 65 and _jd.get('o') == 1:
                                    hdr = str(_jd.get('h','')); txt = str(_jd.get('s','')); btn = str(_jd.get('b',''))
                                    try:
                                        _ct = int(_jd.get('t', 2))
                                    except:
                                        _ct = 2
                                    self._last_call = {'t': _ct}
                                    msg = f"[CALL] {hdr}: {txt} [{btn}] — принять: /y"
                                    samp_payload = SampRpcBuilder.client_message_payload(0xFFAA00FF, msg)
                                    self._queue_samp_entry({'body': SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.CLIENT_MESSAGE, samp_payload)), 'reliability': SampRel.RELIABLE, 'ordering_channel': 0}, SampPriority.MEDIUM_PRIORITY)
                                    return []

                                if _gid2 == 13 and _jd.get('o') == 1:
                                    txt = str(_jd.get('i',''))
                                    msg = f"[NOTIFICATION] {txt}"
                                    samp_payload = SampRpcBuilder.client_message_payload(0xFFFF00FF, msg)
                                    self._queue_samp_entry({'body': SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.CLIENT_MESSAGE, samp_payload)), 'reliability': SampRel.RELIABLE, 'ordering_channel': 0}, SampPriority.MEDIUM_PRIORITY)
                                    return []

                                if _gid2 == 39 and _jd.get('o') == 1:
                                    qtext = str(_jd.get('mq',''))
                                    msg = f"[QUEST] {qtext}"
                                    samp_payload = SampRpcBuilder.client_message_payload(0x00FF00FF, msg)
                                    self._queue_samp_entry({'body': SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.CLIENT_MESSAGE, samp_payload)), 'reliability': SampRel.RELIABLE, 'ordering_channel': 0}, SampPriority.MEDIUM_PRIORITY)
                                    return []

                                if _gid2 == 63 and _jd.get('o') == 1:
                                    nname = str(_jd.get('n','NPC'))
                                    ntext = str(_jd.get('d',''))
                                    raw_btns = _jd.get('b', [])
                                    _rbtns = [b for b in raw_btns if isinstance(b, dict)] if isinstance(raw_btns, list) else []
                                    btns = [str(b.get('bn','')) for b in _rbtns]
                                    if len(btns) <= 1:

                                        _btn = btns[0] if btns else "OK"
                                        self._last_npc_dialog = {'buttons': _rbtns, 'single': True}
                                        samp_payload2 = SampRpcBuilder.dialog_box_payload(1001, 0, nname, _btn, "Закрыть", ntext)
                                        self._queue_samp_entry({'body': SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.SCR_DIALOG_BOX, samp_payload2)), 'reliability': SampRel.RELIABLE, 'ordering_channel': 0}, SampPriority.MEDIUM_PRIORITY)
                                        logger.info(f"NPC dialog {nname} -> SAMP (single)")
                                        return []
                                    _rows = ntext.split('\n') if ntext else []
                                    _rows_off = len(_rows)
                                    _rows.extend([f"{i+1}. {b}" for i, b in enumerate(btns)])
                                    info = "\n".join(_rows)
                                    btn1 = btns[0]
                                    btn2 = btns[1]

                                    self._last_npc_dialog = {'buttons': _rbtns, 'offset': _rows_off}
                                    samp_payload2 = SampRpcBuilder.dialog_box_payload(1001, 2, nname, btn1, btn2, info)
                                    self._queue_samp_entry({'body': SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.SCR_DIALOG_BOX, samp_payload2)), 'reliability': SampRel.RELIABLE, 'ordering_channel': 0}, SampPriority.MEDIUM_PRIORITY)
                                    logger.info(f"NPC dialog {nname} -> SAMP")
                                    return []


                                if _gid2 == 50:
                                    if not (isinstance(_jd, dict) and isinstance(_jd.get('m'), list) and len(_jd['m']) > 0):
                                        return []
                                    try:
                                        _known_spawns = {0: (2, "Фракция"), 1: (0, "Вокзал"), 2: (1, "Последнее место"), 3: (3, "Гараж"), 4: (4, "Дом"), 5: (6, "Семейный гараж"), 6: (7, "Семейный дом"), 7: (5, "Яхта")}
                                        _spawns = []
                                        for _s in _jd['m']:
                                            if isinstance(_s, dict):
                                                try:
                                                    _sid = int(_s.get('id', _s.get('place', 0)))
                                                except:
                                                    continue
                                                _kp = _known_spawns.get(_sid)
                                                _pname = str(_s.get('place_name', _s.get('name', ''))) or (_kp[1] if _kp else f"Спавн {_sid}")
                                                _spawns.append({"id": _sid, "place_name": _pname, "t": _sid})
                                            else:
                                                try:
                                                    _sid = int(_s)
                                                except:
                                                    continue
                                                _kp = _known_spawns.get(_sid, (None, None))
                                                _pname = _kp[1] if _kp[1] else f"Спавн {_sid}"
                                                _spawns.append({"id": _sid, "place_name": _pname, "t": _sid})
                                        if not _spawns:
                                            return []
                                        _info = "\n".join([s["place_name"] for s in _spawns])
                                        self._last_spawn_select = _spawns
                                        samp_payload_s = SampRpcBuilder.dialog_box_payload(1002, 2, "Выберите место спавна", "Выбрать", "", _info)
                                        self._queue_samp_entry({'body': SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.SCR_DIALOG_BOX, samp_payload_s)), 'reliability': SampRel.RELIABLE, 'ordering_channel': 0}, SampPriority.MEDIUM_PRIORITY)
                                        logger.info(f"Spawn select -> SAMP dialog 1002 with {len(_spawns)} spawns")
                                        return []
                                    except Exception as _se:
                                        logger.warn(f"spawn select failed {_se}")
                        except Exception as _e2:
                            pass
            except Exception:
                pass
            parsed = br_parsers.parse_interface_sync(payload)
            if parsed is not None:
                if parsed.get("state") in ("auth_allowed","registration_allowed","ack_auth","registration_continue_required",):
                    self._auth_handle_interface_sync(parsed)
                    return []
                logger.debug(f"BR interface_sync ignored: {parsed}")
            return []

        if packet_id == BrInID.AUTH_KEY:
            parsed = br_parsers.parse_auth_key(payload)

            if parsed is None:
                logger.warn(
                    f"bad BR AUTH_KEY body: {len(payload)}B "
                    f"{payload[:16].hex()}"
                )
                return []


            self._br_server_auth_key = parsed["key"]


            logger.info(
                f"AUTH_KEY <- BR len={parsed['key_len']} "
                f"key={parsed['key'][:16]}... | "
                f"-> client fixed key {FIXED_SAMP_AUTH_KEY}"
            )

            return [{
                "body": SampBodyBuilder.auth_key_body(FIXED_SAMP_AUTH_KEY),
                "reliability": SampRel.RELIABLE,
                "ordering_channel": 0,
            }]

        if packet_id == BrInID.CONNECTION_REQUEST_ACCEPTED:
            return self._synth_samp_accepted(samp_reliability)

        if packet_id == BrInID.RPC:
            return self._translate_br_rpc(payload, samp_reliability, samp_channel)


        if packet_id == 0x1E:
            try:
                from core.bitstream import BitStream as _BSy
                _bs = _BSy(payload)
                _o = _BSy()
                _o.write_uint16(_bs.read_uint16())
                for _i in range(2):
                    _hb = _bs.read_bool()
                    _o.write_bool(_hb)
                    if _hb:
                        _o.write_uint16(_bs.read_uint16())
                _o.write_uint16(_bs.read_uint16())
                for _i in range(3):
                    _o.write_float(_bs.read_float())
                for _i in range(4):
                    _o.write_bool(_bs.read_bool())
                for _i in range(3):
                    _o.write_uint16(_bs.read_uint16())
                _hh = float(_bs.read_uint16())
                _aa = float(_bs.read_uint16())
                _o.write_bits(0xF if _hh >= 100.0 else (0 if _hh <= 0.0 else min(14, max(1, int(_hh / 7.0)))), 4)
                _o.write_bits(0xF if _aa >= 100.0 else (0 if _aa <= 0.0 else min(14, max(1, int(_aa / 7.0)))), 4)
                _o.write_uint8(_bs.read_uint8())
                _o.write_uint8(_bs.read_uint8())
                _vm = _bs.read_float()
                _o.write_float(_vm)
                if _vm != 0.0:
                    for _i in range(3):
                        _o.write_uint16(_bs.read_uint16())
                _hs = _bs.read_bool()
                _o.write_bool(_hs)
                if _hs:
                    _o.write_uint16(_bs.read_uint16())
                    for _i in range(3):
                        _o.write_float(_bs.read_float())
                _ha = _bs.read_bool()
                _o.write_bool(_ha)
                if _ha:
                    _o.write_bits(_bs.read_bits(32), 32)
                return [{"body": bytes([SampID.PLAYER_SYNC]) + _o.get_bytes(), "reliability": samp_reliability, "ordering_channel": samp_channel}]
            except Exception as _e:
                logger.warn(f"BR player sync convert failed: {_e}")
        if packet_id == 0x22:
            try:
                from core.bitstream import BitStream as _BSv
                _bs = _BSv(payload)
                _o = _BSv()
                _o.write_uint16(_bs.read_uint16())
                for _i in range(4):
                    _o.write_uint16(_bs.read_uint16())
                for _i in range(4):
                    _o.write_bool(_bs.read_bool())
                for _i in range(3):
                    _o.write_uint16(_bs.read_uint16())
                for _i in range(3):
                    _o.write_float(_bs.read_float())
                _vm = _bs.read_float()
                _o.write_float(_vm)
                if _vm != 0.0:
                    for _i in range(3):
                        _o.write_uint16(_bs.read_uint16())
                _o.write_uint16(_bs.read_uint16())
                _vh = float(_bs.read_uint16())
                _va = float(_bs.read_uint16())
                _o.write_bits(0xF if _vh >= 100.0 else (0 if _vh <= 0.0 else min(14, max(1, int(_vh / 7.0)))), 4)
                _o.write_bits(0xF if _va >= 100.0 else (0 if _va <= 0.0 else min(14, max(1, int(_va / 7.0)))), 4)
                _o.write_uint8(_bs.read_uint8())
                _o.write_bool(_bs.read_bool())
                _o.write_bool(_bs.read_bool())
                _o.write_bool(False)
                _o.write_bool(False)
                _o.write_bool(False)
                return [{"body": bytes([SampID.VEHICLE_SYNC]) + _o.get_bytes(), "reliability": samp_reliability, "ordering_channel": samp_channel}]
            except Exception as _e:
                logger.warn(f"BR vehicle sync convert failed: {_e}")

        if packet_id == 33:
            try:
                from core.bitstream import BitStream as _BSp
                _bs = _BSp(payload)
                _o = _BSp()
                _o.write_uint16(_bs.read_uint16())
                _o.write_uint16(_bs.read_uint16())
                _o.write_bits(_bs.read_bits(2), 2)
                _o.write_bits(_bs.read_bits(6), 6)
                _o.write_bits(_bs.read_bits(2), 2)
                _o.write_bits(_bs.read_bits(6), 6)
                _o.write_uint8(min(255, _bs.read_uint16()))
                _o.write_uint8(min(255, _bs.read_uint16()))
                _o.write_uint16(_bs.read_uint16())
                _o.write_uint16(_bs.read_uint16())
                _o.write_uint16(_bs.read_uint16())
                _o.write_float(_bs.read_float())
                _o.write_float(_bs.read_float())
                _o.write_float(_bs.read_float())
                return [{"body": bytes([SampID.PASSENGER_SYNC]) + _o.get_bytes(), "reliability": samp_reliability, "ordering_channel": samp_channel}]
            except Exception as _e:
                logger.warn(f"BR passenger sync convert failed: {_e}")

        if packet_id in (0x1E, 0x22, 0x26, 0x0C):
            samp_packet_id = PACKET_ID_BR_TO_SAMP.get(packet_id)
            if samp_packet_id is None:

                samp_packet_id = packet_id


            return [{"body": bytes([samp_packet_id]) + payload, "reliability": samp_reliability, "ordering_channel": samp_channel}]
        samp_packet_id = PACKET_ID_BR_TO_SAMP.get(packet_id)

        if samp_packet_id is None:
            logger.packet(
                "BR>SMP",
                f"{name}: no SAMP mapping, dropped",
            )
            return []

        logger.packet(
            "BR>SMP",
            f"{name} ({len(payload)}B payload)",
        )

        return [{
            "body": bytes([samp_packet_id]) + payload,
            "reliability": samp_reliability,
            "ordering_channel": samp_channel,
        }]

    def _synth_samp_accepted(self, samp_reliability: int) -> list[dict]:


        client_ip = self._client_addr[0] if self._client_addr else "127.0.0.1"
        client_port = self._client_addr[1] if self._client_addr else 0

        binary_address = struct.unpack(
            "<I",
            socket.inet_aton(client_ip),
        )[0]
        server_challenge = struct.unpack("<I", os.urandom(4))[0]

        logger.info(
            "CONNECTION_REQUEST_ACCEPTED <- BR, -> samp "
            f"player_index=0 challenge={server_challenge:#x}"
        )

        return [{
            "body": SampBodyBuilder.connection_request_accepted(
                binary_address,
                client_port,
                0,
                server_challenge,
            ),
            "reliability": SampRel.RELIABLE,
            "ordering_channel": 0,
        }]

    def _translate_br_rpc(
        self,
        rpc_frame: bytes,
        samp_reliability: int,
        samp_channel: int,
    ) -> list[dict]:
        parsed = br_parsers.read_rpc(rpc_frame)

        if parsed is None:
            logger.warn("bad BR RPC frame")
            return []

        br_rpc_id = parsed["rpc_id"]
        rpc_payload = parsed["payload"]
        br_name = self._br_rpc_name(br_rpc_id)

        if br_rpc_id == BrRpc.SCR_TOGGLE_PLAYER_CONTROLLABLE and not self.relay_toggle_controllable:
            logger.packet(
                "BR>SMP",
                f"rpc {br_name}({br_rpc_id}): dropped by config toggle_player_controllable=false",
            )
            return []

        if self.damage_ignore and br_rpc_id in (BrRpc.SCR_SET_PLAYER_HEALTH, BrRpc.SCR_SET_PLAYER_ARMOUR):
            try:
                _hp = struct.unpack_from('<f', rpc_payload, 0)[0] if len(rpc_payload) >= 4 else 100.0
            except Exception:
                _hp = 100.0
            if _hp < 100.0:
                logger.packet(
                    "BR>SMP",
                    f"rpc {br_name}({br_rpc_id}): dropped by config damage_ignore (value={_hp:.1f})",
                )
                return []


        if self._emit("OnRpcBRReceive", br_rpc_id, rpc_payload):
            return []
        if br_rpc_id == BrRpc.INIT_GAME:
            self._queue_init_game_response()

        if br_rpc_id == BrRpc.SERVER_JOIN:
            joined = br_parsers.parse_server_join(rpc_payload)
            if joined is None:
                logger.warn('invalid BR ServerJoin payload')
                return []
            return [{
                'body': SampRpcBuilder.rpc_body(SampRpcBuilder.server_join(**joined)),
                'reliability': samp_reliability,
                'ordering_channel': samp_channel,
            }]


        if br_rpc_id == BrRpc.CLIENT_MESSAGE:
            msg = br_parsers.parse_client_message(rpc_payload)

            if msg is None:
                logger.warn("bad BR CLIENT_MESSAGE payload")
                return []

            samp_payload = SampRpcBuilder.client_message_payload(
                msg["color"],
                msg["message"],
            )

            logger.packet(
                "BR>SMP",
                f"rpc CLIENT_MESSAGE utf8->cp1251: {msg['message'][:40]!r}",
            )

            return [{
                "body": SampRpcBuilder.rpc_body(
                    SampRpcBuilder.create(
                        SampRpc.CLIENT_MESSAGE,
                        samp_payload,
                    )
                ),
                "reliability": samp_reliability,
                "ordering_channel": samp_channel,
            }]


        if br_rpc_id == BrRpc.SCR_DIALOG_BOX:
            dlg = br_parsers.parse_show_dialog(rpc_payload)

            if dlg is None:
                logger.warn("bad BR SCR_DIALOG_BOX payload")
                return []

            samp_payload = SampRpcBuilder.dialog_box_payload(
                dlg["dialog_id"],
                dlg["dialog_style"],
                dlg["dialog_title"],
                dlg["dialog_button1"],
                dlg["dialog_button2"],
                dlg["dialog_info"],
            )
            try:
                self._br_dialog_styles[int(dlg["dialog_id"])] = int(dlg["dialog_style"])
                self._last_br_dialog = {'id': int(dlg["dialog_id"]), 'style': int(dlg["dialog_style"])}
            except (TypeError, ValueError, KeyError):
                pass

            logger.packet(
                "BR>SMP",
                f"rpc DIALOG_BOX id={dlg['dialog_id']} "
                f"title={dlg['dialog_title']!r} utf8->cp1251",
            )

            return [{
                "body": SampRpcBuilder.rpc_body(
                    SampRpcBuilder.create(
                        SampRpc.SCR_DIALOG_BOX,
                        samp_payload,
                    )
                ),
                "reliability": samp_reliability,
                "ordering_channel": samp_channel,
            }]


        if br_rpc_id == BrRpc.SCR_CREATE_3D_TEXT_LABEL:
            try:
                from core.bitstream import BitStream
                bs = BitStream(rpc_payload)
                label_id = bs.read_uint16()
                color = bs.read_uint32()
                x = bs.read_float(); y = bs.read_float(); z = bs.read_float()
                draw = bs.read_float()
                use_los = bs.read_uint8()
                player = bs.read_uint16(); vehicle = bs.read_uint16()
                if bs.remaining_bits >= 32:
                    try:
                        text_len = bs.read_uint32()
                        if text_len <= bs.remaining_bits // 8:
                            text_br = bs.read_string(text_len, 'utf-8', errors='ignore')
                        else:
                            raise ValueError('len overflow')
                    except:
                        bs2tmp = BitStream(rpc_payload)
                        bs2tmp.read_uint16(); bs2tmp.read_uint32(); bs2tmp.read_float(); bs2tmp.read_float(); bs2tmp.read_float(); bs2tmp.read_float(); bs2tmp.read_uint8(); bs2tmp.read_uint16(); bs2tmp.read_uint16()
                        text_br = bs2tmp.read_compressed_string(4096, encoding='utf-8')
                else:
                    text_br = ''
                bs2 = BitStream()
                bs2.write_uint16(label_id)
                bs2.write_uint32(color)
                bs2.write_float(x); bs2.write_float(y); bs2.write_float(z)
                bs2.write_float(draw)
                bs2.write_uint8(use_los)
                bs2.write_uint16(player); bs2.write_uint16(vehicle)
                bs2.write_compressed_string(text_br, 'cp1251')
                samp_payload = bs2.get_bytes()
                self._br_labels.add(label_id)
                return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.CREATE_3D_TEXT_LABEL, samp_payload)), "reliability": samp_reliability, "ordering_channel": samp_channel}]
            except Exception as exc:
                logger.warn(f"3D text convert failed {exc}, fallback")


        if br_rpc_id == BrRpc.SCR_GIVE_PLAYER_MONEY:
            if len(rpc_payload) == 4:

                return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.GIVE_PLAYER_MONEY, rpc_payload, parsed["payload_bitlen"])), "reliability": samp_reliability, "ordering_channel": samp_channel}]
            else:

                return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.REMOVE_PLAYER_FROM_VEHICLE, rpc_payload, parsed["payload_bitlen"])), "reliability": samp_reliability, "ordering_channel": samp_channel}]

        if br_rpc_id == BrRpc.SCR_VEHICLE_PARAMS:
            if len(rpc_payload) >= 3:
                return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.SET_VEHICLE_PARAMS_EX, rpc_payload, parsed["payload_bitlen"])), "reliability": samp_reliability, "ordering_channel": samp_channel}]


        if br_rpc_id == BrRpc.SCR_APPLY_ANIMATION:
            if len(rpc_payload) <= 1:
                return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.CONNECTION_REJECTED, rpc_payload, parsed["payload_bitlen"])), "reliability": samp_reliability, "ordering_channel": samp_channel}]
            return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.APPLY_PLAYER_ANIMATION, rpc_payload, parsed["payload_bitlen"])), "reliability": samp_reliability, "ordering_channel": samp_channel}]


        if br_rpc_id == BrRpc.SCR_SET_MAP_ICON and len(rpc_payload) >= 20:
            try:
                from core.bitstream import BitStream as _BSm
                _bs = _BSm(rpc_payload)
                _mid = _bs.read_uint16()
                _mx = _bs.read_float()
                _my = _bs.read_float()
                _mz = _bs.read_float()
                _mtp = _bs.read_uint8()
                _mc = _bs.read_uint32()
                _ms = _bs.read_uint8()
                _o = _BSm()
                _o.write_uint8(_mid & 0xFF)
                _o.write_float(_mx)
                _o.write_float(_my)
                _o.write_float(_mz)
                _o.write_uint8(_mtp)
                _o.write_uint32(_mc)
                _o.write_uint8(_ms)
                return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.SET_MAP_ICON, _o.get_bytes())), "reliability": samp_reliability, "ordering_channel": samp_channel}]
            except Exception as _e:
                logger.warn(f"BR set_map_icon convert failed: {_e}")


        if br_rpc_id == BrRpc.SCR_GIVE_PLAYER_WEAPON:
            if len(rpc_payload) != 8:
                logger.warn(f"BR give_weapon bad size: {len(rpc_payload)}B, dropped")
                return []
            return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.GIVE_PLAYER_WEAPON, rpc_payload, parsed["payload_bitlen"])), "reliability": samp_reliability, "ordering_channel": samp_channel}]


        if br_rpc_id == BrRpc.SCR_SHOW_TEXT_DRAW:
            try:
                if len(rpc_payload) < 67:
                    raise ValueError(f'truncated ShowTextDraw ({len(rpc_payload)}B)')
                _tlen = struct.unpack_from('<H', rpc_payload, 65)[0]
                if 65 + 2 + _tlen != len(rpc_payload):
                    raise ValueError(f'text len mismatch ({_tlen} vs {len(rpc_payload)}B)')
                _td_id = struct.unpack_from('<H', rpc_payload, 0)[0]
                _txt = rpc_payload[67:67 + _tlen].decode('utf-8', errors='ignore')
                _enc = _txt.encode('cp1251', errors='ignore')
                samp_payload = rpc_payload[:65] + struct.pack('<H', len(_enc)) + _enc
                self._br_textdraws.add(_td_id)
                logger.packet(
                    "BR>SMP",
                    f"rpc SHOW_TEXT_DRAW id={_td_id} utf8->cp1251 ({len(rpc_payload)}B->{len(samp_payload)}B)",
                )
                return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.SHOW_TEXT_DRAW, samp_payload)), "reliability": samp_reliability, "ordering_channel": samp_channel}]
            except Exception as _e:
                logger.warn(f"BR show_text_draw convert failed: {_e}")
                return []

        if br_rpc_id == BrRpc.SCR_TEXT_DRAW_SET_STRING:
            try:
                if len(rpc_payload) < 4:
                    raise ValueError(f'truncated TextDrawSetString ({len(rpc_payload)}B)')
                _tlen = struct.unpack_from('<H', rpc_payload, 2)[0]
                if 2 + 2 + _tlen != len(rpc_payload):
                    raise ValueError(f'text len mismatch ({_tlen} vs {len(rpc_payload)}B)')
                _td_id = struct.unpack_from('<H', rpc_payload, 0)[0]
                _txt = rpc_payload[4:4 + _tlen].decode('utf-8', errors='ignore')
                _enc = _txt.encode('cp1251', errors='ignore')
                samp_payload = rpc_payload[:2] + struct.pack('<H', len(_enc)) + _enc
                self._br_textdraws.add(_td_id)
                logger.packet(
                    "BR>SMP",
                    f"rpc TEXT_DRAW_SET_STRING id={_td_id} utf8->cp1251 ({len(rpc_payload)}B->{len(samp_payload)}B)",
                )
                return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.TEXT_DRAW_SET_STRING, samp_payload)), "reliability": samp_reliability, "ordering_channel": samp_channel}]
            except Exception as _e:
                logger.warn(f"BR text_draw_set_string convert failed: {_e}")
                return []

        if br_rpc_id == BrRpc.SCR_HIDE_TEXT_DRAW:
            try:
                if len(rpc_payload) != 2:
                    raise ValueError(f'expected 2 bytes, got {len(rpc_payload)}B')
                _item = struct.unpack_from('<H', rpc_payload, 0)[0]
                self._br_textdraws.discard(_item)
                return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.HIDE_TEXT_DRAW, rpc_payload, parsed["payload_bitlen"])), "reliability": samp_reliability, "ordering_channel": samp_channel}]
            except Exception as _e:
                logger.warn(f"BR hide_text_draw convert failed: {_e}")
                return []

        if br_rpc_id == BrRpc.SCR_DELETE_3D_TEXT_LABEL:
            try:
                if len(rpc_payload) != 2:
                    raise ValueError(f'expected 2 bytes, got {len(rpc_payload)}B')
                _item = struct.unpack_from('<H', rpc_payload, 0)[0]
                self._br_labels.discard(_item)
                return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.DELETE_3D_TEXT_LABEL, rpc_payload, parsed["payload_bitlen"])), "reliability": samp_reliability, "ordering_channel": samp_channel}]
            except Exception as _e:
                logger.warn(f"BR delete_3d_text_label convert failed: {_e}")
                return []


        if br_rpc_id == BrRpc.CHAT_BUBBLE:
            try:
                if len(rpc_payload) < 15:
                    raise ValueError(f'truncated ChatBubble ({len(rpc_payload)}B)')
                _tlen = rpc_payload[14]
                if 15 + _tlen != len(rpc_payload):
                    raise ValueError(f'text len mismatch ({_tlen} vs {len(rpc_payload)}B)')
                _txt = rpc_payload[15:15 + _tlen].decode('utf-8', errors='ignore')
                _enc = _txt.encode('cp1251', errors='ignore')[:255]
                samp_payload = rpc_payload[:14] + bytes([len(_enc)]) + _enc
                logger.packet(
                    "BR>SMP",
                    f"rpc CHAT_BUBBLE utf8->cp1251 ({len(rpc_payload)}B->{len(samp_payload)}B)",
                )
                return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.CHAT_BUBBLE, samp_payload)), "reliability": samp_reliability, "ordering_channel": samp_channel}]
            except Exception as _e:
                logger.warn(f"BR chat_bubble convert failed: {_e}, passthrough")


        if br_rpc_id == BrRpc.DISABLE_CHECKPOINT and parsed["payload_bitlen"] == 0:
            return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.DISABLE_CHECKPOINT, b"")), "reliability": samp_reliability, "ordering_channel": samp_channel}]

        if br_rpc_id == BrRpc.WORLD_PLAYER_ADD:
            try:
                from core.bitstream import BitStream as _BSa
                _bs = _BSa(rpc_payload)
                _pid = _bs.read_uint16()
                _skin = _bs.read_uint32()
                _x = _bs.read_float()
                _y = _bs.read_float()
                _z = _bs.read_float()
                _rot = _bs.read_float()
                _fs = _bs.read_uint8()
                _bs.read_float()
                _bs.read_float()
                _o = _BSa()
                _o.write_uint16(_pid)
                _o.write_uint8(0)
                _o.write_uint32(_skin)
                _o.write_float(_x)
                _o.write_float(_y)
                _o.write_float(_z)
                _o.write_float(_rot)
                _o.write_uint32(0xFFFFFFFF)
                _o.write_uint8(_fs)
                return [{"body": SampRpcBuilder.rpc_body(SampRpcBuilder.create(SampRpc.WORLD_PLAYER_ADD, _o.get_bytes())), "reliability": SampRel.RELIABLE_ORDERED, "ordering_channel": 0}]
            except Exception as _e:
                logger.warn(f"BR world_player_add convert failed: {_e}")
        if br_rpc_id == BrRpc.REQUEST_SPAWN:
            if parsed['payload_bitlen'] != 8 or rpc_payload[0] not in (0, 1, 2):
                logger.warn('invalid BR RequestSpawn response')
                return []
            rpc_payload = SampRpcBuilder.request_spawn_payload(rpc_payload[0])
        elif br_rpc_id == BrRpc.SET_SPAWN_INFO:
            if parsed['payload_bitlen'] != 46 * 8:
                logger.warn('invalid BR SetSpawnInfo')
                return []
            self._spawn_info = rpc_payload
            for _, _, pending in self._samp_send_queue:
                if pending.get('local_class_response'):
                    pending['body'] = SampRpcBuilder.rpc_body(SampRpcBuilder.create(
                        SampRpc.REQUEST_CLASS, b'\x01' + self._spawn_info))
        elif br_rpc_id == BrRpc.REQUEST_CLASS:

            logger.debug('BR RequestClass response ignored: SA-MP is answered locally')
            return []

        samp_rpc_id = RPC_ID_BR_TO_SAMP.get(br_rpc_id)

        if samp_rpc_id is None:
            from samp_side.constants import RPC as SampRpcCheck
            if any(getattr(SampRpcCheck, n)==br_rpc_id for n in dir(SampRpcCheck) if n.isupper()):
                samp_rpc_id = br_rpc_id
                logger.packet("BR>SMP", f"rpc {br_name}({br_rpc_id}): fallback same ID")
            else:
                logger.packet(
                    "BR>SMP",
                    f"rpc {br_name}({br_rpc_id}): no SAMP mapping, dropped ({len(rpc_payload)}B payload)",
                )
                return []

        logger.packet(
            "BR>SMP",
            f"rpc {br_name} -> samp id {samp_rpc_id} "
            f"({len(rpc_payload)}B payload passed through)",
        )


        if samp_rpc_id in (SampRpc.WORLD_PLAYER_ADD, SampRpc.WORLD_PLAYER_REMOVE):
            samp_reliability = SampRel.RELIABLE_ORDERED
            samp_channel = 0

        return [{
            "body": SampRpcBuilder.rpc_body(
                SampRpcBuilder.create(samp_rpc_id, rpc_payload, parsed["payload_bitlen"])
            ),
            "reliability": samp_reliability,
            "ordering_channel": samp_channel,
        }]

    def _send_entries_to_samp(self, entries: list[dict]) -> None:
        if self._samp_sock is None or self._client_addr is None:
            return
        batches = self._samp_builder.build_batches(entries)
        now = time.monotonic()
        for datagram, records in batches:
            for record in records:
                if record['reliability'] in (SampRel.RELIABLE, SampRel.RELIABLE_ORDERED,
                                              SampRel.RELIABLE_SEQUENCED):
                    number = record['message_number']
                    if number in self._samp_resends:
                        raise RuntimeError('SA-MP message number still unacknowledged; reconnect required')
                    self._samp_resends[number] = {
                        'data': record['data'], 'sent': now,
                        'next': now + SAMP_RESEND_INTERVAL, 'retries': 0,
                    }
            try:
                self._send_to_samp(datagram)
            except OSError as exc:
                logger.warn(f'SAMP send failed; reliable frames retained for retry: {exc}')

    def _handle_samp_ack(self, ranges) -> None:
        now = time.monotonic()
        for number in list(self._samp_resends):
            if any(first <= number <= last for first, last in ranges):
                item = self._samp_resends.pop(number)
                if not item['retries']:
                    self._update_ping_from_rtt('samp', (now - item['sent']) * 1000)

    def _process_samp_resends(self) -> None:
        if self._samp_sock is None or self._client_addr is None:
            return
        now = time.monotonic()
        for number, item in list(self._samp_resends.items()):
            if now < item['next']:
                continue
            try:
                self._send_to_samp(item['data'])
            except OSError as exc:
                logger.warn(f'SAMP resend failed msg={number}: {exc}')
            item['retries'] += 1
            item['next'] = now + SAMP_RESEND_INTERVAL
            if item['retries'] in (1, 5, 15):
                logger.warn(f'SAMP waiting for ACK msg={number}, retries={item["retries"]}')
