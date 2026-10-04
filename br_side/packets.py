import json
import secrets
import struct

from core.bitstream import BitStream
from core import logger
from . import security


_APPMETRICA_ID = secrets.token_urlsafe(16)
_APPMETRICA_DEVICE_ID = str(secrets.randbelow((1 << 64) - (10 ** 19)) + (10 ** 19))
_APPMETRICA_ADJUST_ID = secrets.token_hex(16)
from .constants import (
    MAX_DATAGRAM_BYTES,
    OutgoingPacketID,
    OutgoingInterfaceSync,
    PacketReliability,
    RPC as BrRpcID,
    ORDERING_METADATA_RELIABILITIES,
    OPEN_CONNECTION_REQUEST_BODY,
    CLIENT_JOIN_HEADER_HEX,
)


def _has_ordering(reliability: int) -> bool:
    return reliability in ORDERING_METADATA_RELIABILITIES


def _is_encrypted(reliability: int) -> bool:
    return reliability in security.ENCRYPTED_RELIABILITIES


class BrDatagramBuilder:


    def __init__(self):
        self.message_number = 0
        self.split_packet_id = 0
        self.ordering_indices = [0] * 16

    def _next_message_number(self) -> int:
        current = self.message_number
        self.message_number = (current + 1) & 0xFFFF
        return current

    def _next_split_packet_id(self) -> int:
        current = self.split_packet_id
        self.split_packet_id = (current + 1) & 0xFFFF
        return current

    def _build_internal_packet(
        self,
        body: bytes,
        reliability: int,
        ordering_channel: int = 0,
        *,
        is_first_packet: bool,
        has_ack: bool,
        message_number: int | None = None,
        ordering_index: int | None = None,
        encrypt: bool | None = None,
        split_id: int | None = None,
        split_index: int | None = None,
        split_count: int | None = None,
    ) -> bytes:
        if message_number is None:
            message_number = self._next_message_number()

        if encrypt is None:
            encrypted = _is_encrypted(reliability)
        else:
            encrypted = bool(encrypt)

        if encrypted:
            wire_body, checksum_byte = security.encode_with_checksum(body)
        else:
            wire_body, checksum_byte = bytes(body), None

        bs = BitStream()

        if is_first_packet:
            bs.write_bool(has_ack)

        bs.write_uint16(message_number)
        bs.write_bits(int(reliability) & 0x07, 3)


        if encrypted:
            bs.write_uint8(checksum_byte & 0xFF)

        if _has_ordering(reliability):
            ordering_channel &= 0x0F
            if ordering_index is None:
                ordering_index = self.ordering_indices[ordering_channel]
                self.ordering_indices[ordering_channel] = (ordering_index + 1) & 0xFFFF
            bs.write_uint16(ordering_index)
            bs.write_bits((~ordering_channel) & 0x0F, 4)

        is_split = split_id is not None
        bs.write_bool(is_split)

        if is_split:
            bs.write_uint16(split_id & 0xFFFF)
            bs.write_compressed(split_index, 32, unsigned=True)
            bs.write_compressed(split_count, 32, unsigned=True)

        bs.write_compressed(len(wire_body) * 8, 16, unsigned=True)
        bs.align_to_byte()
        bs.write_bytes(wire_body)

        return bs.get_bytes()

    def _split_entries(self, entry: dict) -> list[dict]:

        body = bytes(entry["body"])
        reliability = int(entry.get("reliability", PacketReliability.RELIABLE))
        ordering_channel = int(entry.get("ordering_channel", 0))
        force_encrypt = entry.get("encrypt")

        chunk_size = min(len(body) - 1, MAX_DATAGRAM_BYTES - 80)
        split_id = self._next_split_packet_id()

        while chunk_size > 0:
            split_count = (len(body) + chunk_size - 1) // chunk_size
            preview = self._build_internal_packet(
                body[:chunk_size],
                reliability,
                ordering_channel,
                is_first_packet=True,
                has_ack=False,
                message_number=0,
                ordering_index=0,
                encrypt=force_encrypt,
                split_id=split_id,
                split_index=split_count - 1,
                split_count=split_count,
            )

            if len(preview) <= MAX_DATAGRAM_BYTES:
                break

            chunk_size -= max(1, len(preview) - MAX_DATAGRAM_BYTES)
        else:
            raise ValueError("split header leaves no room for payload")

        split_count = (len(body) + chunk_size - 1) // chunk_size

        fragments: list[dict] = []
        for index in range(split_count):
            fragment = dict(entry)
            fragment["body"] = body[index * chunk_size:(index + 1) * chunk_size]
            fragment["split_id"] = split_id
            fragment["split_index"] = index
            fragment["split_count"] = split_count
            fragments.append(fragment)

        logger.debug(
            f"br outgoing split: {len(body)}B -> {split_count} "
            f"fragments x {chunk_size}B"
        )

        return fragments

    def _pack_flat(self, flat_entries: list[dict]) -> bytes:
        chunks: list[bytes] = []

        for index, entry in enumerate(flat_entries):
            chunks.append(
                self._build_internal_packet(
                    entry["body"],
                    int(entry.get("reliability", PacketReliability.RELIABLE)),
                    int(entry.get("ordering_channel", 0)),
                    is_first_packet=(index == 0),
                    has_ack=False,
                    encrypt=entry.get("encrypt"),
                    ordering_index=entry.get("ordering_index"),
                    split_id=entry.get("split_id"),
                    split_index=entry.get("split_index"),
                    split_count=entry.get("split_count"),
                )
            )

        return b"".join(chunks)

    def build_datagrams(self, entries: list[dict]) -> list[bytes]:


        datagrams: list[bytes] = []
        batch: list[dict] = []

        for original in entries:
            entry = dict(original)
            reliability = int(entry.get('reliability', PacketReliability.RELIABLE))
            if _has_ordering(reliability) and 'ordering_index' not in entry:
                channel = int(entry.get('ordering_channel', 0)) & 0x0F
                entry['ordering_index'] = self.ordering_indices[channel]
                self.ordering_indices[channel] = (self.ordering_indices[channel] + 1) & 0xFFFF
            needs_split = (
                "split_id" not in entry
                and len(entry["body"]) > MAX_DATAGRAM_BYTES - 40
            )

            if not needs_split:
                batch.append(entry)
                continue

            if batch:
                datagrams.append(self._pack_flat(batch))
                batch = []

            for fragment in self._split_entries(entry):
                datagrams.append(self._pack_flat([fragment]))

        if batch:
            datagrams.append(self._pack_flat(batch))

        for datagram in datagrams:
            if len(datagram) > MAX_DATAGRAM_BYTES:
                raise ValueError(
                    f"BR datagram {len(datagram)} bytes > "
                    f"{MAX_DATAGRAM_BYTES}"
                )

        return datagrams

    def build_datagram(self, entries: list[dict]) -> bytes:

        datagrams = self.build_datagrams(entries)
        if not datagrams:
            return b""
        if len(datagrams) > 1:
            raise ValueError("body needs split — use build_datagrams()")
        return datagrams[0]

    def build_ack(self, message_numbers: list[int]) -> bytes:

        numbers = sorted({int(n) & 0xFFFF for n in message_numbers})

        ranges: list[tuple[int, int]] = []
        for n in numbers:
            if ranges and n == ranges[-1][1] + 1:
                ranges[-1] = (ranges[-1][0], n)
            else:
                ranges.append((n, n))

        bs = BitStream()
        bs.write_bool(True)
        bs.write_compressed(len(ranges), 16, unsigned=True)

        for rmin, rmax in ranges:
            bs.write_bool(rmin == rmax)
            bs.write_uint16(rmin)
            if rmin != rmax:
                bs.write_uint16(rmax)

        bs.align_to_byte()
        return bs.get_bytes()


def open_connection_request() -> bytes:

    return bytes([OutgoingPacketID.OPEN_CONNECTION_REQUEST]) + (
        OPEN_CONNECTION_REQUEST_BODY
    )


class BrBodyBuilder:

    @staticmethod
    def create(packet_id: int, payload: bytes = b"") -> bytes:
        return bytes([packet_id & 0xFF]) + (payload or b"")

    @staticmethod
    def connection_request() -> bytes:
        return BrBodyBuilder.create(OutgoingPacketID.CONNECTION_REQUEST)

    @staticmethod
    def auth_key_response(server_key: str) -> bytes:


        key_96 = sub_4d368c(server_key)

        return BrBodyBuilder.create(
            OutgoingPacketID.AUTH_KEY_RESPONSE,
            bytes([0x60]) + key_96.encode("ascii") + b"\x00",
        )

    @staticmethod
    def new_incoming_connection(server_ip: str, server_port: int) -> bytes:
        import socket as _socket

        return BrBodyBuilder.create(
            OutgoingPacketID.NEW_INCOMING_CONNECTION,
            _socket.inet_aton(str(server_ip))
            + (server_port & 0xFFFF).to_bytes(2, "big")
            + b"\x00",
        )

    @staticmethod
    def internal_ping(sender_ms: int) -> bytes:
        return BrBodyBuilder.create(
            OutgoingPacketID.INTERNAL_PING,
            int(sender_ms & 0xFFFFFFFF).to_bytes(4, "little"),
        )

    @staticmethod
    def connected_pong(receiver_ms: int, sender_ms: int) -> bytes:
        return BrBodyBuilder.create(
            OutgoingPacketID.CONNECTED_PONG,
            int(sender_ms & 0xFFFFFFFF).to_bytes(4, "little")
            + int(receiver_ms & 0xFFFFFFFF).to_bytes(4, "little"),
        )

    @staticmethod
    def disconnection_notification() -> bytes:
        return BrBodyBuilder.create(
            OutgoingPacketID.DISCONNECTION_NOTIFICATION
        )


class BrRpcBuilder:

    @staticmethod
    def spawn_payload() -> bytes:
        return b''

    @staticmethod
    def request_spawn_payload() -> bytes:
        return b''

    @staticmethod
    def chat_payload(message: str) -> bytes:
        encoded = message.encode('utf-8')
        if len(encoded) > 255:
            raise ValueError('BR chat exceeds 255 bytes')
        return bytes([len(encoded)]) + encoded + b'\x00'

    @staticmethod
    def server_command_payload(command: str) -> bytes:
        encoded = command.encode('utf-8')
        return struct.pack('<I', len(encoded)) + encoded if encoded else b''

    @staticmethod
    def create(rpc_id: int, payload: bytes = b"", payload_bitlen: int | None = None) -> bytes:
        if not 0 <= rpc_id <= 511:
            raise ValueError("BR RPC ID does not fit the known 9-bit framing")
        bitlen = len(payload) * 8 if payload_bitlen is None else payload_bitlen
        if not 0 <= bitlen <= len(payload) * 8 or bitlen > 65535:
            raise ValueError("invalid RPC payload bit length")
        bs = BitStream()
        bs.write_uint(rpc_id & 0xFF, 8)
        bs.write_uint((rpc_id >> 8) & 1, 1)
        bs.write_uint(0b11, 2)
        bs.write_compressed(bitlen, 16, unsigned=True)
        if bitlen:
            bs.write_bits(int.from_bytes(payload, "big") >> (len(payload) * 8 - bitlen), bitlen)
        return bs.get_bytes()

    @staticmethod
    def rpc_body(packet_body: bytes) -> bytes:
        return BrBodyBuilder.create(OutgoingPacketID.RPC, packet_body)

    @staticmethod
    def client_join_payload(nickname: str) -> tuple[bytes, int]:

        nickname_bytes = str(nickname or "").encode("ascii", errors="ignore")
        nickname_bytes = nickname_bytes[:20]

        bs = BitStream()
        bs.write_bytes(bytes.fromhex(CLIENT_JOIN_HEADER_HEX))
        bs.write_uint(len(nickname_bytes), 8)
        bs.write_bytes(nickname_bytes)

        return bs.get_bytes(), len(bs)

    @staticmethod
    def request_class_payload(wclass: int = 0) -> tuple[bytes, int]:

        bs = BitStream()
        bs.write_uint32(int(wclass) & 0xFFFFFFFF)
        return bs.get_bytes(), len(bs)


class InterfaceSyncPayloadBuilder:


    @staticmethod
    def create(interface_id: int, data: dict) -> bytes:
        json_bytes = json.dumps(
            data, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        return (
            struct.pack("<H", interface_id & 0xFFFF)
            + struct.pack("<I", len(json_bytes))
            + json_bytes
        )

    @staticmethod
    def auth_password(password: str) -> bytes:
        return InterfaceSyncPayloadBuilder.create(
            OutgoingInterfaceSync.AUTH, {"t": 6, "s": str(password), "r": 0}
        )

    @staticmethod
    def register_password(password: str, email: str = "") -> bytes:
        return InterfaceSyncPayloadBuilder.create(
            OutgoingInterfaceSync.AUTH,
            {"t": 1, "s": str(email or ""), "p": str(password)},
        )

    @staticmethod
    def register_unknown_after_password() -> bytes:
        return InterfaceSyncPayloadBuilder.create(
            OutgoingInterfaceSync.AUTH, {"t": 2, "s": "", "r": 0}
        )

    @staticmethod
    def register_referral(referral: str = "") -> bytes:
        return InterfaceSyncPayloadBuilder.create(
            OutgoingInterfaceSync.AUTH, {"t": 4, "s": str(referral or "")}
        )

    @staticmethod
    def register_gender(gender: int = 0) -> bytes:
        gender = int(gender)
        if gender not in (0, 1):
            raise ValueError("gender must be 0 or 1")
        return InterfaceSyncPayloadBuilder.create(
            OutgoingInterfaceSync.AUTH, {"t": 3, "r": gender}
        )

    @staticmethod
    def register_skin(skin_id: int = 78) -> bytes:
        return InterfaceSyncPayloadBuilder.create(
            OutgoingInterfaceSync.AUTH, {"t": 5, "r": int(skin_id)}
        )

    @staticmethod
    def register_finish() -> bytes:
        return InterfaceSyncPayloadBuilder.create(
            OutgoingInterfaceSync.AUTH, {"c": 1}
        )

    @staticmethod
    def appmetrica() -> bytes:
        return InterfaceSyncPayloadBuilder.create(
            OutgoingInterfaceSync.AUTH,
            {
                "t": 9,
                "id": _APPMETRICA_ID,
                "appmetrica_device_id": _APPMETRICA_DEVICE_ID,
                "adjust_id": _APPMETRICA_ADJUST_ID,
            },
        )


_AUTH_SEEDS = [
    0xA311, 0xA436, 0xAF70, 0xA5BD,
    0xAC07, 0xA693, 0xABE2, 0xA119,
    0xAE4B, 0xA7D0, 0xAAC5, 0xA00A,
    0xA2BC, 0xA5D1, 0xA88D, 0xAD62,
    0xA658, 0xA9A3, 0xA46D, 0xAB30,
    0xA081, 0xAC14, 0xA7CB, 0xAEDF,
    0xA326, 0xA133, 0xAF50, 0xA515,
    0xA9E8, 0xA242, 0xA69C, 0xAA67,
]

_AUTH_GLOBAL_XOR = 0xA5C3

_U32_MASK = 0xFFFFFFFF
_MUL_A = (-0x61C8864F) & _U32_MASK
_MUL_B = (-0x3D4D51C3) & _U32_MASK
_CONST_MIX = 0x85EBCA77
_CONST_XOR = 0x5A5A5A5A


def _u32(value: int) -> int:
    return value & _U32_MASK


def _rotr32(value: int, count: int) -> int:
    value &= _U32_MASK
    count &= 31

    if count == 0:
        return value

    return ((value >> count) | (value << (32 - count))) & _U32_MASK


def _auth_mix_finish(value: int, add_value: int) -> int:
    mixed = _u32(value * _MUL_B)

    return _u32((mixed ^ (mixed >> 15)) + add_value)


def sub_4d368c(key: str) -> str:
    if not isinstance(key, str):
        raise TypeError("auth key must be str")

    key_bytes = key.encode("ascii")
    n = len(key_bytes)

    if n > 0x80:
        raise ValueError(f"bad len {n}")

    buf = bytearray(0x81)
    buf[:n] = key_bytes
    buf[n] = 0

    state = [
        (_AUTH_GLOBAL_XOR ^ seed) & _U32_MASK
        for seed in _AUTH_SEEDS
    ]

    if n == 0:
        state = [v & 0xFFF for v in state]

    else:
        local_134 = -0x15B
        local_138 = 0

        for idx in range(0x20):
            u8 = state[idx]
            i14 = 0

            for pos in range(n):
                b = buf[pos]
                u12 = b

                u6 = _u32(pos * -0x4F + u12)
                u3 = _u32(pos + ((u12 + i14) & 0xFF))

                u8 = _u32(u8 + u3)
                u13 = u6 & 0xFF

                u8 = _u32((idx - n) + _rotr32(u8, 25)) ^ u8
                u8 = _u32(
                    ((u13 << 2) ^ _rotr32(u8, 27) ^ 0x20)
                    + local_134
                    + pos
                )
                u8 = _u32(u8 * _MUL_A)

                branch = u12 & 3

                if (b & 3) == 0:
                    r1 = ((~u8) & 0x1F) | 0x18
                    u4 = _u32(local_138 + pos)
                    r2 = ((~u13) & 0x1F) | 0x10

                    t = _u32((u13 | 1) * _MUL_A)
                    t = _u32(t + (_rotr32(u13, r1) ^ u8))
                    t = _u32(t ^ _rotr32(u4, r2))
                    i10 = _u32(t - (u4 ^ _CONST_MIX))

                    u3 = _auth_mix_finish(
                        i10,
                        _rotr32(u8, (-u4) & 0x1F)
                    )

                elif branch == 1:
                    u4 = _u32(u8 ^ pos)
                    r1 = ((~u13) & 0x1F) | 0x18
                    r2 = (-((idx & 0xF) + 1)) & 0x1F

                    t = _u32(
                        (_rotr32(idx, r1) ^ u13)
                        + _u32((idx | 1) * _MUL_A)
                    )
                    t = _u32(t ^ _rotr32(u4, r2))
                    t = _u32(t - (u4 ^ _CONST_MIX))
                    t = _u32(t * _MUL_B)

                    u3 = _u32(
                        _rotr32(u13, (-u4) & 0x1F)
                        + (t ^ (t >> 15))
                    )

                elif branch == 2:
                    r1 = (-((idx & 7) + 1)) & 0x1F
                    r2 = ((~u8) & 0x1F) | 0x10

                    t = _u32((u8 | 1) * _MUL_A)
                    t = _u32(t + (_rotr32(u8, r1) ^ idx))
                    t = _u32(t ^ _rotr32(u3, r2))
                    t = _u32(t - (u3 ^ _CONST_MIX))
                    t = _u32(t * _MUL_B)

                    u3 = _u32(
                        _rotr32(idx, (-u3) & 0x1F)
                        + (t ^ (t >> 15))
                    )

                else:
                    u7 = _u32(u8 ^ u13)
                    u4 = _u32(idx ^ u13)
                    r1 = ((~u7) & 0x1F) | 0x18
                    u9 = _u32(pos ^ idx)
                    r2 = ((~u4) & 0x1F) | 0x10

                    t = _u32((u4 | 1) * _MUL_A)
                    t = _u32(t + (_rotr32(u4, r1) ^ u7))
                    t = _u32(_rotr32(u9, r2) ^ t)
                    i10 = _u32(t - (u9 ^ _CONST_MIX))

                    u3 = _auth_mix_finish(
                        i10,
                        _rotr32(u7, (-u9) & 0x1F)
                    )

                r = (-(u6 & 0xF)) & 0x1F
                u8 = _u32(u8 ^ _rotr32(u3, r))

                check = _u32(u8 ^ u13) | 1

                if ((check * check) & 5) == 1:
                    r1 = ((~u3) & 0x1F) | 0x18
                    r2 = ((~u8) & 0x1F) | 0x10

                    t = _u32((u8 | 1) * _MUL_A)
                    t = _u32(t + (_rotr32(u8, r1) ^ u3))
                    t = _u32(t ^ _rotr32(u13, r2))
                    t = _u32(t - (u13 ^ _CONST_MIX))
                    t = _u32(t * _MUL_B)

                    add = _u32(
                        _rotr32(u3, (-u13) & 0x1F)
                        + (t ^ (t >> 15))
                    )

                    u8 = _u32(u8 + (add ^ _CONST_XOR))

                i14 = _u32(i14 - 0x4F)

                buf[pos] = (
                    b
                    + (((idx & 0xFF) + (n & 0xFF)) & 0xFF) * 4
                ) & 0xFF

            local_138 = _u32(local_138 + 0x1F)
            state[idx] = u8 & 0xFFF
            local_134 -= 1

    out = bytearray(97)

    for i in range(32):
        v = state[i] & 0xFFF
        base = i * 3

        out[base] = ((v >> 8) & 0xF) + 0x44
        out[base + 1] = ((v >> 4) & 0xF) + 0x47
        out[base + 2] = (v & 0xF) + 0x4B

    out[96] = 0

    return out[:96].decode("ascii")


def log_build(datagram: bytes, note: str = "") -> None:
    logger.packet("BR>>", f"{len(datagram)}B {datagram[:24].hex()} {note}")
