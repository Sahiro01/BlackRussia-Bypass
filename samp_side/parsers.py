import time

from core.bitstream import BitStream
from core import logger
from . import security
from .constants import (
    OFFLINE_PACKET_IDS,
    OPEN_CONNECTION_COOKIE,
    PacketID,
    PacketReliability,
    ORDERING_METADATA_RELIABILITIES,
)

SPLIT_TIMEOUT_SEC = 30.0
SPLIT_STORE_MAX_GROUPS = 128


def _has_ordering(reliability: int) -> bool:
    return reliability in ORDERING_METADATA_RELIABILITIES


class SampDatagramParser:

    @staticmethod
    def parse_datagram(
        data: bytes,
        *,
        decrypt_port: int | None = None,
        split_store: dict | None = None,
        receive_state=None,
    ) -> list[dict]:


        if not data:
            return []

        if decrypt_port is not None and len(data) >= 2:
            plaintext, ok = security.decrypt_incoming(data, decrypt_port)

            if ok:
                return SampDatagramParser._parse_plain(plaintext, split_store, receive_state)

            logger.debug(
                f"SAMP decrypt checksum mismatch "
                f"(port={decrypt_port}, {len(data)}B) — fallback plain"
            )

        return SampDatagramParser._parse_plain(data, split_store, receive_state)

    @staticmethod
    def _parse_plain(data: bytes, split_store: dict | None = None, receive_state=None) -> list[dict]:
        if not data:
            return []


        if len(data) < 3:
            return [{
                "kind": "offline",
                "packet_id": data[0],
                "body": data[1:],
                "data": data,
            }]

        if data[0] in OFFLINE_PACKET_IDS and len(data) <= 5:
            return [{
                "kind": "offline",
                "packet_id": data[0],
                "body": data[1:],
                "data": data,
            }]

        packets: list[dict] = []
        bs = BitStream(data)

        try:
            has_acks = bs.read_bool()

            if has_acks:
                packets.append({"kind": "ack", "ranges": SampDatagramParser._skip_range_list(bs)})

            if bs.remaining_bits < 16:
                return packets

            while bs.remaining_bits >= 16:
                pkt = SampDatagramParser._parse_one_packet(bs)
                if receive_state is not None and receive_state.is_duplicate(pkt):
                    packets.append({'kind': 'duplicate', 'message_number': pkt['message_number'],
                                    'reliability': pkt['reliability']})
                    continue

                if pkt.get("has_split"):
                    assembled = SampDatagramParser._handle_split(
                        pkt, split_store
                    )
                    packets.append({
                        "kind": "fragment",
                        "message_number": pkt["message_number"],
                        "reliability": pkt["reliability"],
                    })
                    if assembled is not None:
                        packets.append(assembled)
                else:
                    packets.append(pkt)

        except Exception as exc:
            logger.debug(f"SAMP datagram parse fallback to raw: {exc}")
            packets.append({
                "kind": "raw",
                "data": data[bs.bitpos // 8:] if bs.bitpos else data,
            })

        return packets


    @staticmethod
    def _cleanup_splits(split_store: dict, now: float) -> None:
        expired = [
            key for key, entry in split_store.items()
            if now - entry.get("created_at", now) > SPLIT_TIMEOUT_SEC
        ]
        for key in expired:
            del split_store[key]

        while len(split_store) > SPLIT_STORE_MAX_GROUPS:
            del split_store[next(iter(split_store))]

    @staticmethod
    def _handle_split(pkt: dict, split_store: dict | None) -> dict | None:
        if split_store is None:
            logger.debug("SAMP split packet without split_store — dropped")
            return None

        split_id = pkt.get("split_id")
        split_index = pkt.get("split_index")
        split_count = pkt.get("split_count")

        if not split_count or split_index is None or split_index >= split_count:
            return None

        now = time.monotonic()
        SampDatagramParser._cleanup_splits(split_store, now)

        key = (split_id, split_count)
        entry = split_store.get(key)

        if entry is None:
            entry = {
                "count": split_count,
                "parts": {},
                "first": pkt,
                "created_at": now,
            }
            split_store[key] = entry

        entry["parts"][split_index] = pkt["body"]

        if len(entry["parts"]) < split_count:
            return None

        if any(i not in entry["parts"] for i in range(split_count)):
            return None

        full_body = b"".join(entry["parts"][i] for i in range(split_count))
        del split_store[key]

        first = entry["first"]
        return {
            "kind": "packet",
            "message_number": first["message_number"],
            "reliability": first["reliability"],
            "ordering_channel": first.get("ordering_channel"),
            "ordering_index": first.get("ordering_index"),
            "has_split": False,
            "body": full_body,
            "reassembled": True,
        }

    @staticmethod
    def _skip_range_list(bs: BitStream) -> list[tuple[int, int]]:
        count = bs.read_compressed(16, unsigned=True)
        ranges: list[tuple[int, int]] = []

        for _ in range(count):
            max_eq_min = bs.read_bool()
            rmin = bs.read_uint16()

            if max_eq_min:
                ranges.append((rmin, rmin))
            else:
                ranges.append((rmin, bs.read_uint16()))

        return ranges

    @staticmethod
    def _parse_one_packet(bs: BitStream) -> dict:
        message_number = bs.read_uint16()
        reliability = bs.read_bits(4)

        ordering_channel = None
        ordering_index = None


        if _has_ordering(reliability):
            ordering_channel = bs.read_bits(5)
            ordering_index = bs.read_uint16()

        has_split = bs.read_bool()
        split_id = None
        split_index = None
        split_count = None
        if has_split:
            split_id = bs.read_uint16()
            split_index = bs.read_compressed(32, unsigned=True)
            split_count = bs.read_compressed(32, unsigned=True)

        bitlen = bs.read_compressed(16, unsigned=True)
        body_bytes = (bitlen + 7) // 8

        bs.align_to_byte()
        body = bs.read_bytes(body_bytes) if body_bytes else b""

        return {
            "kind": "packet",
            "message_number": message_number,
            "reliability": reliability,
            "ordering_channel": ordering_channel,
            "ordering_index": ordering_index,
            "has_split": has_split,
            "split_id": split_id,
            "split_index": split_index,
            "split_count": split_count,
            "body": body,
        }


def _is_reliable(reliability: int) -> bool:
    return reliability in RELIABLE_RELIABILITIES


def parse_open_connection_request(body: bytes) -> dict | None:

    if len(body) < 2:
        return None

    cookie = ((body[0] << 8) | body[1]) ^ OPEN_CONNECTION_COOKIE

    return {
        "type": "open_connection_request",
        "cookie": cookie,
    }


def read_rpc(body: bytes) -> dict | None:

    if len(body) < 2:
        return None

    try:
        bs = BitStream(body)
        rpc_id = bs.read_bits(8)
        skip = bs.read_bits(2)
        bitlen = bs.read_compressed(16, unsigned=True)

        if bitlen > bs.remaining_bits:
            logger.warn(
                f"SAMP RPC bitlen {bitlen} > remaining {bs.remaining_bits}"
            )
            return None

        payload_value = bs.read_bits(bitlen)

        if bitlen <= 0:
            payload = b""
        else:
            count = (bitlen + 7) // 8
            pad = count * 8 - bitlen
            if pad:
                payload_value <<= pad
            payload = payload_value.to_bytes(count, "big")

        return {
            "rpc_id": rpc_id,
            "skip": skip,
            "payload": payload,
            "payload_bitlen": bitlen,
        }

    except Exception as exc:
        logger.warn(f"SAMP rpc parse error: {exc}")
        return None


def parse_client_join(payload: bytes) -> dict | None:


    try:
        bs = BitStream(payload)

        iversion = bs.read_uint32()
        byte_mod = bs.read_uint8()

        nick_len = bs.read_uint8()
        nickname = bs.read_string(nick_len, "cp1251", errors="ignore")

        challenge_response = bs.read_uint32()

        auth_key_len = bs.read_uint8()
        auth_auth = bs.read_string(auth_key_len, "ascii", errors="ignore")

        client_version_len = bs.read_uint8()
        client_version = bs.read_string(
            client_version_len, "ascii", errors="ignore"
        )

        return {
            "iversion": iversion,
            "byte_mod": byte_mod,
            "nickname": nickname,
            "challenge_response": challenge_response,
            "auth_key": auth_auth,
            "client_version": client_version,
        }

    except Exception as exc:
        logger.warn(f"SAMP client_join parse error: {exc}")
        return None


def parse_auth_key_response(body: bytes) -> dict | None:

    if len(body) < 2:
        return None

    key_len = body[0]
    if key_len == 0 or len(body) < 1 + key_len:
        return None

    key = body[1:1 + key_len].decode("ascii", errors="ignore")

    return {
        "type": "auth_key_response",
        "key": key,
        "key_len": key_len,
    }


def parse_new_incoming_connection(body: bytes) -> dict | None:

    if len(body) < 6:
        return None

    import socket as _socket

    try:
        ip = _socket.inet_ntoa(body[0:4])
        port = (body[4] << 8) | body[5]
    except OSError:
        return None

    return {
        "type": "new_incoming_connection",
        "ip": ip,
        "port": port,
    }


PACKET_BODY_NAMES = {
    PacketID.OPEN_CONNECTION_REQUEST: "open_connection_request",
    PacketID.CONNECTION_REQUEST: "connection_request",
    PacketID.AUTH_KEY_RESPONSE: "auth_key_response",
    PacketID.NEW_INCOMING_CONNECTION: "new_incoming_connection",
    PacketID.INTERNAL_PING: "internal_ping",
    PacketID.CONNECTED_PONG: "connected_pong",
    PacketID.RECEIVED_STATIC_DATA: "received_static_data",
    PacketID.DETECT_LOST_CONNECTIONS: "detect_lost_connections",
    PacketID.RPC: "rpc",
}


parse_datagram = SampDatagramParser.parse_datagram
