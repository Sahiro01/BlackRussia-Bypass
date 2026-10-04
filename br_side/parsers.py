import json
import time

from core.bitstream import BitStream
from core import logger
from . import security
from .constants import (
    OFFLINE_PACKET_IDS,
    AUTH_KEY_LEN,
    IncomingPacketID,
    IncomingInterfaceSync,
    PacketReliability,
    ORDERING_METADATA_RELIABILITIES,
)

SPLIT_TIMEOUT_SEC = 30.0
SPLIT_STORE_MAX_GROUPS = 128


def _has_ordering(reliability: int) -> bool:
    return reliability in ORDERING_METADATA_RELIABILITIES


def _is_encrypted(reliability: int) -> bool:
    return reliability in security.ENCRYPTED_RELIABILITIES


class BrDatagramParser:

    @staticmethod
    def parse_datagram(
        data: bytes,
        *,
        split_store: dict | None = None,
        receive_state=None,
    ) -> list[dict]:


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
                packets.append({"kind": "ack", "ranges": BrDatagramParser._skip_range_list(bs)})

            if bs.remaining_bits < 16:
                return packets

            while bs.remaining_bits >= 16:
                pkt = BrDatagramParser._parse_one_packet(bs)
                if receive_state is not None and receive_state.is_duplicate(pkt):
                    packets.append({"kind": "duplicate", "message_number": pkt['message_number'],
                                    "reliability": pkt['reliability']})
                    continue

                if pkt.get("has_split"):
                    assembled = BrDatagramParser._handle_split(
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
            logger.debug(f"BR datagram parse fallback to raw: {exc}")
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
            oldest = next(iter(split_store))
            del split_store[oldest]

    @staticmethod
    def _handle_split(pkt: dict, split_store: dict | None) -> dict | None:

        if split_store is None:
            logger.debug("BR split packet without split_store — dropped")
            return None

        split_id = pkt.get("split_id")
        split_index = pkt.get("split_index")
        split_count = pkt.get("split_count")

        if not split_count or split_index is None or split_index >= split_count:
            return None

        now = time.monotonic()
        BrDatagramParser._cleanup_splits(split_store, now)

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

        missing = [
            i for i in range(split_count) if i not in entry["parts"]
        ]
        if missing:
            return None

        full_body = b"".join(
            entry["parts"][i] for i in range(split_count)
        )

        del split_store[key]

        first = entry["first"]

        return {
            "kind": "packet",
            "message_number": first["message_number"],
            "reliability": first["reliability"],
            "ordering_channel": first.get("ordering_channel"),
            "ordering_index": first.get("ordering_index"),
            "has_split": False,
            "encrypted": False,
            "checksum_ok": True,
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
        reliability = bs.read_bits(3)
        encrypted = _is_encrypted(reliability)

        header_checksum = bs.read_uint8() if encrypted else None

        ordering_channel = None
        ordering_index = None


        if _has_ordering(reliability):
            ordering_index = bs.read_uint16()
            ordering_channel = (~bs.read_bits(4)) & 0x0F

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
        wire_body = bs.read_bytes(body_bytes) if body_bytes else b""

        checksum_ok = None
        if encrypted:
            body, checksum_ok, _calculated = (
                security.decode_with_checksum(wire_body, header_checksum)
            )


            if checksum_ok is False:
                raise ValueError(
                    f"BR payload checksum mismatch "
                    f"(msg={message_number})"
                )
        else:
            body = wire_body

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
            "encrypted": encrypted,
            "checksum_ok": checksum_ok,
            "body": body,
        }


def parse_server_join(payload: bytes) -> dict | None:

    if len(payload) < 16:
        return None
    length = payload[15]
    if len(payload) < 16 + length:
        return None
    return {
        'player_id': int.from_bytes(payload[:2], 'little'),
        'color': int.from_bytes(payload[2:6], 'little'),
        'is_npc': payload[6],
        'name': payload[16:16 + length].decode('cp1251', errors='replace'),
    }


def parse_open_connection_reply(body: bytes) -> dict:
    return {
        "type": "open_connection_reply",
        "raw": body,
    }


def parse_auth_key(body: bytes) -> dict | None:


    if len(body) < 2:
        return None

    key_len = body[0]

    if key_len not in (AUTH_KEY_LEN, 24):
        return None

    if len(body) < 1 + key_len:
        return None

    key = body[1:1 + key_len].decode("ascii", errors="ignore")

    terminator = (
        body[1 + key_len]
        if len(body) > 1 + key_len
        else None
    )

    return {
        "type": "auth_key",
        "key": key,
        "key_len": key_len,
        "terminator": terminator,
    }


def read_rpc(body: bytes) -> dict | None:

    if len(body) < 3:
        return None

    try:
        bs = BitStream(body)

        low8 = bs.read_uint8()
        high1 = bs.read_bits(1)
        rpc_id = low8 | (high1 << 8)

        skip = bs.read_bits(2)
        bitlen = bs.read_compressed(16, unsigned=True)

        if bitlen > bs.remaining_bits:
            logger.warn(
                f"BR RPC bitlen {bitlen} > remaining {bs.remaining_bits}"
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
        logger.warn(f"BR rpc parse error: {exc}")
        return None


def parse_client_message(payload: bytes) -> dict | None:


    try:
        bs = BitStream(payload)

        color = bs.read_uint32()
        length = bs.read_uint32()
        message = bs.read_string(length, "utf-8", errors="ignore")

        return {
            "color": color,
            "message": message,
        }

    except Exception as exc:
        logger.debug(f"BR client_message parse error: {exc}")
        return None


def parse_show_dialog(payload: bytes) -> dict | None:


    def try_parse(title_len_reader):
        try:
            bs = BitStream(payload)
            dialog_id = bs.read_uint16()
            style = bs.read_uint8()
            title = bs.read_string(title_len_reader(bs), "utf-8", errors="ignore")
            button1 = bs.read_string(title_len_reader(bs), "utf-8", errors="ignore")
            button2 = bs.read_string(title_len_reader(bs), "utf-8", errors="ignore")
            info_len = bs.read_uint32()

            if info_len > bs.remaining_bits // 8:
                return None
            info = bs.read_string(info_len, "utf-8", errors="ignore")

            if bs.remaining_bits not in (0, 1, 2, 3, 4, 5, 6, 7):

                if bs.remaining_bits > 8:
                    return None
            return {
                "dialog_id": dialog_id,
                "dialog_style": style,
                "dialog_title": title,
                "dialog_button1": button1,
                "dialog_button2": button2,
                "dialog_info": info,
            }
        except:
            return None


    for reader in (lambda bs: bs.read_uint32(), lambda bs: bs.read_uint8()):
        res = try_parse(reader)
        if res is not None:

            if res["dialog_title"] or res["dialog_info"]:
                return res

            return res
    logger.debug(f"BR show_dialog parse error: both u32/u8 failed, payload {payload[:40].hex()}")
    return None


def parse_interface_sync(body: bytes) -> dict | None:


    try:
        bs = BitStream(body)

        if bs.remaining_bits < 48:
            return None

        interface_id = bs.read_uint16()
        json_len = bs.read_uint32()

        if json_len > bs.remaining_bits // 8:
            return None

        json_bytes = bs.read_bytes(json_len)

        try:
            data = json.loads(json_bytes.decode("utf-8", errors="ignore"))
        except Exception:
            return None


        if interface_id == IncomingInterfaceSync.AUTH:
            return _parse_auth_interface(data)

        if interface_id == IncomingInterfaceSync.SPAWN_SELECT:
            if isinstance(data, dict):
                return {
                    "type": "interface_sync",
                    "state": "spawn_select",
                    "spawn_available": (
                        list(data.get("m"))
                        if isinstance(data.get("m"), list)
                        else []
                    ),
                    "json": data,
                    "interface_id": interface_id,
                }
            return None


        if isinstance(data, dict):
            return {
                "type": "interface_sync",
                "state": "unknown",
                "interface_id": interface_id,
                "json": data,
            }

        return None

    except Exception as exc:
        logger.debug(f"InterfaceSync parse error: {exc}")
        return None


def _parse_auth_interface(data) -> dict | None:
    if not isinstance(data, dict):
        return None

    keys = list(data.keys())
    if not keys:
        return None


    if "o" in data:
        if data.get("o") != 1 or "r" not in data:
            return None
        r = data.get("r")
        if r == 1:
            return {"type": "interface_sync", "state": "auth_allowed"}
        if r == 0:
            return {"type": "interface_sync", "state": "registration_allowed"}
        return None


    if "t" in data:
        t = data.get("t")
        if t == 0:
            return {"type": "interface_sync", "state": "ack_auth"}
        if t == 3:
            return {"type": "interface_sync", "state": "registration_continue_required"}
        return None

    return None


PACKET_BODY_NAMES = {
    IncomingPacketID.OPEN_CONNECTION_REPLY: "open_connection_reply",
    IncomingPacketID.AUTH_KEY: "auth_key",
    IncomingPacketID.CONNECTION_REQUEST_ACCEPTED: (
        "connection_request_accepted"
    ),
    IncomingPacketID.USER_INTERFACE_SYNC: "interface_sync",
    IncomingPacketID.INTERNAL_PING: "internal_ping",
    IncomingPacketID.CONNECTED_PONG: "connected_pong",
    IncomingPacketID.DISCONNECTION_NOTIFICATION: (
        "disconnection_notification"
    ),
    IncomingPacketID.RPC: "rpc",
}


parse_datagram = BrDatagramParser.parse_datagram
