import struct

from core.bitstream import BitStream
from core import logger
from .constants import (
    MAX_DATAGRAM_BYTES,
    PacketID,
    PacketReliability,
    RPC as SampRpcID,
    ORDERING_METADATA_RELIABILITIES,
)


def _has_ordering(reliability: int) -> bool:
    return reliability in ORDERING_METADATA_RELIABILITIES


class SampDatagramBuilder:


    def __init__(self):
        self.message_number = 0
        self.split_packet_id = 0
        self.ordering_indices = [0] * 32

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
        split_id: int | None = None,
        split_index: int | None = None,
        split_count: int | None = None,
    ) -> bytes:
        if message_number is None:
            message_number = self._next_message_number()

        bs = BitStream()


        if is_first_packet:
            bs.write_bool(has_ack)

        bs.write_uint16(message_number)
        bs.write_bits(int(reliability) & 0x0F, 4)

        if _has_ordering(reliability):
            ordering_channel &= 0x1F
            bs.write_bits(ordering_channel, 5)
            if ordering_index is None:
                ordering_index = self.ordering_indices[ordering_channel]
                self.ordering_indices[ordering_channel] = (ordering_index + 1) & 0xffff
            bs.write_uint16(ordering_index)

        is_split = split_id is not None
        bs.write_bool(is_split)

        if is_split:

            bs.write_uint16(split_id & 0xFFFF)
            bs.write_compressed(split_index, 32, unsigned=True)
            bs.write_compressed(split_count, 32, unsigned=True)

        wire_body = bytes(body)
        bs.write_compressed(len(wire_body) * 8, 16, unsigned=True)
        bs.align_to_byte()
        bs.write_bytes(wire_body)

        return bs.get_bytes()

    def _split_entries(self, entry: dict) -> list[dict]:

        body = bytes(entry["body"])
        reliability = int(entry.get("reliability", PacketReliability.RELIABLE))
        ordering_channel = int(entry.get("ordering_channel", 0))

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
            f"samp outgoing split: {len(body)}B -> {split_count} "
            f"fragments x {chunk_size}B"
        )

        return fragments

    def build_batches(self, entries: list[dict]) -> list[tuple[bytes, list[dict]]]:


        batches = []
        chunks = []
        records = []
        size = 0
        for original in entries:
            entry = dict(original)
            reliability = int(entry.get('reliability', PacketReliability.RELIABLE))
            channel = int(entry.get('ordering_channel', 0)) & 0x1f
            order = self.ordering_indices[channel] if _has_ordering(reliability) else None
            entry['ordering_index'] = order
            preview = self._build_internal_packet(
                entry['body'], reliability, channel, is_first_packet=True, has_ack=False,
                message_number=0, ordering_index=order,
                split_id=entry.get('split_id'), split_index=entry.get('split_index'),
                split_count=entry.get('split_count'))
            flat = (self._split_entries(entry) if len(preview) > MAX_DATAGRAM_BYTES
                    and 'split_id' not in entry else [entry])
            if _has_ordering(reliability):
                self.ordering_indices[channel] = (order + 1) & 0xffff
            for fragment in flat:
                number = self._next_message_number()
                kwargs = dict(body=fragment['body'], reliability=reliability,
                              ordering_channel=channel, has_ack=False,
                              message_number=number, ordering_index=order,
                              split_id=fragment.get('split_id'),
                              split_index=fragment.get('split_index'),
                              split_count=fragment.get('split_count'))
                standalone = self._build_internal_packet(is_first_packet=True, **kwargs)
                if len(standalone) > MAX_DATAGRAM_BYTES:
                    raise ValueError('SAMP InternalPacket exceeds datagram limit')
                compact = self._build_internal_packet(is_first_packet=False, **kwargs)
                wire = compact if chunks else standalone
                if chunks and size + len(wire) > MAX_DATAGRAM_BYTES:
                    batches.append((b''.join(chunks), records))
                    chunks, records, size = [], [], 0
                    wire = standalone
                chunks.append(wire)
                size += len(wire)
                records.append({'message_number': number, 'reliability': reliability,
                                'data': standalone})
        if chunks:
            batches.append((b''.join(chunks), records))
        return batches

    def build_datagrams(self, entries: list[dict]) -> list[bytes]:
        return [wire for wire, _ in self.build_batches(entries)]

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


class SampBodyBuilder:

    @staticmethod
    def create(packet_id: int, payload: bytes = b"") -> bytes:
        return bytes([packet_id & 0xFF]) + (payload or b"")

    @staticmethod
    def open_connection_reply() -> bytes:

        return SampBodyBuilder.create(PacketID.OPEN_CONNECTION_REPLY)

    @staticmethod
    def connection_request_accepted(
        binary_address: int,
        port: int,
        player_index: int,
        server_challenge: int,
    ) -> bytes:
        bs = BitStream()
        bs.write_uint32(binary_address & 0xFFFFFFFF)
        bs.write_uint16(port & 0xFFFF)
        bs.write_uint16(player_index & 0xFFFF)
        bs.write_uint32(server_challenge & 0xFFFFFFFF)
        return SampBodyBuilder.create(
            PacketID.CONNECTION_REQUEST_ACCEPTED,
            bs.get_bytes(),
        )

    @staticmethod
    def auth_key_body(key: str | bytes) -> bytes:
        key_bytes = (
            key.encode("ascii", errors="ignore")
            if isinstance(key, str)
            else bytes(key)
        )
        return SampBodyBuilder.create(
            PacketID.AUTH_KEY,
            bytes([len(key_bytes) & 0xFF]) + key_bytes,
        )

    @staticmethod
    def internal_ping(sender_ms: int) -> bytes:
        return SampBodyBuilder.create(
            PacketID.INTERNAL_PING,
            struct.pack("<I", sender_ms & 0xFFFFFFFF),
        )

    @staticmethod
    def connected_pong(receiver_ms: int, sender_ms: int) -> bytes:
        return SampBodyBuilder.create(
            PacketID.CONNECTED_PONG,
            struct.pack("<I", sender_ms & 0xFFFFFFFF)
            + struct.pack("<I", receiver_ms & 0xFFFFFFFF),
        )

    @staticmethod
    def disconnection_notification() -> bytes:
        return SampBodyBuilder.create(PacketID.DISCONNECTION_NOTIFICATION)

    @staticmethod
    def new_incoming_connection(server_ip: str, server_port: int) -> bytes:
        return SampBodyBuilder.create(
            PacketID.NEW_INCOMING_CONNECTION,
            socket_inet_aton(server_ip) + struct.pack(">H", server_port & 0xFFFF),
        )


def socket_inet_aton(ip: str) -> bytes:
    import socket as _socket
    return _socket.inet_aton(str(ip))


class SampRpcBuilder:

    @staticmethod
    def spawn_info_payload(team=0, skin=78, unused=0, x=0.0, y=0.0,
                           z=0.0, rotation=0.0, weapons=(0, 0, 0), ammo=(0, 0, 0)) -> bytes:
        return struct.pack('<BIB4f6I', team, skin, unused, x, y, z, rotation,
                           *weapons, *ammo)

    @staticmethod
    def request_class_payload(response=1, **spawn_info) -> bytes:
        return bytes([response]) + (SampRpcBuilder.spawn_info_payload(**spawn_info)
                                    if response else b'')

    @staticmethod
    def request_spawn_payload(response: int) -> bytes:
        if response not in (0, 1, 2):
            raise ValueError('invalid spawn response')
        return bytes([response])

    @staticmethod
    def create(rpc_id: int, payload: bytes = b"", payload_bitlen: int | None = None) -> bytes:
        bitlen = len(payload) * 8 if payload_bitlen is None else payload_bitlen
        if not 0 <= bitlen <= len(payload) * 8 or bitlen > 65535:
            raise ValueError("invalid RPC payload bit length")
        bs = BitStream()
        bs.write_bits(rpc_id & 0xFF, 8)
        bs.write_bits(0b11, 2)
        bs.write_compressed(bitlen, 16, unsigned=True)
        if bitlen:
            bs.write_bits(int.from_bytes(payload, "big") >> (len(payload) * 8 - bitlen), bitlen)
        return bs.get_bytes()

    @staticmethod
    def rpc_body(packet_body: bytes) -> bytes:
        return SampBodyBuilder.create(PacketID.RPC, packet_body)

    @staticmethod
    def server_join(
        player_id: int,
        color: int,
        is_npc: int,
        name: str,
    ) -> bytes:

        bs = BitStream()
        bs.write_uint16(player_id & 0xFFFF)
        bs.write_uint32(color & 0xFFFFFFFF)
        bs.write_uint8(is_npc & 0xFF)
        name_bytes = name.encode("cp1251", errors="ignore")[:255]
        bs.write_uint8(len(name_bytes))
        bs.write_bytes(name_bytes)
        return SampRpcBuilder.create(SampRpcID.SERVER_JOIN, bs.get_bytes())


    @staticmethod
    def client_message_payload(color: int, message: str) -> bytes:


        text = str(message).encode("cp1251", errors="ignore")[:144]

        bs = BitStream()
        bs.write_uint32(color & 0xFFFFFFFF)
        bs.write_uint32(len(text))
        bs.write_bytes(text)

        return bs.get_bytes()

    @staticmethod
    def dialog_box_payload(
        dialog_id: int,
        style: int,
        title: str,
        button1: str,
        button2: str,
        info: str,
    ) -> bytes:


        bs = BitStream()
        bs.write_uint16(dialog_id & 0xFFFF)
        bs.write_uint8(style & 0xFF)

        for text in (title, button1, button2):
            encoded = str(text).encode("cp1251", errors="ignore")
            bs.write_uint8(len(encoded) & 0xFF)
            bs.write_bytes(encoded)

        bs.write_compressed_string(str(info), "cp1251")

        return bs.get_bytes()


def log_build(datagram: bytes, note: str = "") -> None:
    logger.packet("SAMP>>", f"{len(datagram)}B {datagram[:24].hex()} {note}")
