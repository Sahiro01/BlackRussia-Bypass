import math
import struct

from core.bitstream import BitStream


CLIENT_SYNC_IDS = {207: 0x1e, 200: 0x22, 211: 0x05}
SERVER_SYNC_IDS = {0x1e: 207, 0xc8: 200, 0xd3: 211}


def client_sync(packet_id, p):

    size = {207: 68, 200: 63, 211: 24}[packet_id]
    if len(p) != size:
        raise ValueError(f'sync {packet_id}: expected {size} bytes, got {len(p)}')
    if packet_id == 207:

        animation = struct.unpack_from('<h', p, 64)[0]
        p = p[:38] + b'\0\0' + p[38:64] + struct.pack('<i', animation) + p[66:68]
    elif packet_id == 200:
        p += b'\0\0'
    return bytes([CLIENT_SYNC_IDS[packet_id]]) + p


def _copy(src, dst, count):
    dst.write_bits(src.read_bits(count), count)


def _vector(src, dst):
    for _ in range(3):
        value = src.read_float()
        if not math.isfinite(value):
            raise ValueError('non-finite sync coordinate')
        dst.write_float(value)


def _velocity(src, dst):
    magnitude = src.read_float()
    if not math.isfinite(magnitude) or magnitude < 0:
        raise ValueError('invalid velocity magnitude')
    dst.write_float(magnitude)
    if magnitude:
        _copy(src, dst, 48)


def _percent(value):
    return 15 if value >= 100 else max(0, value // 7)


def server_sync(packet_id, p):

    src, dst = BitStream(p), BitStream()
    dst.write_uint8(SERVER_SYNC_IDS[packet_id])
    _copy(src, dst, 16)
    if packet_id == 0x1e:
        for _ in range(2):
            present = src.read_bool()
            dst.write_bool(present)
            if present:
                _copy(src, dst, 16)
        _copy(src, dst, 16)
        _vector(src, dst)
        _copy(src, dst, 52)
        health, armour = src.read_uint16(), src.read_uint16()
        dst.write_uint8((_percent(health) << 4) | _percent(armour))
        _copy(src, dst, 16)
        _velocity(src, dst)
        surf = src.read_bool()
        dst.write_bool(surf)
        if surf:
            _copy(src, dst, 16)
            _vector(src, dst)
        anim = src.read_bool()
        dst.write_bool(anim)
        if anim:
            _copy(src, dst, 32)
    elif packet_id == 0xc8:
        _copy(src, dst, 64)
        _copy(src, dst, 52)
        _vector(src, dst)
        _velocity(src, dst)
        _copy(src, dst, 24)


        dst.write_uint8(0)
        dst.write_bits(0, 4)
    else:
        _copy(src, dst, 16)
        drive_by, seat = src.read_bits(1), src.read_bits(7)
        if seat > 63:
            raise ValueError('BR passenger seat exceeds SA-MP six-bit field')
        dst.write_bits(drive_by, 2)
        dst.write_bits(seat, 6)
        _copy(src, dst, 72)
        _vector(src, dst)
    return dst.get_bytes()
