import struct

from core.bitstream import BitStream
from br_side.constants import RPC as BrRpc
from samp_side.constants import RPC as SampRpc
from samp_side.packets import SampRpcBuilder


FIXED = {
    BrRpc.CREATE_PICKUP: (SampRpc.CREATE_PICKUP, 24, 'CreatePickup'),
    BrRpc.DESTROY_PICKUP: (SampRpc.DESTROY_PICKUP, 4, 'DestroyPickup'),
    BrRpc.WORLD_VEHICLE_REMOVE: (SampRpc.WORLD_VEHICLE_REMOVE, 2, 'WorldVehicleRemove'),
    BrRpc.SERVER_QUIT: (SampRpc.SERVER_QUIT, 3, 'ServerQuit'),
    BrRpc.EXIT_VEHICLE: (SampRpc.EXIT_VEHICLE, 4, 'ExitVehicle'),
    BrRpc.SCR_SET_PLAYER_POS: (SampRpc.SET_PLAYER_POS, 12, 'SetPlayerPos'),
    BrRpc.SCR_SET_PLAYER_POS_FIND_Z: (SampRpc.SET_PLAYER_POS_FIND_Z, 12, 'SetPlayerPosFindZ'),
    BrRpc.SCR_SET_PLAYER_FACING_ANGLE: (SampRpc.SET_PLAYER_FACING_ANGLE, 4, 'SetPlayerFacingAngle'),
    BrRpc.SCR_SET_PLAYER_SKIN: (SampRpc.SET_PLAYER_SKIN, 8, 'SetPlayerSkin'),
    BrRpc.SCR_SET_PLAYER_HEALTH: (SampRpc.SET_PLAYER_HEALTH, 4, 'SetPlayerHealth'),
    BrRpc.SCR_SET_PLAYER_ARMOUR: (SampRpc.SET_PLAYER_ARMOUR, 4, 'SetPlayerArmour'),
    BrRpc.SCR_SET_WEAPON_AMMO: (SampRpc.SET_WEAPON_AMMO, 3, 'SetWeaponAmmo'),
    BrRpc.SCR_SET_INTERIOR: (SampRpc.SET_INTERIOR, 1, 'SetInterior'),
    BrRpc.SCR_SET_CAMERA_POS: (SampRpc.SET_CAMERA_POS, 12, 'SetCameraPos'),
    BrRpc.SCR_DISABLE_MAP_ICON: (SampRpc.REMOVE_MAP_ICON, 1, 'RemoveMapIcon'),
    BrRpc.SCR_CLEAR_ANIMATIONS: (SampRpc.CLEAR_PLAYER_ANIMATION, 2, 'ClearPlayerAnimation'),
    BrRpc.SET_CHECKPOINT: (SampRpc.SET_CHECKPOINT, 16, 'SetCheckpoint'),
    BrRpc.SET_RACE_CHECKPOINT: (SampRpc.SET_RACE_CHECKPOINT, 29, 'SetRaceCheckpoint'),
    BrRpc.DISABLE_RACE_CHECKPOINT: (SampRpc.DISABLE_RACE_CHECKPOINT, 0, 'DisableRaceCheckpoint'),
    BrRpc.SCR_TOGGLE_PLAYER_CONTROLLABLE: (SampRpc.TOGGLE_PLAYER_CONTROLLABLE, 1, 'ToggleControllable'),
    BrRpc.GAME_MODE_RESTART: (SampRpc.GAME_MODE_RESTART, 0, 'GameModeRestart'),

    BrRpc.STOP_AUDIO_STREAM & 0x1FF: (SampRpc.STOP_AUDIO_STREAM, 0, 'StopAudioStream'),
}


PREFIX = {
    BrRpc.ENTER_VEHICLE: (SampRpc.ENTER_VEHICLE, 5),
    BrRpc.SCR_PUT_PLAYER_IN_VEHICLE: (SampRpc.PUT_PLAYER_IN_VEHICLE, 3),
    BrRpc.SCR_SET_PLAYER_VELOCITY: (SampRpc.SET_PLAYER_VELOCITY, 12),
}


def require_size(payload, bits, size):
    if bits != size * 8 or len(payload) != size:
        raise ValueError(f'expected {size} bytes, got {bits} bits')


class WorldRpcTranslator:
    def __init__(self):
        self.colors = {}
        self.labels = set()
        self.textdraws = set()

    def clear(self):
        self.colors.clear()
        self.labels.clear()
        self.textdraws.clear()

    def translate(self, rpc_id, payload, bits):

        result = self._payload(rpc_id, payload, bits)
        if result is None:
            return None
        target, data, bitlen = result
        return SampRpcBuilder.rpc_body(SampRpcBuilder.create(target, data, bitlen))

    def _payload(self, rpc_id, p, bits):
        def out(target, data=p, bitlen=None):
            return target, data, len(data) * 8 if bitlen is None else bitlen

        if rpc_id in FIXED:
            target, size, _ = FIXED[rpc_id]
            require_size(p, bits, size)
            if rpc_id == BrRpc.SERVER_QUIT:
                self.colors.pop(int.from_bytes(p[:2], 'little'), None)
            if rpc_id == BrRpc.GAME_MODE_RESTART:
                self.clear()
            return out(target)
        if rpc_id in PREFIX:
            target, size = PREFIX[rpc_id]
            if bits < size * 8:
                raise ValueError('truncated RPC prefix')
            return out(target, p[:size])


        if rpc_id == BrRpc.WORLD_PLAYER_REMOVE:
            if bits == 2 * 8:
                return out(SampRpc.WORLD_PLAYER_REMOVE)
            if bits == 6 * 8:
                return out(SampRpc.SET_VEHICLE_Z_ANGLE)
            raise ValueError('ambiguous RPC 325: expected 2 or 6 bytes')

        if rpc_id == BrRpc.DISABLE_CHECKPOINT and bits == 0:
            return out(SampRpc.DISABLE_CHECKPOINT)
        if rpc_id == BrRpc.SCR_GIVE_PLAYER_MONEY:
            if bits == 0:
                return out(SampRpc.REMOVE_PLAYER_FROM_VEHICLE)
            require_size(p, bits, 4)
            return out(SampRpc.GIVE_PLAYER_MONEY)
        if rpc_id == BrRpc.SCR_RESET_PLAYER_WEAPONS:
            if bits == 0:
                return out(SampRpc.RESET_PLAYER_WEAPONS)
            if bits < 104:
                raise ValueError('truncated SetVehicleVelocity')
            return out(SampRpc.SET_VEHICLE_VELOCITY, p[:13])
        if rpc_id == BrRpc.WORLD_PLAYER_DEATH:
            if bits == 16:
                return out(SampRpc.WORLD_PLAYER_DEATH)
            if bits < 112:
                raise ValueError('truncated SetVehiclePos')
            return out(SampRpc.SET_VEHICLE_POS, p[:14])
        if rpc_id == BrRpc.SCR_HIDE_TEXT_DRAW:
            require_size(p, bits, 2)
            self.textdraws.discard(int.from_bytes(p, 'little'))
            return out(SampRpc.HIDE_TEXT_DRAW)
        if rpc_id == BrRpc.SCR_DELETE_3D_TEXT_LABEL:
            require_size(p, bits, 2)
            self.labels.discard(int.from_bytes(p, 'little'))
            return out(SampRpc.DELETE_3D_TEXT_LABEL)
        if rpc_id == BrRpc.SCR_APPLY_ANIMATION and bits != 8:
            bs = BitStream(p)
            bs.read_uint16()
            bs.read_bytes(bs.read_uint8())
            bs.read_bytes(bs.read_uint8())
            bs.read_float()
            bs.read_bits(4)
            bs.read_uint32()
            if bs.bitpos != bits:
                raise ValueError('invalid ApplyAnimation bit length')
            return out(SampRpc.APPLY_PLAYER_ANIMATION, p, bits)

        if rpc_id == BrRpc.UPDATE_SCORES_PINGS_IPS:
            if bits % 80:
                raise ValueError('UpdateScoresAndPings must contain complete 10-byte records')

            return out(SampRpc.UPDATE_SCORES_PINGS_IPS)
        if rpc_id == BrRpc.WORLD_PLAYER_ADD:
            if bits < 248:
                raise ValueError('truncated WorldPlayerAdd')
            player, skin, x, y, z, angle, style, health, armour = struct.unpack_from('<HI4fB2f', p)


            data = struct.pack('<HBI4fIB11H', player, 255, skin, x, y, z, angle,
                               self.colors.get(player, 0xffffffff), style, *([1000] * 11))
            return out(SampRpc.WORLD_PLAYER_ADD, data)
        if rpc_id == BrRpc.WORLD_VEHICLE_ADD:
            if bits < 232:
                raise ValueError('truncated WorldVehicleAdd')


            tail = struct.pack('<IIBBB14BBII', 0, 0, 0, 0, 0, *([0] * 14), 255,
                               0xffffffff, 0xffffffff)
            return out(SampRpc.WORLD_VEHICLE_ADD, p[:29] + tail)
        if rpc_id == BrRpc.SCR_CREATE_3D_TEXT_LABEL:
            if bits < 216 or bits % 8:
                raise ValueError('truncated 3D label')
            text_bytes = p[27:].split(b'\0', 1)[0]
            if len(text_bytes) > 4095:
                raise ValueError('3D label exceeds SA-MP limit')
            bs = BitStream()
            bs.write_bytes(p[:27])
            bs.write_compressed_string(text_bytes.decode('cp1251', errors='replace'), 'cp1251')
            self.labels.add(int.from_bytes(p[:2], 'little'))
            return out(SampRpc.CREATE_3D_TEXT_LABEL, bs.get_bytes(), len(bs))
        if rpc_id == BrRpc.SCR_SET_MAP_ICON:
            if bits < 160:
                raise ValueError('truncated SetMapIcon')
            icon = int.from_bytes(p[:2], 'little')
            if icon >= 100:
                raise ValueError('map icon does not fit SA-MP slots 0..99')
            return out(SampRpc.SET_MAP_ICON, bytes([icon]) + p[2:20])
        if rpc_id == BrRpc.SET_PLAYER_NAME:
            if len(p) < 3:
                raise ValueError('truncated SetPlayerName')
            end = 3 + p[2]
            if bits != end * 8 + 1:
                raise ValueError('SetPlayerName must end with a BR success bit')
            bs = BitStream(p)
            name = bs.read_bytes(end)
            return out(SampRpc.SET_PLAYER_NAME, name + bytes([bs.read_bool()]))
        if rpc_id == BrRpc.SCR_GIVE_PLAYER_WEAPON:

            require_size(p, bits, 8)
            return out(SampRpc.GIVE_PLAYER_WEAPON)
        if rpc_id == BrRpc.SCR_VEHICLE_PARAMS:
            if bits < 16 + 7 * 32 or (bits - 16) % 32:
                raise ValueError('VehicleParams requires uint16 ID and 7..16 int32 fields')
            values = [v[0] for v in struct.iter_unpack('<i', p[2:])]
            if len(values) > 16 or any(v not in (-1, 0, 1) for v in values):
                raise ValueError('unknown vehicle parameter layout/value')
            values += [-1] * (16 - len(values))
            return out(SampRpc.SET_VEHICLE_PARAMS_EX, p[:2] + bytes(v & 255 for v in values))
        if rpc_id == BrRpc.SCR_PLAY_AUDIO_STREAM:
            if len(p) < 5 or bits < (5 + p[4]) * 8:
                raise ValueError('truncated audio URL')

            return out(SampRpc.PLAY_AUDIO_STREAM, p[4:5 + p[4]] + struct.pack('<4fB', 0, 0, 0, 0, 0))
        if rpc_id in (BrRpc.SCR_SHOW_TEXT_DRAW, BrRpc.SCR_TEXT_DRAW_SET_STRING):
            offset, width, target = {
                BrRpc.SCR_SHOW_TEXT_DRAW: (65, 2, SampRpc.SHOW_TEXT_DRAW),
                BrRpc.SCR_TEXT_DRAW_SET_STRING: (2, 2, SampRpc.TEXT_DRAW_SET_STRING),
            }[rpc_id]
            if len(p) < offset + width:
                raise ValueError('truncated text RPC header')
            length = int.from_bytes(p[offset:offset + width], 'little')
            require_size(p, bits, offset + width + length)
            if rpc_id == BrRpc.SCR_SHOW_TEXT_DRAW:
                self.textdraws.add(int.from_bytes(p[:2], 'little'))
            return out(target)
        return None
