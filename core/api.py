from core import logger
from br_side.constants import (
    OutgoingPacketID as BrOutID,
    PacketReliability as BrRel,
)
from br_side.packets import (
    BrBodyBuilder,
    BrRpcBuilder,
    InterfaceSyncPayloadBuilder,
)
from samp_side.constants import (
    RPC as SampRpc,
    PacketReliability as SampRel,
    PacketPriority as SampPriority,
)
from samp_side.packets import (
    SampBodyBuilder,
    SampRpcBuilder,
)


class ScriptApi:

    def __init__(self, relay):
        self._relay = relay

    def send_to_br(self, body, reliability=BrRel.RELIABLE, ordering_channel=0):
        return self._relay._send_entries_to_br([{
            'body': bytes(body),
            'reliability': int(reliability),
            'ordering_channel': int(ordering_channel) & 0x0F,
        }])

    def send_to_samp(self, body, reliability=SampRel.RELIABLE,
                      ordering_channel=0, priority=SampPriority.MEDIUM_PRIORITY):
        self._relay._queue_samp_entry({
            'body': bytes(body),
            'reliability': int(reliability),
            'ordering_channel': int(ordering_channel) & 0x1F,
        }, int(priority))

    def send_br_rpc(self, rpc_id, payload=b'', reliability=BrRel.RELIABLE):
        frame = BrRpcBuilder.create(int(rpc_id), bytes(payload or b''))
        return self.send_to_br(
            BrBodyBuilder.create(BrOutID.RPC, frame), reliability, 0)

    def send_samp_rpc(self, rpc_id, payload=b'',
                      reliability=SampRel.RELIABLE, priority=SampPriority.MEDIUM_PRIORITY):
        frame = SampRpcBuilder.create(int(rpc_id), bytes(payload or b''))
        self.send_to_samp(
            SampBodyBuilder.rpc_body(frame), reliability, 0, priority)

    def send_br_interface(self, gui_id, data: dict,
                          reliability=BrRel.RELIABLE_ORDERED):
        inner = InterfaceSyncPayloadBuilder.create(int(gui_id), dict(data))
        return self.send_to_br(
            BrBodyBuilder.create(BrOutID.USER_INTERFACE_SYNC, inner),
            reliability, 0)

    def send_samp_chat(self, text: str, color: int = 0xFFFFFFFF):
        payload = SampRpcBuilder.client_message_payload(color, str(text))
        self.send_samp_rpc(SampRpc.CLIENT_MESSAGE, payload)

    def log(self, msg):
        logger.info(f'[script] {msg}')
