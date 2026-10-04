from collections import deque
from .constants import PacketReliability as Rel, RELIABLE_RELIABILITIES

class BrReceiveState:
    def __init__(self, reliable_types=RELIABLE_RELIABILITIES):
        self.reliable_types = reliable_types
        self.highest = None
        self.seen = set()
        self.seen_order = deque()
        self.ordered = [0] * 16
        self.sequenced = [0] * 16
        self.buffers = [{} for _ in range(16)]

    def is_duplicate(self, packet):
        raw = packet['message_number']
        if self.highest is None:
            extended = raw
        else:
            delta = ((raw - (self.highest & 0xffff) + 0x8000) & 0xffff) - 0x8000
            extended = self.highest + delta
        if self.highest is None or extended > self.highest:
            self.highest = extended
        if packet['reliability'] not in self.reliable_types:
            return False
        if extended in self.seen:
            return True
        self.seen.add(extended)
        self.seen_order.append(extended)
        while len(self.seen_order) > 8192:
            self.seen.discard(self.seen_order.popleft())
        return False

    def deliver(self, packet):
        reliability = packet['reliability']
        if reliability not in (Rel.RELIABLE_ORDERED, Rel.RELIABLE_SEQUENCED,
                               Rel.UNRELIABLE_SEQUENCED):
            return [packet]
        channel = packet['ordering_channel']
        index = packet['ordering_index']
        if channel is None or index is None or not 0 <= channel < 16:
            return []
        indices = self.ordered if reliability == Rel.RELIABLE_ORDERED else self.sequenced
        expected = indices[channel]
        delta = (index - expected) & 0xffff
        if delta >= 0x8000:
            return []
        if reliability != Rel.RELIABLE_ORDERED:
            indices[channel] = (index + 1) & 0xffff
            return [packet]
        buffer = self.buffers[channel]
        if delta:
            if len(buffer) >= 8192 and index not in buffer:
                raise ValueError('BR ordered receive buffer exhausted; reconnect required')
            buffer.setdefault(index, packet)
            return []
        result = [packet]
        expected = (expected + 1) & 0xffff
        while expected in buffer:
            result.append(buffer.pop(expected))
            expected = (expected + 1) & 0xffff
        indices[channel] = expected
        return result
