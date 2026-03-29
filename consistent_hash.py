import bisect
import hashlib


class ConsistentHashRing:
    def __init__(self, nodes, virtual_nodes=50):
        if not nodes:
            raise ValueError("nodes must not be empty")
        # avoid duplicate
        self.nodes = list(dict.fromkeys(nodes))
        self.virtual_nodes = max(1, int(virtual_nodes))
        
        self._ring = []     # ring of virtual node tokens
        self._owners = {}   # token -> node mapping

        for node in self.nodes:
            for replica in range(self.virtual_nodes):
                token = self._hash(f"{node}#{replica}")
                self._ring.append(token)
                self._owners[token] = node

        self._ring.sort()

    @staticmethod
    def _hash(value):
        return int(hashlib.sha256(value.encode("utf-8")).hexdigest(), 16)

    def get_node(self, key):
        if not self._ring:
            raise RuntimeError("hash ring is empty")

        key_hash = self._hash(key)
        idx = bisect.bisect_left(self._ring, key_hash)
        if idx == len(self._ring):
            idx = 0
        return self._owners[self._ring[idx]]
