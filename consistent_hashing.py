import hashlib
import bisect

class ConsistentHashRing:
    def __init__(self, nodes=None, replicas=100):
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []
        self.nodes = set()  # Track unique physical nodes
        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node: str):
        if node in self.nodes:
            return
        self.nodes.add(node)
        for i in range(self.replicas):
            h_key = self._hash(f"{node}:{i}")
            self.ring[h_key] = node
            bisect.insort(self.sorted_keys, h_key)

    def remove_node(self, node: str):
        if node not in self.nodes:
            return
        self.nodes.remove(node)
        for i in range(self.replicas):
            h_key = self._hash(f"{node}:{i}")
            if h_key in self.ring:
                del self.ring[h_key]
                # Note: bisect doesn't have a fast remove, so we find and pop
                idx = bisect.bisect_left(self.sorted_keys, h_key)
                if idx < len(self.sorted_keys) and self.sorted_keys[idx] == h_key:
                    self.sorted_keys.pop(idx)

    def get_node(self, key: str) -> str:
        """Returns the primary physical node for a given key."""
        if not self.sorted_keys:
            return None
        h_key = self._hash(key)
        idx = bisect.bisect_left(self.sorted_keys, h_key)
        # Use modulo to wrap around the ring
        return self.ring[self.sorted_keys[idx % len(self.sorted_keys)]]

    def get_nodes(self, key: str, count=2) -> list:
        """
        Returns a list of 'count' unique physical nodes for a given key.
        This is used for Read-Repair/Replication logic.
        """
        if not self.sorted_keys or not self.nodes:
            return []
        
        # If the user asks for more nodes than exist, return all active nodes
        actual_count = min(count, len(self.nodes))
        
        h_key = self._hash(key)
        start_idx = bisect.bisect_left(self.sorted_keys, h_key)
        
        unique_targets = []
        # Walk clockwise around the ring
        for i in range(len(self.sorted_keys)):
            current_key = self.sorted_keys[(start_idx + i) % len(self.sorted_keys)]
            node = self.ring[current_key]
            
            if node not in unique_targets:
                unique_targets.append(node)
            
            if len(unique_targets) == actual_count:
                break
                
        return unique_targets