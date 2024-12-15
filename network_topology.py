# network_topology.py

import socket
import random

class NetworkTopology:
    def __init__(self):
        self._storage_nodes = [
            ('192.168.1.101', 9001),
            ('192.168.1.102', 9002),
            ('192.168.1.103', 9003),
            ('192.168.1.104', 9004)
        ]
        self._coordinator_node = ('192.168.1.100', 6000)
        self._local_ip = self._detect_local_ip()

    def _detect_local_ip(self):
        """Detecta o IP local da máquina."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect(("8.8.8.8", 80))
            local_ip = sock.getsockname()[0]
            sock.close()
            return local_ip
        except Exception:
            return "127.0.0.1"

    def get_storage_nodes(self):
        """Retorna a lista de nós de armazenamento."""
        return self._storage_nodes

    def get_coordinator_node(self):
        """Retorna o endereço do nó coordenador."""
        return self._coordinator_node

    def is_coordinator(self):
        """Verifica se o nó atual é o coordenador."""
        return self._local_ip == self._coordinator_node[0]

    def get_local_node_info(self):
        """Retorna informações sobre o nó local."""
        if self.is_coordinator():
            return self._coordinator_node
        for node in self._storage_nodes:
            if node[0] == self._local_ip:
                return node
        return None

    def get_random_storage_node(self):
        """Retorna um nó de armazenamento aleatório."""
        return random.choice(self._storage_nodes)

    def get_other_storage_nodes(self):
        """Retorna todos os nós de armazenamento exceto o local."""
        return [node for node in self._storage_nodes if node[0] != self._local_ip]

    def node_count(self):
        """Retorna o número total de nós no cluster."""
        return len(self._storage_nodes) + 1  # +1 para o coordenador

    def get_node_type(self):
        """Retorna o tipo do nó atual (coordenador ou armazenamento)."""
        return "Coordinator" if self.is_coordinator() else "Storage"

    def update_node_status(self, node_address, is_active):
        """Atualiza o status de um nó no cluster."""
        # Esta é uma implementação simplificada. Em um sistema real,
        # você precisaria de um mecanismo mais robusto para gerenciar o estado dos nós.
        pass

    def __str__(self):
        """Retorna uma representação em string da topologia da rede."""
        return f"Cluster com {self.node_count()} nós: 1 Coordenador e {len(self._storage_nodes)} nós de armazenamento"

# Exemplo de uso
if __name__ == "__main__":
    topology = NetworkTopology()
    print(topology)
    print(f"Nó local: {topology.get_local_node_info()}")
    print(f"Tipo de nó: {topology.get_node_type()}")
    print(f"Nós de armazenamento: {topology.get_storage_nodes()}")
    print(f"Nó coordenador: {topology.get_coordinator_node()}")
