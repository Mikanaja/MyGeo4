# node_manager.py

import rpyc
from rpyc.utils.server import ThreadedServer
import threading
import time
from network_topology import NetworkTopology

class NodeManager:
    def __init__(self):
        self.topology = NetworkTopology()
        self.connections = {}
        self.is_coordinator = self.topology.is_coordinator()
        self.local_info = self.topology.get_local_node_info()
        self.server = None
        self.heartbeat_interval = 10  # segundos

    def start(self):
        """Inicia o gerenciador de nós."""
        self._start_server()
        self._connect_to_nodes()
        if not self.is_coordinator:
            self._start_heartbeat()

    def _start_server(self):
        """Inicia o servidor RPC."""
        if self.is_coordinator:
            service = CoordinatorService(self)
        else:
            service = StorageService(self)
        
        self.server = ThreadedServer(service, port=self.local_info[1], 
                                     protocol_config={'allow_public_attrs': True})
        threading.Thread(target=self.server.start, daemon=True).start()
        print(f"Servidor iniciado em {self.local_info}")

    def _connect_to_nodes(self):
        """Estabelece conexões com outros nós."""
        nodes_to_connect = ([self.topology.get_coordinator_node()] 
                            if not self.is_coordinator 
                            else self.topology.get_storage_nodes())
        
        for node in nodes_to_connect:
            if node != self.local_info:
                self._connect_to_node(node)

    def _connect_to_node(self, node):
        """Conecta a um nó específico."""
        try:
            conn = rpyc.connect(node[0], node[1])
            self.connections[node] = conn
            print(f"Conectado ao nó {node}")
        except Exception as e:
            print(f"Falha ao conectar ao nó {node}: {e}")

    def _start_heartbeat(self):
        """Inicia o heartbeat para o nó coordenador."""
        def send_heartbeat():
            while True:
                try:
                    coordinator = self.topology.get_coordinator_node()
                    if coordinator in self.connections:
                        self.connections[coordinator].root.heartbeat(self.local_info)
                    time.sleep(self.heartbeat_interval)
                except Exception as e:
                    print(f"Erro no heartbeat: {e}")
                    time.sleep(5)  # Espera antes de tentar novamente

        threading.Thread(target=send_heartbeat, daemon=True).start()

    def broadcast_to_storage_nodes(self, method, *args, **kwargs):
        """Transmite uma mensagem para todos os nós de armazenamento."""
        results = {}
        for node, conn in self.connections.items():
            if node in self.topology.get_storage_nodes():
                try:
                    result = getattr(conn.root, method)(*args, **kwargs)
                    results[node] = result
                except Exception as e:
                    print(f"Erro ao chamar {method} em {node}: {e}")
        return results

    def send_to_coordinator(self, method, *args, **kwargs):
        """Envia uma mensagem para o nó coordenador."""
        coordinator = self.topology.get_coordinator_node()
        if coordinator in self.connections:
            try:
                return getattr(self.connections[coordinator].root, method)(*args, **kwargs)
            except Exception as e:
                print(f"Erro ao chamar {method} no coordenador: {e}")
        return None

class CoordinatorService(rpyc.Service):
    def __init__(self, node_manager):
        self.node_manager = node_manager

    def exposed_heartbeat(self, node_info):
        print(f"Heartbeat recebido de {node_info}")
        # Aqui você pode adicionar lógica para atualizar o status do nó

class StorageService(rpyc.Service):
    def __init__(self, node_manager):
        self.node_manager = node_manager

    def exposed_store_fragment(self, fragment_id, data):
        # Implementar lógica de armazenamento
        print(f"Armazenando fragmento {fragment_id}")
        return True

    def exposed_retrieve_fragment(self, fragment_id):
        # Implementar lógica de recuperação
        print(f"Recuperando fragmento {fragment_id}")
        return b"fragment_data"  # Placeholder

if __name__ == "__main__":
    manager = NodeManager()
    manager.start()
    
    # Mantém o programa rodando
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Encerrando o nó...")
