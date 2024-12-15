# coordinator_node.py

import rpyc
from rpyc.utils.server import ThreadedServer
import threading
import time
import json
from data_distributor import DataDistributor

class CoordinatorNode(rpyc.Service):
    def __init__(self):
        self.data_distributor = DataDistributor()
        self.image_metadata = {}
        self.storage_nodes = {}
        self.lock = threading.Lock()

    def exposed_register_storage_node(self, node_id, address):
        """
        Registra um novo nó de armazenamento.

        Args:
            node_id (str): ID único do nó de armazenamento
            address (tuple): Endereço (IP, porta) do nó

        Returns:
            bool: Sucesso do registro
        """
        with self.lock:
            self.storage_nodes[node_id] = {
                'address': address,
                'status': 'active',
                'last_heartbeat': time.time()
            }
        print(f"Nó de armazenamento registrado: {node_id} em {address}")
        return True

    def exposed_deregister_storage_node(self, node_id):
        """
        Remove o registro de um nó de armazenamento.

        Args:
            node_id (str): ID do nó a ser removido

        Returns:
            bool: Sucesso da remoção
        """
        with self.lock:
            if node_id in self.storage_nodes:
                del self.storage_nodes[node_id]
                print(f"Nó de armazenamento removido: {node_id}")
                return True
            return False

    def exposed_heartbeat(self, node_id):
        """
        Atualiza o status de um nó de armazenamento.

        Args:
            node_id (str): ID do nó

        Returns:
            bool: Sucesso da atualização
        """
        with self.lock:
            if node_id in self.storage_nodes:
                self.storage_nodes[node_id]['last_heartbeat'] = time.time()
                self.storage_nodes[node_id]['status'] = 'active'
                return True
            return False

    def exposed_initiate_upload(self, image_name, total_size):
        """
        Inicia o processo de upload de uma imagem.

        Args:
            image_name (str): Nome da imagem
            total_size (int): Tamanho total da imagem em bytes

        Returns:
            dict: Informações para o upload
        """
        fragment_size = 2 * 1024 * 1024  # 2 MB
        num_fragments = (total_size + fragment_size - 1) // fragment_size

        distribution = self.data_distributor.distribute_fragments(image_name, range(num_fragments))
        
        self.image_metadata[image_name] = {
            'total_size': total_size,
            'num_fragments': num_fragments,
            'fragment_size': fragment_size,
            'distribution': distribution
        }

        return {
            'fragment_size': fragment_size,
            'num_fragments': num_fragments,
            'distribution': distribution
        }

    def exposed_complete_upload(self, image_name):
        """
        Finaliza o processo de upload de uma imagem.

        Args:
            image_name (str): Nome da imagem

        Returns:
            bool: Sucesso da finalização
        """
        if image_name in self.image_metadata:
            self.image_metadata[image_name]['status'] = 'complete'
            print(f"Upload completo: {image_name}")
            return True
        return False

    def exposed_get_fragment_locations(self, image_name, fragment_id):
        """
        Obtém as localizações de um fragmento específico.

        Args:
            image_name (str): Nome da imagem
            fragment_id (int): ID do fragmento

        Returns:
            list: Lista de nós que contêm o fragmento
        """
        if image_name in self.image_metadata:
            return self.image_metadata[image_name]['distribution'].get(fragment_id, [])
        return []

    def exposed_initiate_download(self, image_name):
        """
        Inicia o processo de download de uma imagem.

        Args:
            image_name (str): Nome da imagem

        Returns:
            dict: Informações para o download
        """
        if image_name in self.image_metadata:
            metadata = self.image_metadata[image_name]
            return {
                'total_size': metadata['total_size'],
                'num_fragments': metadata['num_fragments'],
                'fragment_size': metadata['fragment_size'],
                'distribution': metadata['distribution']
            }
        return None

    def exposed_delete_image(self, image_name):
        """
        Deleta uma imagem do sistema.

        Args:
            image_name (str): Nome da imagem a ser deletada

        Returns:
            bool: Sucesso da deleção
        """
        if image_name in self.image_metadata:
            del self.image_metadata[image_name]
            print(f"Imagem deletada: {image_name}")
            return True
        return False

    def exposed_list_images(self):
        """
        Lista todas as imagens no sistema.

        Returns:
            list: Lista de nomes de imagens
        """
        return list(self.image_metadata.keys())

    def exposed_get_system_status(self):
        """
        Obtém o status geral do sistema.

        Returns:
            dict: Status do sistema
        """
        return {
            'active_nodes': sum(1 for node in self.storage_nodes.values() if node['status'] == 'active'),
            'total_nodes': len(self.storage_nodes),
            'total_images': len(self.image_metadata)
        }

    def _check_node_health(self):
        """Verifica a saúde dos nós de armazenamento periodicamente."""
        while True:
            with self.lock:
                current_time = time.time()
                for node_id, info in self.storage_nodes.items():
                    if current_time - info['last_heartbeat'] > 30:  # 30 segundos timeout
                        info['status'] = 'inactive'
                        print(f"Nó {node_id} marcado como inativo")
            time.sleep(10)

    def _save_metadata(self):
        """Salva os metadados em um arquivo."""
        with open('coordinator_metadata.json', 'w') as f:
            json.dump({
                'image_metadata': self.image_metadata,
                'storage_nodes': self.storage_nodes
            }, f)

    def _load_metadata(self):
        """Carrega os metadados de um arquivo."""
        try:
            with open('coordinator_metadata.json', 'r') as f:
                data = json.load(f)
                self.image_metadata = data['image_metadata']
                self.storage_nodes = data['storage_nodes']
        except FileNotFoundError:
            print("Arquivo de metadados não encontrado. Iniciando com estado vazio.")

def start_coordinator(port=8000):
    coordinator = CoordinatorNode()
    coordinator._load_metadata()
    
    # Inicia a verificação de saúde dos nós em uma thread separada
    health_check_thread = threading.Thread(target=coordinator._check_node_health, daemon=True)
    health_check_thread.start()

    server = ThreadedServer(coordinator, port=port, protocol_config={'allow_public_attrs': True})
    print(f"Coordinator Node iniciado na porta {port}")
    server.start()

if __name__ == "__main__":
    start_coordinator()
