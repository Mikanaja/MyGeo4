# data_distributor.py

import random
import hashlib
from network_topology import NetworkTopology

class DataDistributor:
    def __init__(self, replication_factor=2):
        self.topology = NetworkTopology()
        self.replication_factor = replication_factor

    def distribute_fragments(self, image_name, fragments):
        """
        Distribui os fragmentos de uma imagem entre os nós de armazenamento.
        
        Args:
            image_name (str): Nome da imagem
            fragments (list): Lista de fragmentos da imagem
        
        Returns:
            dict: Mapeamento de fragmentos para nós de armazenamento
        """
        distribution_map = {}
        available_nodes = self.topology.get_storage_nodes()

        for i, fragment in enumerate(fragments):
            selected_nodes = self._select_nodes_for_fragment(image_name, i, available_nodes)
            distribution_map[i] = selected_nodes

        return distribution_map

    def _select_nodes_for_fragment(self, image_name, fragment_index, available_nodes):
        """
        Seleciona nós para um fragmento específico usando um método de hash consistente.
        
        Args:
            image_name (str): Nome da imagem
            fragment_index (int): Índice do fragmento
            available_nodes (list): Lista de nós disponíveis
        
        Returns:
            list: Nós selecionados para o fragmento
        """
        hash_key = f"{image_name}_{fragment_index}"
        hash_value = self._consistent_hash(hash_key)
        
        # Ordena os nós com base na proximidade do hash
        sorted_nodes = sorted(available_nodes, 
                              key=lambda node: self._consistent_hash(str(node)))
        
        selected_nodes = []
        start_index = sorted_nodes.index(min(sorted_nodes, 
                                             key=lambda node: abs(self._consistent_hash(str(node)) - hash_value)))
        
        for i in range(self.replication_factor):
            node_index = (start_index + i) % len(sorted_nodes)
            selected_nodes.append(sorted_nodes[node_index])
        
        return selected_nodes

    def _consistent_hash(self, key):
        """
        Implementa um hash consistente para distribuição equilibrada.
        
        Args:
            key (str): Chave para hash
        
        Returns:
            int: Valor de hash
        """
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def get_fragment_locations(self, image_name, total_fragments):
        """
        Determina as localizações dos fragmentos de uma imagem.
        
        Args:
            image_name (str): Nome da imagem
            total_fragments (int): Número total de fragmentos
        
        Returns:
            dict: Mapeamento de índices de fragmentos para nós de armazenamento
        """
        return {i: self._select_nodes_for_fragment(image_name, i, self.topology.get_storage_nodes())
                for i in range(total_fragments)}

    def rebalance_data(self):
        """
        Reequilibra os dados entre os nós de armazenamento.
        Esta é uma implementação simplificada e deve ser expandida conforme necessário.
        """
        print("Iniciando rebalanceamento de dados...")
        # Aqui você implementaria a lógica para redistribuir os dados
        # entre os nós de armazenamento para manter o equilíbrio
        print("Rebalanceamento concluído.")

    def handle_node_failure(self, failed_node):
        """
        Lida com a falha de um nó, redistribuindo seus dados.
        
        Args:
            failed_node (tuple): Endereço do nó que falhou
        """
        print(f"Lidando com falha do nó: {failed_node}")
        # Implementar lógica para redistribuir dados do nó falho
        # para outros nós disponíveis
        print("Recuperação de falha concluída.")

    def optimize_distribution(self):
        """
        Otimiza a distribuição de dados com base no uso e desempenho dos nós.
        Esta é uma implementação simplificada.
        """
        print("Otimizando distribuição de dados...")
        # Implementar lógica para otimizar a distribuição
        # com base em métricas de desempenho e uso dos nós
        print("Otimização concluída.")

# Exemplo de uso
if __name__ == "__main__":
    distributor = DataDistributor(replication_factor=2)
    
    # Simulando fragmentos de uma imagem
    image_name = "exemplo.jpg"
    fragments = [b"fragmento1", b"fragmento2", b"fragmento3"]
    
    distribution = distributor.distribute_fragments(image_name, fragments)
    print(f"Distribuição dos fragmentos: {distribution}")
    
    locations = distributor.get_fragment_locations(image_name, len(fragments))
    print(f"Localizações dos fragmentos: {locations}")
    
    distributor.rebalance_data()
    distributor.handle_node_failure(('192.168.1.101', 9001))
    distributor.optimize_distribution()
