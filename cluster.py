import rpyc
import time
import random
from typing import List, Dict, Any

class Cluster:
    def __init__(self, enderecos_datanodes, fator_replicacao, tamanho_fragmento):
        """
        Inicializa o cluster de datanodes
        
        Args:
            enderecos_datanodes (List[tuple]): Lista de endereços dos datanodes
            fator_replicacao (int): Número de réplicas para cada fragmento
            tamanho_fragmento (int): Tamanho máximo de um fragmento
        """
        self.enderecos_datanodes = enderecos_datanodes
        self.fator_replicacao = fator_replicacao
        self.tamanho_fragmento = tamanho_fragmento
        
        # Dicionário para armazenar informações dos datanodes
        self.data_nodes: Dict[str, Dict[str, Any]] = {
            f'data_node_{i+1}': {
                'addr': endereco, 
                'conn': None, 
                'status': {
                    'cpu': None, 
                    'memoria': None, 
                    'disco': None
                }
            }
            for i, endereco in enumerate(enderecos_datanodes)
        }
        
        # Tabela de índices para gerenciar metadados das imagens
        self.tabela_indice = {}

    def conectar_cluster(self):
        """Estabelece conexão com todos os datanodes"""
        for key, value in self.data_nodes.items():
            try:
                value['conn'] = self._conectar_datanode(value['addr'])
                print(f'[STATUS] Conexão estabelecida com {key}')
            except Exception as e:
                print(f'[ERRO] Falha ao conectar {key}: {e}')

    def _conectar_datanode(self, endereco):
        """
        Conecta a um datanode específico
        
        Args:
            endereco (tuple): Endereço (IP, porta) do datanode
        
        Returns:
            rpyc.Connection: Conexão com o datanode
        """
        tentativas = 0
        while tentativas < 3:
            try:
                ip, porta = endereco
                conexao = rpyc.connect(ip, porta)
                return conexao
            except Exception as e:
                print(f'[ERRO] Tentativa {tentativas + 1} de conexão falhou: {e}')
                tentativas += 1
                time.sleep(2)
        
        raise ConnectionError(f"Não foi possível conectar ao datanode {endereco}")

    def calcular_pontuacao_no(self, id_no):
        """
        Calcula pontuação de um datanode baseado em seus recursos
        
        Args:
            id_no (str): ID do datanode
        
        Returns:
            float: Pontuação do datanode
        """
        try:
            # Obtém status do datanode
            status = self.data_nodes[id_no]['conn'].root.obter_status_no()
            
            # Calcula pontuação (quanto menor o uso, melhor)
            return (
                (100 - status['cpu']) * 0.4 +
                (100 - status['memoria']) * 0.3 +
                (100 - status['disco']) * 0.3
            )
        except Exception as e:
            print(f'[ERRO] Falha ao calcular pontuação do {id_no}: {e}')
            return 0

    def selecionar_nos_armazenamento(self):
        """
        Seleciona nós para armazenamento de fragmentos
        
        Returns:
            List[str]: Lista de IDs de datanodes para armazenamento
        """
        # Ordena nós por pontuação
        nos_pontuados = [
            (id_no, self.calcular_pontuacao_no(id_no))
            for id_no in self.data_nodes
        ]
        
        # Ordena em ordem decrescente de pontuação
        nos_pontuados.sort(key=lambda x: x[1], reverse=True)
        
        # Retorna os melhores nós
        return [no for no, _ in nos_pontuados[:max(self.fator_replicacao, len(nos_pontuados))]]

    def selecionar_nos_recuperacao(self, nome_imagem):
        """
        Seleciona nós para recuperação de fragmentos de uma imagem
        
        Args:
            nome_imagem (str): Nome da imagem
        
        Returns:
            List[str]: Lista de IDs de datanodes para recuperação
        """
        nos_recuperacao = []
        
        for fragmento in self.tabela_indice.get(nome_imagem, []):
            # Seleciona o melhor nó entre os nós do fragmento
            nos_fragmento = fragmento['nodes']
            melhor_no = max(
                nos_fragmento, 
                key=lambda no: self.calcular_pontuacao_no(no)
            )
            nos_recuperacao.append(melhor_no)
        
        return nos_recuperacao

    def iniciar_tabela_indice(self, nome_imagem, divisoes):
        """
        Inicializa entrada na tabela de índices para uma nova imagem
        
        Args:
            nome_imagem (str): Nome da imagem
            divisoes (int): Número de fragmentos
        
        Returns:
            bool: Sucesso da operação
        """
        if nome_imagem in self.tabela_indice:
            return False
        
        # Inicializa entrada na tabela de índices
        self.tabela_indice[nome_imagem] = [
            {'nodes': [], 'size': None} 
            for _ in range(divisoes)
        ]
        
        return True

    def atualizar_tabela_indice(self, nome_imagem, indice_fragmento, tamanho_fragmento, nos):
        """
        Atualiza a tabela de índices com informações de um fragmento
        
        Args:
            nome_imagem (str): Nome da imagem
            indice_fragmento (int): Índice do fragmento
            tamanho_fragmento (int): Tamanho do fragmento
            nos (List[str]): Nós que armazenam o fragmento
        """
        fragmento = self.tabela_indice[nome_imagem][indice_fragmento]
        
        # Adiciona nós únicos
        for no in nos:
            if no not in fragmento['nodes']:
                fragmento['nodes'].append(no)
        
        # Atualiza tamanho do fragmento
        fragmento['size'] = tamanho_fragmento

    def tamanho_total_imagem(self, nome_imagem):
        """
        Calcula o tamanho total de uma imagem
        
        Args:
            nome_imagem (str): Nome da imagem
        
        Returns:
            int: Tamanho total da imagem
        """
        return sum(
            fragmento['size'] or 0 
            for fragmento in self.tabela_indice.get(nome_imagem, [])
        )