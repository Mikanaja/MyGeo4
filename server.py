import os
import rpyc
import math
import time
import socket
from rpyc.utils.server import ThreadedServer

# Função para encontrar uma porta livre
def encontrar_porta_livre(porta_inicial=5000, max_tentativas=10):
    """
    Encontra uma porta livre para o servidor
    
    Args:
        porta_inicial (int): Porta para iniciar a busca
        max_tentativas (int): Número máximo de tentativas
    
    Returns:
        int: Porta livre encontrada
    """
    for tentativa in range(max_tentativas):
        porta = porta_inicial + tentativa
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('localhost', porta))
                return porta
        except OSError:
            continue
    
    raise Exception(f"Não foi possível encontrar uma porta livre após {max_tentativas} tentativas")

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
        self.data_nodes = {
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
                ip, porta = value['addr']
                value['conn'] = rpyc.connect(ip, porta)
                print(f'[STATUS] Conexão estabelecida com {key}')
            except Exception as e:
                print(f'[ERRO] Falha ao conectar {key}: {e}')

    def selecionar_nos_armazenamento(self):
        """
        Seleciona nós para armazenamento de fragmentos
        
        Returns:
            List[str]: Lista de IDs de datanodes para armazenamento
        """
        # Retorna os primeiros nós de acordo com o fator de replicação
        return list(self.data_nodes.keys())[:self.fator_replicacao]

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

class Server(rpyc.Service):
    ENDERECOS_DATANODES = [
        ('localhost', 8001),
        ('localhost', 8002),
        ('localhost', 8003),
        ('localhost', 8004)
    ]
    
    FATOR_REPLICACAO = 2
    TAMANHO_FRAGMENTO = 2_097_152  # 2 MB

    def __init__(self, porta=None):
        # Se nenhuma porta for fornecida, encontra uma livre
        self.porta = porta if porta is not None else encontrar_porta_livre()
        
        self.cluster = Cluster(
            self.ENDERECOS_DATANODES, 
            self.FATOR_REPLICACAO, 
            self.TAMANHO_FRAGMENTO
        )
        self.cluster.conectar_cluster()
        self._reiniciar_estado_upload()

    def _reiniciar_estado_upload(self):
        """Reinicia variáveis de estado de upload"""
        self.nome_imagem_atual = None
        self.tamanho_imagem = None
        self.indice_fragmento = None
        self.tamanho_fragmento_acumulado = 0
        self.tamanho_imagem_acumulado = 0
        self.divisoes_imagem = None
        self.nos_selecionados = None

    def exposed_iniciar_upload(self, nome_imagem, tamanho_imagem):
        """
        Inicia o processo de upload de uma imagem
        
        Args:
            nome_imagem (str): Nome da imagem
            tamanho_imagem (int): Tamanho total da imagem
        
        Returns:
            dict: Informações de configuração do upload
        """
        try:
            # Verifica se a imagem já existe
            if not self.cluster.iniciar_tabela_indice(nome_imagem, math.ceil(tamanho_imagem / self.TAMANHO_FRAGMENTO)):
                return {
                    'sucesso': False, 
                    'mensagem': "Imagem já existente no cluster"
                }

            # Configura estado de upload
            self.nome_imagem_atual = nome_imagem
            self.tamanho_imagem = tamanho_imagem
            self.divisoes_imagem = math.ceil(tamanho_imagem / self.TAMANHO_FRAGMENTO)
            self.indice_fragmento = 0
            self.tamanho_fragmento_acumulado = 0
            self.tamanho_imagem_acumulado = 0

            # Seleciona nós para armazenamento
            self.nos_selecionados = self.cluster.selecionar_nos_armazenamento()

            return {
                'sucesso': True,
                'divisoes': self.divisoes_imagem,
                'tamanho_fragmento': self.TAMANHO_FRAGMENTO,
                'nos': self.nos_selecionados
            }

        except Exception as e:
            return {
                'sucesso': False, 
                'mensagem': str(e)
            }

    def exposed_upload_fragmento(self, fragmento, indice_fragmento):
        """
        Carrega um fragmento de imagem com mais robustez
        
        Args:
            fragmento (bytes): Fragmento da imagem
            indice_fragmento (int): Índice do fragmento
        
        Returns:
            dict: Resultado do upload do fragmento
        """
        try:
            # Verifica se o upload foi iniciado
            if not hasattr(self, 'nome_imagem_atual'):
                return {
                    'sucesso': False, 
                    'mensagem': "Upload não iniciado"
                }

            tamanho_fragmento = len(fragmento)

            # Envia fragmento para os nós selecionados
            for id_no in self.nos_selecionados:
                no = self.cluster.data_nodes[id_no]
                sucesso = no['conn'].root.armazenar_fragmento_imagem(
                    self.nome_imagem_atual,
                    indice_fragmento, 
                    fragmento
                )
                
                if not sucesso:
                    return {
                        'sucesso': False, 
                        'mensagem': f"Falha ao armazenar no nó {id_no}"
                    }

            # Atualiza tabela de índices
            self.cluster.atualizar_tabela_indice(
                self.nome_imagem_atual, 
                indice_fragmento, 
                tamanho_fragmento, 
                self.nos_selecionados
            )

            return {
                'sucesso': True,
                'mensagem': f"Fragmento {indice_fragmento} enviado"
            }

        except Exception as e:
            print(f"[ERRO] Upload de fragmento: {e}")
            return {
                'sucesso': False, 
                'mensagem': str(e)
            }

    def exposed_finalizar_upload(self):
        """
        Finaliza o processo de upload
        
        Returns:
            dict: Resultado da finalização
        """
        try:
            if not hasattr(self, 'nome_imagem_atual'):
                return {
                    'sucesso': False, 
                    'mensagem': "Nenhum upload em andamento"
                }

            # Limpa estado de upload
            nome_imagem = self.nome_imagem_atual
            del self.nome_imagem_atual

            return {
                'sucesso': True,
                'mensagem': f"Upload da imagem {nome_imagem} concluído"
            }

        except Exception as e:
            return {
                'sucesso': False, 
                'mensagem': str(e)
            }

    def exposed_download_imagem(self, nome_imagem):
        """
        Baixa uma imagem do cluster
        
        Args:
            nome_imagem (str): Nome da imagem a ser baixada
        
        Returns:
            tuple: (sucesso, mensagem, tamanho_imagem)
        """
        try:
            if nome_imagem not in self.cluster.tabela_indice:
                return False, "Imagem não encontrada", None

            # Configura estado de download
            self.nome_imagem_atual = nome_imagem
            self.indice_fragmento = 0
            self.tamanho_imagem = self.cluster.tamanho_total_imagem(nome_imagem)

            return True, None, self.tamanho_imagem

        except Exception as e:
            return False, str(e), None

    def exposed_recuperar_fragmento(self):
        """
        Recupera um fragmento da imagem
        
        Returns:
            bytes ou str: Fragmento da imagem ou 'erro'
        """
        try:
            fragmento = None
            
            if self.indice_fragmento < len(self.cluster.tabela_indice[self.nome_imagem_atual]):
                no_id = self.cluster.tabela_indice[self.nome_imagem_atual][self.indice_fragmento]['nodes'][0]
                no = self.cluster.data_nodes[no_id]
                
                fragmento, fim = no['conn'].root.recuperar_fragmento_imagem(
                    self.nome_imagem_atual, 
                    self.indice_fragmento
                )
                
                if fim:
                    self.indice_fragmento += 1
            
            return fragmento or 'erro'

        except Exception as e:
            print(f"[ERRO] Recuperação de fragmento: {e}")
            return 'erro'

    def exposed_listar_imagens(self):
        """
        Lista imagens disponíveis no cluster
        
        Returns:
            list: Nomes das imagens
        """
        return list(self.cluster.tabela_indice.keys())

    def exposed_deletar_imagem(self, nome_imagem):
        """
        Deleta uma imagem do cluster
        
        Args:
            nome_imagem (str): Nome da imagem a ser deletada
        
        Returns:
            bool: Sucesso da operação
        """
        try:
            if nome_imagem not in self.cluster.tabela_indice:
                return False

            # Deleta fragmentos em todos os nós
            for indice, fragmento in enumerate(self.cluster.tabela_indice[nome_imagem]):
                for id_no in fragmento['nodes']:
                    no = self.cluster.data_nodes[id_no]
                    no['conn'].root.deletar_fragmento_imagem(nome_imagem, indice)

            # Remove da tabela de índices
            del self.cluster.tabela_indice[nome_imagem]
            return True

        except Exception as e:
            print(f"[ERRO] Deleção de imagem: {e}")
            return False

    def iniciar(self):
        """
        Inicia o servidor RPC
        """
        try:
            print(f"[STATUS] Tentando iniciar servidor na porta {self.porta}")
            
            servidor_threads = ThreadedServer(
                service=Server, 
                port=self.porta,
                protocol_config={
                    'allow_public_attrs': True,
                    'sync_request_timeout': 300  # 5 minutos de timeout
                }
            )
            print(f"[STATUS] Servidor iniciado na porta {self.porta}")
            
            # Salva a porta em um arquivo para referência
            with open('server_port.txt', 'w') as f:
                f.write(str(self.porta))
            
            servidor_threads.start()
        
        except Exception as e:
            print(f"[ERRO] Falha ao iniciar servidor: {e}")
            # Em caso de erro, tenta uma porta diferente
            try:
                nova_porta = encontrar_porta_livre(self.porta + 1)
                self.porta = nova_porta
                self.iniciar()
            except Exception as erro_porta:
                print(f"[ERRO] Falha crítica ao encontrar porta: {erro_porta}")

if __name__ == "__main__":
    servidor = Server()
    servidor.iniciar()