import threading
import time
import logging
from typing import Callable, Any, Dict, List

class PubSub:
    def __init__(self, cluster, intervalo_verificacao=30):
        """
        Inicializa o sistema Pub/Sub para monitoramento de cluster

        Args:
            cluster: Objeto representando o cluster de datanodes
            intervalo_verificacao (int): Intervalo entre verificações de saúde (em segundos)
        """
        self.cluster = cluster
        self.assinantes: Dict[str, List[Callable]] = {}
        self.intervalo_verificacao = intervalo_verificacao
        
        # Configuração de logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        # Thread para monitoramento de saúde
        self.thread_monitoramento = threading.Thread(
            target=self._monitoramento_continuo, 
            daemon=True
        )
        self.thread_monitoramento.start()

    def publicar_evento(self, topico: str, dados: Any):
        """
        Publica um evento para todos os assinantes do tópico

        Args:
            topico (str): Tópico do evento
            dados (Any): Dados do evento
        """
        try:
            self.logger.info(f"Publicando evento no tópico '{topico}': {dados}")
            
            for topico_registrado, assinantes in self.assinantes.items():
                if topico_registrado in topico:
                    for assinante in assinantes:
                        try:
                            assinante(dados)
                        except Exception as e:
                            self.logger.error(f"Erro ao notificar assinante: {e}")
        except Exception as e:
            self.logger.error(f"Erro na publicação de evento: {e}")

    def assinar(self, topico: str, callback: Callable):
        """
        Adiciona um assinante para um tópico específico

        Args:
            topico (str): Tópico para assinatura
            callback (Callable): Função a ser chamada quando um evento ocorrer
        """
        try:
            if topico not in self.assinantes:
                self.assinantes[topico] = []
            
            self.assinantes[topico].append(callback)
            self.logger.info(f"Novo assinante adicionado ao tópico '{topico}'")
        except Exception as e:
            self.logger.error(f"Erro ao adicionar assinante: {e}")

    def _verificar_saude_datanode(self, datanode):
        """
        Verifica a saúde de um datanode específico

        Args:
            datanode: Datanode a ser verificado

        Returns:
            bool: Status de saúde do datanode
        """
        try:
            status = datanode['conn'].root.obter_status_no()
            
            # Critérios de saúde customizáveis
            condicoes_saude = [
                status['cpu'] < 80,        # Uso de CPU
                status['memoria'] < 90,    # Uso de memória
                status['disco'] < 95       # Uso de disco
            ]
            
            return all(condicoes_saude)
        except Exception as e:
            self.logger.error(f"Erro ao verificar saúde do datanode: {e}")
            return False

    def detectar_falha_datanode(self):
        """
        Detecta falhas em datanodes do cluster
        """
        datanodes_com_falha = []
        
        for id_datanode, datanode in self.cluster.data_nodes.items():
            if not self._verificar_saude_datanode(datanode):
                self.logger.warning(f"Datanode {id_datanode} detectado com falha")
                datanodes_com_falha.append({
                    'id': id_datanode,
                    'endereco': datanode['addr']
                })
                
                # Publica evento de falha
                self.publicar_evento(
                    'falha_datanode', 
                    {
                        'datanode_id': id_datanode, 
                        'endereco': datanode['addr']
                    }
                )
        
        return datanodes_com_falha

    def _monitoramento_continuo(self):
        """
        Thread para monitoramento contínuo de saúde dos datanodes
        """
        while True:
            try:
                falhas = self.detectar_falha_datanode()
                
                if falhas:
                    self.logger.critical(f"Falhas detectadas: {falhas}")
                
                # Aguarda antes da próxima verificação
                time.sleep(self.intervalo_verificacao)
            
            except Exception as e:
                self.logger.error(f"Erro no monitoramento contínuo: {e}")
                time.sleep(self.intervalo_verificacao)

    def reconfigurar_cluster(self, datanodes_falhos):
        """
        Reconfigura o cluster removendo datanodes com falha

        Args:
            datanodes_falhos (list): Lista de datanodes com falha
        """
        try:
            for datanode in datanodes_falhos:
                # Remove datanode do cluster
                del self.cluster.data_nodes[datanode['id']]
                self.logger.info(f"Datanode {datanode['id']} removido do cluster")

            # Reconecta o cluster
            self.cluster.connect_cluster()
            
            # Publica evento de reconfiguração
            self.publicar_evento('reconfigurar_cluster', {
                'datanodes_removidos': datanodes_falhos
            })
        
        except Exception as e:
            self.logger.error(f"Erro na reconfiguração do cluster: {e}")


# Exemplo de uso
def exemplo_callback(dados):
    """Callback de exemplo para demonstrar assinatura"""
    print(f"Evento recebido: {dados}")


def exemplo_uso(cluster):
    # Cria instância do PubSub
    pubsub = PubSub(cluster)

    # Assina tópicos
    pubsub.assinar('falha_datanode', exemplo_callback)
    pubsub.assinar('reconfigurar_cluster', exemplo_callback)

    # Publica um evento de teste
    pubsub.publicar_evento('teste', {'mensagem': 'Evento de teste'})

    # Simula detecção de falha
    pubsub.detectar_falha_datanode()

    # Mantém o programa rodando para demonstração
    import time
    time.sleep(60)