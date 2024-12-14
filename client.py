import rpyc
import os
import sys
import time

class ClienteImagem:
    def __init__(self, host='localhost', porta=5001):
        """
        Inicializa o cliente para comunicação com o servidor de imagens
        
        Args:
            host (str): Endereço do servidor
            porta (int): Porta do servidor
        """
        try:
            # Lê a porta do arquivo se não for fornecida
            if porta == 5001:
                try:
                    with open('server_port.txt', 'r') as f:
                        porta = int(f.read().strip())
                except FileNotFoundError:
                    print("[AVISO] Arquivo server_port.txt não encontrado. Usando porta padrão.")
            
            self.conexao = rpyc.connect(host, porta)
            self.servidor = self.conexao.root
            print(f"[STATUS] Conectado ao servidor {host}:{porta}")
        except Exception as e:
            print(f"[ERRO] Falha ao conectar ao servidor: {e}")
            sys.exit(1)

    def upload_imagem(self, caminho_imagem):
        """
        Realiza upload de uma imagem
        
        Args:
            caminho_imagem (str): Caminho completo para a imagem
        
        Returns:
            bool: Sucesso do upload
        """
        try:
            # Verifica se o arquivo existe
            if not os.path.exists(caminho_imagem):
                print(f"[ERRO] Arquivo {caminho_imagem} não encontrado")
                return False

            # Obtém informações do arquivo
            nome_imagem = os.path.basename(caminho_imagem)
            tamanho_imagem = os.path.getsize(caminho_imagem)

            # Inicia upload
            inicio_upload = self.servidor.iniciar_upload(nome_imagem, tamanho_imagem)
            
            if not inicio_upload['sucesso']:
                print(f"[ERRO] {inicio_upload['mensagem']}")
                return False

            # Configura upload
            tamanho_fragmento = inicio_upload['tamanho_fragmento']
            divisoes = inicio_upload['divisoes']

            # Abre arquivo para leitura
            with open(caminho_imagem, 'rb') as arquivo:
                for indice_fragmento in range(divisoes):
                    # Lê fragmento
                    fragmento = arquivo.read(tamanho_fragmento)
                    
                    if not fragmento:
                        break

                    # Envia fragmento
                    resultado = self.servidor.upload_fragmento(fragmento, indice_fragmento)
                    
                    if not resultado['sucesso']:
                        print(f"[ERRO] {resultado['mensagem']}")
                        return False
                    
                    # Barra de progresso
                    progresso = (indice_fragmento + 1) / divisoes * 100
                    print(f"Enviando fragmento {indice_fragmento + 1}/{divisoes} ({progresso:.2f}%)")
                    
                    # Pequena pausa para evitar sobrecarga
                    time.sleep(0.1)

            # Finaliza upload
            finalizacao = self.servidor.finalizar_upload()
            
            if finalizacao['sucesso']:
                print(f"[SUCESSO] Upload de {nome_imagem} concluído")
                return True
            else:
                print(f"[ERRO] {finalizacao['mensagem']}")
                return False

        except Exception as e:
            print(f"[ERRO] Upload falhou: {e}")
            return False

    def download_imagem(self, nome_imagem, diretorio_destino='.'):
        """
        Baixa uma imagem do cluster
        
        Args:
            nome_imagem (str): Nome da imagem a ser baixada
            diretorio_destino (str): Diretório para salvar a imagem
        
        Returns:
            bool: Sucesso do download
        """
        try:
            # Inicia download
            sucesso, mensagem, tamanho_imagem = self.servidor.download_imagem(nome_imagem)
            
            if not sucesso:
                print(f"[ERRO] {mensagem}")
                return False

            # Prepara caminho de destino
            caminho_destino = os.path.join(diretorio_destino, nome_imagem)
            
            # Abre arquivo para escrita
            with open(caminho_destino, 'wb') as arquivo_destino:
                fragmentos_recebidos = 0
                
                # Recupera fragmentos
                while True:
                    fragmento = self.servidor.recuperar_fragmento()
                    
                    # Verifica fim do download
                    if fragmento == 'erro':
                        print("[ERRO] Falha ao recuperar fragmento")
                        arquivo_destino.close()
                        os.remove(caminho_destino)
                        return False
                    
                    # Escreve fragmento
                    if fragmento:
                        arquivo_destino.write(fragmento)
                        fragmentos_recebidos += 1
                        
                        # Barra de progresso
                        progresso = (fragmentos_recebidos / (tamanho_imagem // (2 * 1024 * 1024) + 1)) * 100
                        print(f"Recebendo fragmento {fragmentos_recebidos} ({progresso:.2f}%)")
                    
                    # Se não há mais fragmentos, para o download
                    if fragmento is None:
                        break

            print(f"[SUCESSO] Download de {nome_imagem} concluído")
            return True

        except Exception as e:
            print(f"[ERRO] Download falhou: {e}")
            return False

    def listar_imagens(self):
        """
        Lista todas as imagens disponíveis no cluster
        
        Returns:
            list: Lista de nomes de imagens
        """
        try:
            imagens = self.servidor.listar_imagens()
            print("\n--- IMAGENS DISPONÍVEIS ---")
            if not imagens:
                print("Nenhuma imagem encontrada no cluster.")
            else:
                for i, imagem in enumerate(imagens, 1):
                    print(f"{i}. {imagem}")
            return imagens
        except Exception as e:
            print(f"[ERRO] Falha ao listar imagens: {e}")
            return []

    def deletar_imagem(self, nome_imagem):
        """
        Deleta uma imagem do cluster
        
        Args:
            nome_imagem (str): Nome da imagem a ser deletada
        
        Returns:
            bool: Sucesso da deleção
        """
        try:
            sucesso = self.servidor.deletar_imagem(nome_imagem)
            
            if sucesso:
                print(f"[SUCESSO] Imagem {nome_imagem} deletada")
                return True
            else:
                print(f"[ERRO] Falha ao deletar {nome_imagem}")
                return False
        except Exception as e:
            print(f"[ERRO] Falha ao deletar imagem: {e}")
            return False

    def __del__(self):
        """Fecha conexão ao destruir o objeto"""
        try:
            self.conexao.close()
        except:
            pass

def menu_interativo():
    """Menu interativo para operações com imagens"""
    cliente = ClienteImagem()
    
    while True:
        print("\n--- MENU DE OPERAÇÕES ---")
        print("1. Upload de Imagem")
        print("2. Download de Imagem")
        print("3. Listar Imagens")
        print("4. Deletar Imagem")
        print("5. Sair")
        
        try:
            escolha = input("Escolha uma opção (1-5): ")
            
            if escolha == '1':
                caminho = input("Digite o caminho completo da imagem: ")
                cliente.upload_imagem(caminho)
            
            elif escolha == '2':
                # Primeiro lista as imagens disponíveis
                imagens = cliente.listar_imagens()
                if imagens:
                    nome = input("Digite o nome da imagem para download: ")
                    cliente.download_imagem(nome)
            
            elif escolha == '3':
                cliente.listar_imagens()
            
            elif escolha == '4':
                # Primeiro lista as imagens disponíveis
                imagens = cliente.listar_imagens()
                if imagens:
                    nome = input("Digite o nome da imagem para deletar: ")
                    cliente.deletar_imagem(nome)
            
            elif escolha == '5':
                print("Saindo...")
                break
            
            else:
                print("Opção inválida! Escolha uma opção entre 1 e 5.")
        
        except KeyboardInterrupt:
            print("\nOperação cancelada pelo usuário.")
        
        except Exception as e:
            print(f"[ERRO] Ocorreu um erro: {e}")

def main():
    try:
        menu_interativo()
    except Exception as e:
        print(f"[ERRO] Erro crítico: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()