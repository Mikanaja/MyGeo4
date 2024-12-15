# client_interface.py

import rpyc
import os
import time
import hashlib

class ClientInterface:
    def __init__(self, coordinator_address, coordinator_port):
        self.coordinator = rpyc.connect(coordinator_address, coordinator_port)
        print(f"Conectado ao coordenador em {coordinator_address}:{coordinator_port}")

    def upload_image(self, image_path):
        """
        Realiza o upload de uma imagem para o sistema distribuído.

        Args:
            image_path (str): Caminho local da imagem

        Returns:
            bool: Sucesso do upload
        """
        if not os.path.exists(image_path):
            print(f"Erro: Arquivo {image_path} não encontrado.")
            return False

        image_name = os.path.basename(image_path)
        image_size = os.path.getsize(image_path)

        print(f"Iniciando upload de {image_name} ({image_size} bytes)")

        # Inicia o processo de upload
        upload_info = self.coordinator.root.initiate_upload(image_name, image_size)
        if not upload_info:
            print("Erro ao iniciar o upload.")
            return False

        fragment_size = upload_info['fragment_size']
        num_fragments = upload_info['num_fragments']
        distribution = upload_info['distribution']

        # Lê e envia os fragmentos
        with open(image_path, 'rb') as file:
            for fragment_id in range(num_fragments):
                fragment_data = file.read(fragment_size)
                if not fragment_data:
                    break

                nodes = distribution.get(str(fragment_id), [])
                if not nodes:
                    print(f"Erro: Nenhum nó disponível para o fragmento {fragment_id}")
                    return False

                for node_address in nodes:
                    try:
                        node = rpyc.connect(node_address[0], node_address[1])
                        success = node.root.store_fragment(image_name, str(fragment_id), fragment_data)
                        if success:
                            print(f"Fragmento {fragment_id} armazenado em {node_address}")
                            break
                    except Exception as e:
                        print(f"Erro ao armazenar fragmento {fragment_id} em {node_address}: {e}")
                else:
                    print(f"Falha ao armazenar o fragmento {fragment_id}")
                    return False

        # Finaliza o upload
        if self.coordinator.root.complete_upload(image_name):
            print(f"Upload de {image_name} concluído com sucesso.")
            return True
        else:
            print(f"Erro ao finalizar o upload de {image_name}")
            return False

    def download_image(self, image_name, destination_path):
        """
        Realiza o download de uma imagem do sistema distribuído.

        Args:
            image_name (str): Nome da imagem a ser baixada
            destination_path (str): Caminho local para salvar a imagem

        Returns:
            bool: Sucesso do download
        """
        print(f"Iniciando download de {image_name}")

        # Obtém informações para o download
        download_info = self.coordinator.root.initiate_download(image_name)
        if not download_info:
            print(f"Erro: Imagem {image_name} não encontrada.")
            return False

        num_fragments = download_info['num_fragments']
        fragment_size = download_info['fragment_size']
        distribution = download_info['distribution']

        with open(destination_path, 'wb') as file:
            for fragment_id in range(num_fragments):
                nodes = distribution.get(str(fragment_id), [])
                if not nodes:
                    print(f"Erro: Nenhum nó disponível para o fragmento {fragment_id}")
                    return False

                for node_address in nodes:
                    try:
                        node = rpyc.connect(node_address[0], node_address[1])
                        fragment_data = node.root.retrieve_fragment(image_name, str(fragment_id))
                        if fragment_data:
                            file.write(fragment_data)
                            print(f"Fragmento {fragment_id} recuperado de {node_address}")
                            break
                    except Exception as e:
                        print(f"Erro ao recuperar fragmento {fragment_id} de {node_address}: {e}")
                else:
                    print(f"Falha ao recuperar o fragmento {fragment_id}")
                    return False

        print(f"Download de {image_name} concluído com sucesso.")
        return True

    def delete_image(self, image_name):
        """
        Deleta uma imagem do sistema distribuído.

        Args:
            image_name (str): Nome da imagem a ser deletada

        Returns:
            bool: Sucesso da deleção
        """
        print(f"Solicitando deleção de {image_name}")

        if self.coordinator.root.delete_image(image_name):
            print(f"Imagem {image_name} deletada com sucesso.")
            return True
        else:
            print(f"Erro ao deletar a imagem {image_name}")
            return False

    def list_images(self):
        """
        Lista todas as imagens disponíveis no sistema.

        Returns:
            list: Lista de nomes de imagens
        """
        images = self.coordinator.root.list_images()
        if images:
            print("Imagens disponíveis:")
            for image in images:
                print(f"- {image}")
        else:
            print("Nenhuma imagem encontrada no sistema.")
        return images

    def get_system_status(self):
        """
        Obtém e exibe o status geral do sistema.

        Returns:
            dict: Status do sistema
        """
        status = self.coordinator.root.get_system_status()
        print("Status do Sistema:")
        print(f"Nós ativos: {status['active_nodes']}")
        print(f"Total de nós: {status['total_nodes']}")
        print(f"Total de imagens: {status['total_images']}")
        return status

def main():
    coordinator_address = 'localhost'  # Altere para o endereço real do coordenador
    coordinator_port = 8000  # Altere para a porta real do coordenador

    client = ClientInterface(coordinator_address, coordinator_port)

    while True:
        print("\n--- Menu do Cliente ---")
        print("1. Upload de Imagem")
        print("2. Download de Imagem")
        print("3. Deletar Imagem")
        print("4. Listar Imagens")
        print("5. Status do Sistema")
        print("6. Sair")

        choice = input("Escolha uma opção: ")

        if choice == '1':
            image_path = input("Digite o caminho da imagem para upload: ")
            client.upload_image(image_path)
        elif choice == '2':
            image_name = input("Digite o nome da imagem para download: ")
            destination = input("Digite o caminho de destino: ")
            client.download_image(image_name, destination)
        elif choice == '3':
            image_name = input("Digite o nome da imagem para deletar: ")
            client.delete_image(image_name)
        elif choice == '4':
            client.list_images()
        elif choice == '5':
            client.get_system_status()
        elif choice == '6':
            print("Encerrando o cliente...")
            break
        else:
            print("Opção inválida. Tente novamente.")

if __name__ == "__main__":
    main()
