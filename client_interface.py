# client_interface.py

import rpyc
import os
import time
import hashlib
import random
import concurrent.futures

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

    def perform_scale_test(self, num_images, images_per_second, num_datanodes):
        """
        Realiza um teste de escala para download de imagens.

        Args:
            num_images (int): Número de imagens para download
            images_per_second (int): Taxa de download (imagens por segundo)
            num_datanodes (int): Número de datanodes para o teste

        Returns:
            float: Tempo total de execução em segundos
        """
        print(f"Iniciando teste de escala: {num_images} imagens, {images_per_second} img/s, {num_datanodes} datanodes")
        
        # Simula a configuração de datanodes (na prática, isso seria feito no coordenador)
        self.simulate_datanode_config(num_datanodes)

        # Lista todas as imagens disponíveis
        available_images = self.list_images()
        if not available_images:
            print("Erro: Não há imagens disponíveis para o teste.")
            return 0

        # Seleciona imagens aleatórias para o teste
        test_images = random.choices(available_images, k=num_images)

        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=images_per_second) as executor:
            futures = []
            for i, image in enumerate(test_images):
                if i > 0 and i % images_per_second == 0:
                    time.sleep(1)  # Pausa para simular a taxa de download por segundo
                futures.append(executor.submit(self.download_image, image, f"test_download_{i}.jpg"))

            # Espera todas as downloads terminarem
            concurrent.futures.wait(futures)

        end_time = time.time()
        total_time = end_time - start_time

        print(f"Teste concluído em {total_time:.2f} segundos")
        return total_time

    def simulate_datanode_config(self, num_datanodes):
        """
        Simula a configuração de um número específico de datanodes.
        Na prática, isso seria feito no coordenador.
        """
        print(f"Simulando configuração com {num_datanodes} datanodes")
        # Aqui você poderia adicionar lógica para configurar os datanodes no coordenador
        # Por exemplo, enviando uma mensagem ao coordenador para ajustar a configuração

    def run_scale_tests(self):
        """
        Executa uma série de testes de escala com diferentes configurações.
        """
        test_configs = [
            (1, 3), (5, 3), (10, 3),  # Testes com 3 datanodes
            (1, 6), (5, 6), (10, 6)   # Testes com 6 datanodes
        ]

        results = []
        for images_per_second, num_datanodes in test_configs:
            # Executa cada teste por 60 segundos
            num_images = images_per_second * 60
            time_taken = self.perform_scale_test(num_images, images_per_second, num_datanodes)
            results.append((images_per_second, num_datanodes, time_taken))

        # Exibe os resultados
        print("\nResultados dos Testes de Escala:")
        print("Imagens/s | Datanodes | Tempo Total (s)")
        print("-" * 40)
        for ips, dn, time in results:
            print(f"{ips:^10} | {dn:^9} | {time:^14.2f}")

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
        print("6. Executar Testes de Escala")
        print("7. Sair")

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
            client.run_scale_tests()
        elif choice == '7':
            print("Encerrando o cliente...")
            break
        else:
            print("Opção inválida. Tente novamente.")

if __name__ == "__main__":
    main()
