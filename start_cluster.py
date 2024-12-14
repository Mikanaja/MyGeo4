import subprocess
import sys
import os
import time
import signal

def iniciar_datanodes(portas=[8001, 8002, 8003, 8004]):
    processos_datanodes = []
    
    # Caminho base do projeto
    base_path = os.path.dirname(os.path.abspath(__file__))
    datanode_path = os.path.join(base_path, 'datanode.py')

    for porta in portas:
        try:
            # Inicia cada datanode em um processo separado
            processo = subprocess.Popen([
                sys.executable, 
                datanode_path, 
                str(porta)
            ])
            processos_datanodes.append(processo)
            print(f"DataNode na porta {porta} iniciado (PID: {processo.pid})")
            
            # Pequena pausa entre inicializações
            time.sleep(1)
        
        except Exception as e:
            print(f"Erro ao iniciar DataNode na porta {porta}: {e}")
    
    return processos_datanodes

def iniciar_servidor():
    # Caminho base do projeto
    base_path = os.path.dirname(os.path.abspath(__file__))
    server_path = os.path.join(base_path, 'server.py')

    try:
        # Inicia o servidor
        processo_servidor = subprocess.Popen([
            sys.executable, 
            server_path
        ])
        print(f"Servidor iniciado (PID: {processo_servidor.pid})")
        return processo_servidor
    
    except Exception as e:
        print(f"Erro ao iniciar servidor: {e}")
        return None

def encerrar_processos(processos):
    for processo in processos:
        try:
            # Primeiro tenta encerrar graciosamente
            processo.terminate()
            
            # Espera um momento
            time.sleep(1)
            
            # Se ainda estiver rodando, força o encerramento
            if processo.poll() is None:
                processo.kill()
        except Exception as e:
            print(f"Erro ao encerrar processo {processo.pid}: {e}")

def main():
    # Limpa processos anteriores
    limpar_processos_anteriores()

    # Inicia os datanodes
    processos_datanodes = iniciar_datanodes()
    
    # Dá um tempo para os datanodes subirem
    time.sleep(3)
    
    # Inicia o servidor
    processo_servidor = iniciar_servidor()
    
    # Processos para encerrar
    processos = processos_datanodes + ([processo_servidor] if processo_servidor else [])
    
    try:
        # Mantém o script rodando
        if processo_servidor:
            processo_servidor.wait()
    
    except KeyboardInterrupt:
        print("\nEncerrando cluster...")
    
    finally:
        # Encerra todos os processos
        encerrar_processos(processos)

def limpar_processos_anteriores():
    """
    Tenta limpar processos em portas específicas
    """
    portas = [8001, 8002, 8003, 8004, 5000]
    
    for porta in portas:
        try:
            # Encontra e mata processos usando essas portas
            subprocess.run(f"lsof -ti:{porta} | xargs kill -9", 
                           shell=True, 
                           stdout=subprocess.DEVNULL, 
                           stderr=subprocess.DEVNULL)
        except Exception as e:
            print(f"Erro ao limpar porta {porta}: {e}")

if __name__ == "__main__":
    main()