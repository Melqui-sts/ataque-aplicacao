#!/usr/bin/env python3
"""
Experimento de Estresse de CPU e RAM (Camada de Aplicação).
Projetado para execução em ambiente controlado / Máquina Virtual.
"""

import multiprocessing
import os
import signal
import sys
import time


def cpu_stress_worker(stop_event):
    """Gera carga constante em 1 núcleo de processamento."""
    # Loop de computação contínua
    while not stop_event.is_set():
        _ = 123456789 * 987654321


def get_available_memory_bytes():
    """Tenta obter a memória livre em bytes (funciona em Linux/macOS/Windows)."""
    # Em Linux via /proc/meminfo para precisão máxima sem depender de libs externas
    if os.path.exists("/proc/meminfo"):
        mem_available = None
        mem_total = None
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    mem_available = int(line.split()[1]) * 1024
                elif line.startswith("MemTotal:"):
                    mem_total = int(line.split()[1]) * 1024
        if mem_available and mem_total:
            return mem_total, mem_available

    # Fallback genérico se psutil estiver instalado
    try:
        import psutil

        mem = psutil.virtual_memory()
        return mem.total, mem.available
    except ImportError:
        pass

    # Estimativa conservadora padrão (ex: 2GB) se não conseguir ler
    default_total = 2 * 1024 * 1024 * 1024
    return default_total, int(default_total * 0.8)


def consume_ram(target_percent=95):
    """
    Aloca e escreve dados em blocos para garantir que o SO
    efetivamente mapeie a memória física (RAM).
    """
    total_mem, avail_mem = get_available_memory_bytes()
    target_bytes = int(total_mem * (target_percent / 100.0))

    print(f"[*] Memória Total detectada: {total_mem / (1024**3):.2f} GB")
    print(f"[*] Alvo de consumo: {target_percent}% ({target_bytes / (1024**3):.2f} GB)")

    allocated_blocks = []
    chunk_size = 50 * 1024 * 1024  # Blocos de 50 MB
    bytes_allocated = 0

    try:
        while bytes_allocated < target_bytes:
            to_allocate = min(chunk_size, target_bytes - bytes_allocated)
            # Cria e preenche o bloco para forçar a alocação física no kernel
            block = bytearray(b"X" * to_allocate)
            allocated_blocks.append(block)
            bytes_allocated += to_allocate

            sys.stdout.write(
                f"\r[*] RAM alocada: {bytes_allocated / (1024**3):.2f} GB / {target_bytes / (1024**3):.2f} GB ({(bytes_allocated / total_mem) * 100:.1f}%)"
            )
            sys.stdout.flush()
            time.sleep(0.01)

        print("\n[+] Alocação de RAM concluída com sucesso.")
    except MemoryError:
        print("\n[!] Limite de memória atingido (MemoryError). Mantendo o que foi alocado.")

    return allocated_blocks


def main():
    target_ram_percent = 95
    if len(sys.argv) > 1:
        try:
            target_ram_percent = float(sys.argv[1])
        except ValueError:
            print(f"Uso: {sys.argv[0]} [porcentagem_ram_alvo (ex: 95)]")
            sys.exit(1)

    cpu_count = multiprocessing.cpu_count()
    print("=" * 60)
    print("  EXPERIMENTO DE CONSUMO DE RECURSOS (100% CPU / RAM)")
    print("=" * 60)
    print(f"[*] Núcleos de CPU detectados: {cpu_count}")

    # Evento para encerramento gracioso
    stop_event = multiprocessing.Event()

    def handle_exit(sig, frame):
        print("\n\n[!] Sinal de interrupção recebido. Liberando recursos...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    # 1. Iniciar estresse de CPU em todos os núcleos
    print(f"[*] Iniciando {cpu_count} processos de CPU...")
    cpu_processes = []
    for _ in range(cpu_count):
        p = multiprocessing.Process(target=cpu_stress_worker, args=(stop_event,))
        p.daemon = True
        p.start()
        cpu_processes.append(p)

    # 2. Iniciar alocação de RAM
    memory_holder = consume_ram(target_percent=target_ram_percent)

    print("\n" + "=" * 60)
    print("[+] Sistema sob carga máxima!")
    print("    - CPU: ~100% em todos os núcleos")
    print(f"    - RAM: ~{target_ram_percent}% alocada fisicamente")
    print("    Pressione [Ctrl + C] para interromper e liberar a memória.")
    print("=" * 60)

    # Mantém o processo principal vivo segurando a RAM
    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        for p in cpu_processes:
            p.join(timeout=1)
        del memory_holder
        print("[+] Recursos liberados. Experimento finalizado.")


if __name__ == "__main__":
    main()
