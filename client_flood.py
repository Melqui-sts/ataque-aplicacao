#!/usr/bin/env python3
"""
Cliente Gerador de Carga HTTP (Camada de Aplicação - L7).
Executado na Máquina Atacante (Machine B) para enviar tráfego HTTP autêntico
através da rede local para a Máquina Servidor (Machine A).
"""

import argparse
import concurrent.futures
import json
import signal
import sys
import time
import urllib.error
import urllib.request

# Métricas globais do cliente
stats = {
    "sent": 0,
    "success": 0,
    "failed": 0,
    "latencies": [],
    "start_time": time.time(),
}


def send_workload_request(target_url, mode, rounds=12000, chunk_mb=10, timeout=10):
    """Envia uma requisição HTTP POST para o endpoint /api/workload."""
    url = f"{target_url.rstrip('/')}/api/workload"
    payload = json.dumps({
        "mode": mode,
        "rounds": rounds,
        "chunk_mb": chunk_mb,
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "Academic-L7-Flood-Client/2.0",
        },
        method="POST",
    )

    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            elapsed = (time.time() - t0) * 1000.0
            return True, elapsed, resp.status
    except urllib.error.HTTPError as e:
        elapsed = (time.time() - t0) * 1000.0
        return False, elapsed, e.code
    except Exception as e:
        elapsed = (time.time() - t0) * 1000.0
        return False, elapsed, str(e)


def print_banner(target, mode, concurrency):
    print("=" * 70)
    print("  GERADOR DE CARGA HTTP - CAMADA DE APLICAÇÃO (L7 FLOOD)")
    print("=" * 70)
    print(f"[*] Alvo na Rede:   {target}")
    print(f"[*] Tipo de Carga:  {mode.upper()}")
    print(f"[*] Concorrência:   {concurrency} conexões HTTP simultâneas")
    print("=" * 70)
    print("[*] Enviando tráfego HTTP real pela rede... Pressione [Ctrl + C] para parar.")
    print("-" * 70)


def print_summary(target):
    total = stats["sent"]
    ok = stats["success"]
    fail = stats["failed"]
    duration = max(0.1, time.time() - stats["start_time"])
    rps = total / duration
    avg_lat = (sum(stats["latencies"]) / len(stats["latencies"])) if stats["latencies"] else 0.0

    print("\n\n" + "=" * 70)
    print("  RELATÓRIO DO EXPERIMENTO DE CAMADA 7 (L7)")
    print("=" * 70)
    print(f"[*] Alvo testado:                    {target}")
    print(f"[*] Duração do teste:                {duration:.2f} segundos")
    print(f"[*] Total de requisições HTTP:       {total:,}")
    print(f"[*] Respostas bem-sucedidas (200):   {ok:,}")
    print(f"[*] Requisições com falha / timeout: {fail:,}")
    print(f"[*] Vazão média alcançada:           {rps:.1f} req/segundo")
    print(f"[*] Latência média de resposta:      {avg_lat:.2f} ms")
    if fail > ok:
        print("\n[!] OBSERVAÇÃO ACADÊMICA: O servidor entrou em colapso / Denial of Service.")
        print("    A taxa de falhas superou o sucesso devido ao esgotamento de CPU/RAM.")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Cliente de Teste de Carga L7 para Redes de Computadores."
    )
    parser.add_argument(
        "target",
        nargs="?",
        default="http://localhost:8080",
        help="URL do servidor alvo (ex: http://192.168.1.50:8080)",
    )
    parser.add_argument(
        "--mode",
        choices=["both", "cpu", "ram"],
        default="both",
        help="Tipo de estresse na aplicação (padrão: both)",
    )
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=15,
        help="Número de threads simultâneas de envio (padrão: 15)",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=12000,
        help="Iterações de hash por requisição de CPU (padrão: 12000)",
    )
    parser.add_argument(
        "--chunk-mb",
        type=int,
        default=10,
        help="Megabytes de RAM alocados por requisição (padrão: 10)",
    )

    args = parser.parse_args()

    # Formata URL do alvo
    target = args.target.strip()
    if not target.startswith("http://") and not target.startswith("https://"):
        target = f"http://{target}"

    print_banner(target, args.mode, args.concurrency)

    # Teste de conectividade inicial
    try:
        urllib.request.urlopen(f"{target.rstrip('/')}/api/stats", timeout=3)
        print("[+] Conexão com o servidor alvo estabelecida com sucesso!\n")
    except Exception as e:
        print(f"[!] Aviso: Não foi possível testar /api/stats ({e}). Tentando enviar carga mesmo assim...\n")

    stop_flag = False

    def handle_sigint(sig, frame):
        nonlocal stop_flag
        stop_flag = True

    signal.signal(signal.SIGINT, handle_sigint)

    stats["start_time"] = time.time()
    last_print = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = set()

        # Enche o pool inicial de conexões
        for _ in range(args.concurrency):
            futures.add(
                executor.submit(
                    send_workload_request,
                    target,
                    args.mode,
                    args.rounds,
                    args.chunk_mb,
                )
            )

        while not stop_flag:
            done, futures = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )

            for f in done:
                success, latency_ms, status = f.result()
                stats["sent"] += 1
                if success:
                    stats["success"] += 1
                    stats["latencies"].append(latency_ms)
                else:
                    stats["failed"] += 1

                # Reabastece o pool com uma nova requisição
                if not stop_flag:
                    futures.add(
                        executor.submit(
                            send_workload_request,
                            target,
                            args.mode,
                            args.rounds,
                            args.chunk_mb,
                        )
                    )

            now = time.time()
            if now - last_print >= 0.5:
                duration = max(0.1, now - stats["start_time"])
                rps = stats["sent"] / duration
                recent_lats = stats["latencies"][-30:] if stats["latencies"] else [0]
                cur_avg_lat = sum(recent_lats) / len(recent_lats)

                sys.stdout.write(
                    f"\r[*] Enviadas: {stats['sent']:,} | "
                    f"200 OK: {stats['success']:,} | "
                    f"Erros/Timeout: {stats['failed']:,} | "
                    f"Vazão: {rps:.1f} req/s | "
                    f"Latência: {cur_avg_lat:.1f} ms   "
                )
                sys.stdout.flush()
                last_print = now

    print_summary(target)


if __name__ == "__main__":
    main()
