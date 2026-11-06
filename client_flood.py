#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cliente Gerador de Carga HTTP (Camada de Aplicação - L7).
Executado na Máquina Atacante (Machine B) para enviar tráfego HTTP autêntico
através da rede local para a Máquina Servidor (Machine A).
Compatível com Python 3.4 / 3.5+ (sem dependência de f-strings ou bibliotecas externas).
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


# Opener que ignora qualquer Proxy HTTP configurado no Linux do laboratorio
DIRECT_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def send_workload_request(target_url, mode, rounds=250000, chunk_mb=40, timeout=10):
    """Envia uma requisição HTTP POST para o endpoint /api/workload."""
    url = "{0}/api/workload".format(target_url.rstrip("/"))
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
        with DIRECT_OPENER.open(req, timeout=timeout) as resp:
            elapsed = (time.time() - t0) * 1000.0
            status_code = getattr(resp, "status", resp.getcode())
            return True, elapsed, status_code
    except urllib.error.HTTPError as e:
        elapsed = (time.time() - t0) * 1000.0
        return False, elapsed, e.code
    except Exception as e:
        elapsed = (time.time() - t0) * 1000.0
        return False, elapsed, str(e)


def print_banner(target, mode, concurrency):
    print("=" * 70)
    print("  GERADOR DE CARGA HTTP - CAMADA DE APLICACAO (L7 FLOOD)")
    print("=" * 70)
    print("[*] Alvo na Rede:   {0}".format(target))
    print("[*] Tipo de Carga:  {0}".format(mode.upper()))
    print("[*] Concorrencia:   {0} conexoes HTTP simultaneas".format(concurrency))
    print("=" * 70)
    print("[*] Enviando trafego HTTP real pela rede... Pressione [Ctrl + C] para parar.")
    print("-" * 70)


def print_summary(target):
    total = stats["sent"]
    ok = stats["success"]
    fail = stats["failed"]
    duration = max(0.1, time.time() - stats["start_time"])
    rps = total / duration
    avg_lat = (sum(stats["latencies"]) / len(stats["latencies"])) if stats["latencies"] else 0.0

    print("\n\n" + "=" * 70)
    print("  RELATORIO DO EXPERIMENTO DE CAMADA 7 (L7)")
    print("=" * 70)
    print("[*] Alvo testado:                    {0}".format(target))
    print("[*] Duracao do teste:                {0:.2f} segundos".format(duration))
    print("[*] Total de requisicoes HTTP:       {0:,}".format(total))
    print("[*] Respostas bem-sucedidas (200):   {0:,}".format(ok))
    print("[*] Requisicoes com falha / timeout: {0:,}".format(fail))
    print("[*] Vazao media alcancada:           {0:.1f} req/segundo".format(rps))
    print("[*] Latencia media de resposta:      {0:.2f} ms".format(avg_lat))
    if fail > ok:
        print("\n[!] OBSERVACAO ACADEMICA: O servidor entrou em colapso / Denial of Service.")
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
        help="Tipo de estresse na aplicacao (padrao: both)",
    )
    parser.add_argument(
        "-c",
        "--concurrency",
        type=int,
        default=15,
        help="Numero de threads simultaneas de envio (padrao: 15)",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=250000,
        help="Iteracoes de hash por requisicao de CPU (padrao: 250000)",
    )
    parser.add_argument(
        "--chunk-mb",
        type=int,
        default=40,
        help="Megabytes de RAM alocados por requisicao (padrao: 40)",
    )

    args = parser.parse_args()

    # Formata URL do alvo
    target = args.target.strip()
    if not target.startswith("http://") and not target.startswith("https://"):
        target = "http://{0}".format(target)

    print_banner(target, args.mode, args.concurrency)

    # Teste de conectividade inicial (sem proxy)
    stats_url = "{0}/api/stats".format(target.rstrip("/"))
    try:
        DIRECT_OPENER.open(stats_url, timeout=3)
        print("[+] Conexao com o servidor alvo estabelecida com sucesso!\n")
    except Exception as e:
        print("[!] Aviso: Nao foi possivel testar /api/stats ({0}). Tentando enviar carga mesmo assim...\n".format(e))

    stop_state = {"stop": False}

    def handle_sigint(sig, frame):
        stop_state["stop"] = True

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

        while not stop_state["stop"]:
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
                if not stop_state["stop"]:
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

                line = (
                    "\r[*] Enviadas: {0:,} | "
                    "200 OK: {1:,} | "
                    "Erros/Timeout: {2:,} | "
                    "Vazao: {3:.1f} req/s | "
                    "Latencia: {4:.1f} ms   "
                ).format(
                    stats["sent"],
                    stats["success"],
                    stats["failed"],
                    rps,
                    cur_avg_lat,
                )
                sys.stdout.write(line)
                sys.stdout.flush()
                last_print = now

    print_summary(target)


if __name__ == "__main__":
    main()
