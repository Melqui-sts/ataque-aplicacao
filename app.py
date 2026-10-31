#!/usr/bin/env python3
"""
Laboratório de Teste de Carga e Resiliência na Camada de Aplicação (L7).
Refatorado com subprocessos dedicados para garantir 100% de CPU real em qualquer
quantidade de núcleos (2, 4, 8+) sem deadlock de fork, e controle preciso de RAM.
"""

import gc
import json
import os
import subprocess
import sys
import threading
import time
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn
import urllib.parse
import psutil

# Detecta configuração da máquina
CPU_COUNT = os.cpu_count() or 2

# Estado global da aplicação
system_state = {
    "simulation_running": False,
    "simulation_mode": "both",  # 'cpu', 'ram', 'both'
    "concurrency": CPU_COUNT,
    "target_ram": 90,  # percentual alvo configurável
    "requests_processed": 0,
    "allocated_ram_chunks": [],
    "cpu_subprocesses": [],
    "stop_event": threading.Event(),
}

state_lock = threading.Lock()


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Servidor HTTP multi-thread resiliente."""
    daemon_threads = True
    allow_reuse_address = True


HTML_DASHBOARD = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Laboratório de Carga e Resiliência (Camada de Aplicação - L7)</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg-primary: #0a0e17;
            --bg-card: rgba(18, 24, 38, 0.75);
            --bg-card-border: rgba(255, 255, 255, 0.08);
            --accent-cyan: #00f2fe;
            --accent-blue: #4facfe;
            --accent-red: #ff416c;
            --accent-orange: #ff4b2b;
            --accent-emerald: #10b981;
            --accent-amber: #f59e0b;
            --text-primary: #f8fafc;
            --text-muted: #94a3b8;
            --font-main: 'Plus Jakarta Sans', sans-serif;
            --font-code: 'JetBrains Mono', monospace;
        }}

        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}

        body {{
            background: radial-gradient(circle at 50% 0%, #152238 0%, #0a0e17 80%);
            color: var(--text-primary);
            font-family: var(--font-main);
            min-height: 100vh;
            padding: 2rem;
            display: flex;
            flex-direction: column;
            align-items: center;
        }}

        .container {{
            width: 100%;
            max-width: 1100px;
        }}

        header {{
            margin-bottom: 2rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--bg-card-border);
            padding-bottom: 1.5rem;
        }}

        .header-title h1 {{
            font-size: 1.75rem;
            font-weight: 800;
            letter-spacing: -0.02em;
            background: linear-gradient(135deg, var(--accent-cyan), var(--accent-blue));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}

        .status-badge {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 6px 14px;
            border-radius: 9999px;
            font-size: 0.85rem;
            font-weight: 600;
            background: rgba(16, 185, 129, 0.1);
            color: var(--accent-emerald);
            border: 1px solid rgba(16, 185, 129, 0.3);
            transition: all 0.3s ease;
        }}

        .status-badge.active {{
            background: rgba(255, 65, 108, 0.15);
            color: var(--accent-red);
            border-color: rgba(255, 65, 108, 0.4);
            animation: pulse 1.5s infinite;
        }}

        .status-dot {{
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background-color: currentColor;
        }}

        @keyframes pulse {{
            0%, 100% {{ opacity: 1; transform: scale(1); }}
            50% {{ opacity: 0.7; transform: scale(1.15); }}
        }}

        .grid {{
            display: grid;
            grid-template-columns: repeat(12, 1fr);
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}

        .card {{
            background: var(--bg-card);
            border: 1px solid var(--bg-card-border);
            border-radius: 16px;
            padding: 1.5rem;
            backdrop-filter: blur(12px);
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
        }}

        .metric-card {{
            grid-column: span 4;
            display: flex;
            flex-direction: column;
            gap: 1rem;
        }}

        @media (max-width: 900px) {{
            .metric-card {{ grid-column: span 12; }}
            .control-card {{ grid-column: span 12; }}
        }}

        .metric-header {{
            display: flex;
            justify-content: space-between;
            align-items: baseline;
        }}

        .metric-title {{
            color: var(--text-muted);
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            font-weight: 600;
        }}

        .metric-value {{
            font-family: var(--font-code);
            font-size: 2rem;
            font-weight: 700;
        }}

        .progress-bar-bg {{
            background: rgba(255, 255, 255, 0.05);
            height: 10px;
            border-radius: 999px;
            overflow: hidden;
            position: relative;
        }}

        .progress-bar-fill {{
            height: 100%;
            width: 0%;
            border-radius: 999px;
            transition: width 0.3s ease, background 0.3s ease;
        }}

        .fill-cpu {{
            background: linear-gradient(90deg, #00f2fe, #ff416c);
        }}

        .fill-ram {{
            background: linear-gradient(90deg, #4facfe, #9b51e0);
        }}

        .fill-swap {{
            background: linear-gradient(90deg, #f59e0b, #ef4444);
        }}

        .metric-sub {{
            font-size: 0.8rem;
            color: var(--text-muted);
            font-family: var(--font-code);
        }}

        .alert-box {{
            grid-column: span 12;
            background: linear-gradient(90deg, rgba(245, 158, 11, 0.12) 0%, rgba(239, 68, 68, 0.08) 100%);
            border: 1px solid rgba(245, 158, 11, 0.35);
            border-left: 4px solid #f59e0b;
            border-radius: 12px;
            padding: 1.1rem 1.4rem;
            display: flex;
            gap: 1rem;
            align-items: flex-start;
        }}

        .alert-icon {{
            font-size: 1.5rem;
            line-height: 1;
            flex-shrink: 0;
        }}

        .alert-content h3 {{
            font-size: 0.95rem;
            font-weight: 700;
            color: #fbbf24;
            margin-bottom: 0.3rem;
            display: flex;
            align-items: center;
            gap: 8px;
        }}

        .alert-content p {{
            font-size: 0.85rem;
            color: #cbd5e1;
            line-height: 1.5;
        }}

        .alert-tag {{
            background: rgba(245, 158, 11, 0.2);
            color: #fbbf24;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.75rem;
            font-family: var(--font-code);
        }}

        .control-card {{
            grid-column: span 12;
            display: flex;
            flex-direction: column;
            gap: 1.5rem;
        }}

        .form-row {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 1.5rem;
        }}

        .form-group {{
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }}

        label {{
            font-size: 0.85rem;
            font-weight: 600;
            color: var(--text-muted);
        }}

        select, input[type="range"] {{
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid var(--bg-card-border);
            color: var(--text-primary);
            padding: 10px 14px;
            border-radius: 8px;
            font-size: 0.95rem;
            font-family: var(--font-main);
            outline: none;
        }}

        select option {{
            background: #111827;
            color: #fff;
        }}

        .slider-container {{
            display: flex;
            align-items: center;
            gap: 1rem;
        }}

        .slider-val {{
            font-family: var(--font-code);
            font-weight: bold;
            min-width: 45px;
            color: var(--accent-cyan);
        }}

        .actions-row {{
            display: flex;
            gap: 1rem;
            align-items: center;
            flex-wrap: wrap;
        }}

        button {{
            border: none;
            padding: 12px 24px;
            border-radius: 10px;
            font-size: 0.95rem;
            font-weight: 700;
            cursor: pointer;
            transition: all 0.2s ease;
            display: inline-flex;
            align-items: center;
            gap: 8px;
            font-family: var(--font-main);
        }}

        .btn-start {{
            background: linear-gradient(135deg, #ff416c, #ff4b2b);
            color: #fff;
            box-shadow: 0 4px 15px rgba(255, 75, 43, 0.4);
        }}

        .btn-start:hover {{
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(255, 75, 43, 0.6);
        }}

        .btn-stop {{
            background: rgba(255, 255, 255, 0.1);
            color: var(--text-primary);
            border: 1px solid var(--bg-card-border);
        }}

        .btn-stop:hover {{
            background: rgba(255, 255, 255, 0.18);
        }}

        .btn-reset {{
            background: transparent;
            color: var(--text-muted);
            border: 1px dashed var(--bg-card-border);
        }}

        .btn-reset:hover {{
            color: var(--text-primary);
            border-color: rgba(255, 255, 255, 0.4);
        }}

        .logs-card {{
            grid-column: span 12;
            background: rgba(5, 8, 15, 0.9);
            border-radius: 12px;
            padding: 1rem 1.25rem;
            border: 1px solid var(--bg-card-border);
            font-family: var(--font-code);
            font-size: 0.82rem;
            color: #a5b4fc;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 10px;
        }}
        .critical-ram-warning {{
            display: none;
            background: rgba(239, 68, 68, 0.15);
            border: 1px solid rgba(239, 68, 68, 0.5);
            color: #fca5a5;
            padding: 10px 14px;
            border-radius: 8px;
            font-size: 0.82rem;
            margin-top: 0.6rem;
            line-height: 1.4;
            animation: pulse 1.5s infinite;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="header-title">
                <h1>Painel de Resiliência e Carga (Camada de Aplicação)</h1>
            </div>
            <div id="statusBadge" class="status-badge">
                <span class="status-dot"></span>
                <span id="statusText">Sistema Ocioso</span>
            </div>
        </header>

        <div class="grid">
            <!-- Alerta de Simulação de Alta Carga (HTTP Flood L7) -->
            <div class="alert-box">
                <div class="alert-icon">⚠️</div>
                <div class="alert-content">
                    <h3>AVISO DE ALTA CARGA <span class="alert-tag">SIMULAÇÃO HTTP FLOOD (L7)</span></h3>
                    <p>
                        Este experimento dispara carga computacional nos processadores e preenche blocos físicos de RAM. 
                        Recomendado testar inicialmente com RAM Alvo em <strong>85%</strong> para verificar o comportamento da VM sem congelar a interface gráfica do sistema operacional.
                    </p>
                </div>
            </div>

            <!-- Métrica de CPU -->
            <div class="card metric-card">
                <div class="metric-header">
                    <span class="metric-title">Uso de CPU</span>
                    <span id="cpuValue" class="metric-value">0%</span>
                </div>
                <div class="progress-bar-bg">
                    <div id="cpuBar" class="progress-bar-fill fill-cpu"></div>
                </div>
                <div class="metric-sub" id="cpuCores">{CPU_COUNT} núcleos detectados</div>
            </div>

            <!-- Métrica de RAM -->
            <div class="card metric-card">
                <div class="metric-header">
                    <span class="metric-title">Memória RAM</span>
                    <span id="ramValue" class="metric-value">0%</span>
                </div>
                <div class="progress-bar-bg">
                    <div id="ramBar" class="progress-bar-fill fill-ram"></div>
                </div>
                <div class="metric-sub" id="ramDetails">0 GB / 0 GB alocados</div>
            </div>

            <!-- Métrica de SWAP -->
            <div class="card metric-card">
                <div class="metric-header">
                    <span class="metric-title">Memória SWAP (Disco)</span>
                    <span id="swapValue" class="metric-value">0%</span>
                </div>
                <div class="progress-bar-bg">
                    <div id="swapBar" class="progress-bar-fill fill-swap"></div>
                </div>
                <div class="metric-sub" id="swapDetails">0 GB / 0 GB usados</div>
            </div>

            <!-- Painel de Controle da Simulação L7 -->
            <div class="card control-card">
                <h2 style="font-size: 1.15rem; font-weight: 700;">Configurar e Disparar Teste de Carga L7</h2>
                
                <div class="form-row">
                    <div class="form-group">
                        <label for="modeSelect">Tipo de Carga da Aplicação</label>
                        <select id="modeSelect">
                            <option value="both">Carga Total (CPU em Todos os Núcleos + RAM)</option>
                            <option value="cpu">Somente CPU (Saturação de Todos os Núcleos)</option>
                            <option value="ram">Somente RAM (Alocação Gradual até o Alvo)</option>
                        </select>
                    </div>

                    <div class="form-group">
                        <label for="concurrencySlider">Processos Concorrentes de CPU</label>
                        <div class="slider-container">
                            <input type="range" id="concurrencySlider" min="1" max="{max(CPU_COUNT * 2, 8)}" value="{CPU_COUNT}">
                            <span id="concurrencyVal" class="slider-val">{CPU_COUNT}</span>
                        </div>
                    </div>

                    <div class="form-group" style="grid-column: span 12;">
                        <label for="ramTargetSlider">Limite Alvo de RAM (%)</label>
                        <div class="slider-container">
                            <input type="range" id="ramTargetSlider" min="50" max="100" value="85">
                            <span id="ramTargetVal" class="slider-val">85%</span>
                        </div>
                        <div id="ramWarningBox" class="critical-ram-warning">
                            🚨 <strong>ALERTA DE USO EXTREMO (100%):</strong> Alocar 100% da RAM física forçará o esgotamento total de memória da VM. O kernel Linux acionará o <em>OOM Killer</em> (que pode derrubar o servidor ou a interface gráfica) ou congelará totalmente o sistema por swap thrashing.
                        </div>
                    </div>
                </div>

                <div class="actions-row">
                    <button id="btnToggle" class="btn-start" onclick="toggleSimulation()">
                        <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
                            <path d="M8 5v14l11-7z"/>
                        </svg>
                        Iniciar Teste de Carga
                    </button>
                    <button class="btn-reset" onclick="resetMemory()">
                        Limpar Memória Alocada
                    </button>
                </div>
            </div>

            <!-- Telemetria -->
            <div class="logs-card">
                <span>Processos Ativos de CPU: <strong id="activeProcCount" style="color: #fff;">0</strong></span>
                <span>Memória Retida pela Aplicação: <strong id="appCacheMb" style="color: #fff;">0 MB</strong></span>
            </div>
        </div>
    </div>

    <script>
        let isRunning = false;
        const concurrencySlider = document.getElementById('concurrencySlider');
        const concurrencyVal = document.getElementById('concurrencyVal');
        concurrencySlider.oninput = () => {{ concurrencyVal.innerText = concurrencySlider.value; }};

        const ramTargetSlider = document.getElementById('ramTargetSlider');
        const ramTargetVal = document.getElementById('ramTargetVal');
        ramTargetSlider.oninput = () => {{
            const val = parseInt(ramTargetSlider.value, 10);
            ramTargetVal.innerText = val + '%';
            const warnBox = document.getElementById('ramWarningBox');
            if (val >= 96) {{
                warnBox.style.display = 'block';
                ramTargetVal.style.color = '#ef4444';
            }} else {{
                warnBox.style.display = 'none';
                ramTargetVal.style.color = 'var(--accent-cyan)';
            }}
        }};

        async function fetchStats() {{
            try {{
                const res = await fetch('/api/stats');
                const data = await res.json();

                // Atualiza CPU
                document.getElementById('cpuValue').innerText = data.cpu_percent.toFixed(1) + '%';
                document.getElementById('cpuBar').style.width = Math.min(100, data.cpu_percent) + '%';
                document.getElementById('cpuCores').innerText = `${{data.cpu_count}} núcleos detectados`;

                // Atualiza RAM
                document.getElementById('ramValue').innerText = data.ram_percent.toFixed(1) + '%';
                document.getElementById('ramBar').style.width = Math.min(100, data.ram_percent) + '%';
                document.getElementById('ramDetails').innerText = `${{data.ram_used_gb.toFixed(2)}} GB de ${{data.ram_total_gb.toFixed(2)}} GB (${{data.ram_percent.toFixed(0)}}%)`;

                // Atualiza SWAP
                document.getElementById('swapValue').innerText = data.swap_percent.toFixed(1) + '%';
                document.getElementById('swapBar').style.width = Math.min(100, data.swap_percent) + '%';
                document.getElementById('swapDetails').innerText = `${{data.swap_used_gb.toFixed(2)}} GB de ${{data.swap_total_gb.toFixed(2)}} GB (${{data.swap_percent.toFixed(0)}}%)`;

                // Telemetria
                document.getElementById('activeProcCount').innerText = data.active_workers;
                document.getElementById('appCacheMb').innerText = data.app_cache_mb + ' MB';

                isRunning = data.simulation_running;
                updateButtonUI();
            }} catch (err) {{
                console.error("Falha ao obter status:", err);
            }}
        }}

        function updateButtonUI() {{
            const btn = document.getElementById('btnToggle');
            const badge = document.getElementById('statusBadge');
            const badgeText = document.getElementById('statusText');

            if (isRunning) {{
                btn.className = 'btn-stop';
                btn.innerHTML = `
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
                        <rect x="6" y="6" width="12" height="12"/>
                    </svg>
                    Interromper Teste
                `;
                badge.className = 'status-badge active';
                badgeText.innerText = 'Carga Ativa na Aplicação';
            }} else {{
                btn.className = 'btn-start';
                btn.innerHTML = `
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M8 5v14l11-7z"/>
                    </svg>
                    Iniciar Teste de Carga
                `;
                badge.className = 'status-badge';
                badgeText.innerText = 'Sistema Ocioso';
            }}
        }}

        async function toggleSimulation() {{
            if (!isRunning) {{
                const targetRam = parseInt(ramTargetSlider.value, 10);
                if (targetRam >= 96) {{
                    const msg = "⚠️ AVISO DE USO EXTREMO (" + targetRam + "% RAM):\\n\\n" +
                                "Voce configurou " + targetRam + "% de uso de RAM. Isso provocara o esgotamento total da memoria fisica da VM, podendo congelar a interface ou acionar o Linux OOM Killer.\\n\\n" +
                                "Deseja realmente continuar com essa carga?";
                    if (!confirm(msg)) return;
                }}
            }}

            const endpoint = isRunning ? '/api/simulation/stop' : '/api/simulation/start';
            const payload = {{
                mode: document.getElementById('modeSelect').value,
                concurrency: parseInt(concurrencySlider.value, 10),
                target_ram: parseInt(ramTargetSlider.value, 10)
            }};

            await fetch(endpoint, {{
                method: 'POST',
                headers: {{ 'Content-Type': 'application/json' }},
                body: JSON.stringify(payload)
            }});

            fetchStats();
        }}

        async function resetMemory() {{
            await fetch('/api/simulation/reset', {{ method: 'POST' }});
            fetchStats();
        }}

        setInterval(fetchStats, 1000);
        fetchStats();
    </script>
</body>
</html>
"""


# Código executado pelos processos isolados de CPU (garante 100% de 1 núcleo cada sem GIL)
WORKER_CODE = """
import sys
while True:
    _ = 123456789 * 987654321
"""


def ram_stress_loop(stop_event, target_percent=90):
    """
    Aloca e suja páginas de memória até atingir a porcentagem alvo configurada.
    Se target_percent for 100, aloca até o limite físico da memória (ou MemoryError do SO).
    """
    chunk_size = 30 * 1024 * 1024  # 30 MB
    while not stop_event.is_set():
        mem = psutil.virtual_memory()
        if target_percent < 100 and mem.percent >= target_percent:
            time.sleep(0.3)
            continue
        try:
            # Cria e preenche fisicamente com 0xFF
            chunk = bytearray(b"\xff" * chunk_size)
            with state_lock:
                system_state["allocated_ram_chunks"].append(chunk)
                system_state["requests_processed"] += 1
            time.sleep(0.02)
        except MemoryError:
            time.sleep(0.5)


class AppRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path in ("/", "/index.html"):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_DASHBOARD.encode("utf-8"))
            return

        elif parsed.path == "/api/stats":
            cpu_pct = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory()
            swap = psutil.swap_memory()

            with state_lock:
                cache_size = sum(len(c) for c in system_state["allocated_ram_chunks"])
                alive_workers = sum(
                    1 for p in system_state["cpu_subprocesses"] if p.poll() is None
                )
                payload = {
                    "cpu_percent": cpu_pct,
                    "cpu_count": CPU_COUNT,
                    "ram_percent": mem.percent,
                    "ram_used_gb": (mem.total - mem.available) / (1024**3),
                    "ram_total_gb": mem.total / (1024**3),
                    "swap_percent": swap.percent,
                    "swap_used_gb": swap.used / (1024**3),
                    "swap_total_gb": swap.total / (1024**3),
                    "simulation_running": system_state["simulation_running"],
                    "active_workers": alive_workers,
                    "app_cache_mb": cache_size // (1024 * 1024),
                }

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode("utf-8"))
            return

        self.send_error(404, "Endpoint não encontrado")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length) if content_length > 0 else b"{}"

        try:
            data = json.loads(body.decode("utf-8")) if body else {}
        except json.JSONDecodeError:
            data = {}

        if parsed.path == "/api/simulation/start":
            mode = data.get("mode", "both")
            concurrency = int(data.get("concurrency", CPU_COUNT))
            target_ram = int(data.get("target_ram", 88))

            with state_lock:
                if not system_state["simulation_running"]:
                    system_state["simulation_running"] = True
                    system_state["simulation_mode"] = mode
                    system_state["concurrency"] = concurrency
                    system_state["target_ram"] = target_ram
                    system_state["stop_event"].clear()

                    # 1. Inicia processos de CPU via subprocess.Popen (imune a deadlocks de fork)
                    if mode in ("cpu", "both"):
                        for _ in range(concurrency):
                            proc = subprocess.Popen(
                                [sys.executable, "-c", WORKER_CODE],
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                            )
                            system_state["cpu_subprocesses"].append(proc)

                    # 2. Inicia thread de preenchimento controlado de RAM
                    if mode in ("ram", "both"):
                        t = threading.Thread(
                            target=ram_stress_loop,
                            args=(system_state["stop_event"], target_ram),
                            daemon=True,
                        )
                        t.start()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "started"}).encode("utf-8"))
            return

        elif parsed.path == "/api/simulation/stop":
            with state_lock:
                system_state["simulation_running"] = False
                system_state["stop_event"].set()

                # Mata todos os processos de CPU imediatamente
                for p in system_state["cpu_subprocesses"]:
                    try:
                        p.kill()
                    except Exception:
                        pass
                system_state["cpu_subprocesses"].clear()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "stopped"}).encode("utf-8"))
            return

        elif parsed.path == "/api/simulation/reset":
            with state_lock:
                system_state["allocated_ram_chunks"].clear()
                gc.collect()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "reset"}).encode("utf-8"))
            return

        self.send_error(404, "Endpoint não encontrado")


def run_server(port=8080):
    psutil.cpu_percent(interval=None)

    server_address = ("0.0.0.0", port)
    httpd = ThreadedHTTPServer(server_address, AppRequestHandler)
    print("=" * 65)
    print("  LABORATÓRIO DE CARGA E RESILIÊNCIA - CAMADA DE APLICAÇÃO")
    print("=" * 65)
    print(f"[*] Núcleos de CPU detectados: {CPU_COUNT}")
    print(f"[*] Servidor rodando em: http://localhost:{port}")
    print(f"[*] Acesse pelo navegador na VM ou na máquina host.")
    print("    Pressione [Ctrl + C] no terminal para encerrar.")
    print("=" * 65)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[!] Encerrando servidor e subprocessos...")
        with state_lock:
            for p in system_state["cpu_subprocesses"]:
                try:
                    p.kill()
                except Exception:
                    pass
        httpd.shutdown()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    run_server(port)
