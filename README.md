# Laboratório de Carga e Resiliência (Camada de Aplicação - L7)

Experimento acadêmico de **Redes de Computadores** e **Sistemas Operacionais** demonstrando um ataque de negação de serviço na **Camada 7 (HTTP Flood / Esgotamento de Recursos)** entre duas máquinas em rede local.

---

## 🏛️ Arquitetura do Experimento Acadêmico

```text
┌─────────────────────────────────┐                 ┌─────────────────────────────────┐
│   MÁQUINA B (Atacante / Cliente) │                 │    MÁQUINA A (Alvo / Servidor)   │
│                                 │                 │                                 │
│  client_flood.py                │  HTTP POST L7   │  app.py                         │
│  - Gera conexões simultâneas    │ ──────────────> │  - Endpoint /api/workload       │
│  - Mede Latência (ms) e RPS     │  (Rede Local)   │  - Painel de Telemetria (8080)  │
│  - Detecta queda do servidor    │                 │  - 100% CPU (Hashing) e RAM     │
└─────────────────────────────────┘                 └─────────────────────────────────┘
                                                             │
                                                             ▼
                                                    [ Linux Kernel OOM Killer ]
                                                    (Derruba o processo em 100%)
```

---

## ⚠️ Riscos e Avisos

> [!CAUTION]
> **ESTE EXPERIMENTO DEVE SER EXECUTADO EXCLUSIVAMENTE EM REDE DE LABORATÓRIO ISOLADA.**
> - **Saturação de CPU a 100%:** O uso total de todos os núcleos pode deixar a interface gráfica da máquina alvo temporariamente sem resposta.
> - **Exaustão de RAM a 100%:** Alocar toda a memória física provocará *Swap Thrashing* (lentidão extrema de disco) ou o acionamento do **Linux OOM Killer**, que encerrará o processo para salvar o kernel.

---

## 📋 Pré-requisitos

- **Python 3.5+** nas duas máquinas. Em laboratório de 32 bits, use uma
  instalação de Python 3.5 compilada para a arquitetura do computador.
- Somente bibliotecas da instalação padrão do Python são necessárias. A
  biblioteca `psutil` é **opcional** na Máquina A: sem ela, o painel usa os
  arquivos `/proc` do Linux para coletar CPU, RAM e swap.

  Para usar `psutil` quando ela estiver disponível:
  ```bash
  pip install psutil
  # ou no Ubuntu/Debian:
  sudo apt install python3-psutil -y
  ```

Confirme o interpretador antes de executar o experimento:
```bash
python3 --version
# esperado: Python 3.5.x ou superior
```

## 🚀 Passo a Passo da Apresentação no Laboratório

### 1. Na Máquina A (Servidor Alvo)
*(Opcional, mas recomendado em máquinas antigas com HD mecânico)* Desative o **Swap** temporariamente para que o **OOM Killer** atue imediatamente ao atingir 100% da RAM física, evitando lentidão de disco (*Swap Thrashing*):
```bash
sudo swapoff -a
# Para reativar o Swap após a apresentação:
# sudo swapon -a
```

Inicie o servidor de aplicação:
```bash
python3 app.py
```
O terminal exibirá o IP da máquina na rede local:
```
[*] Servidor rodando em: http://192.168.1.50:8080
```

Abra o navegador na Máquina A (ou acesse da Máquina B) em `http://192.168.1.50:8080` para acompanhar os gráficos de **CPU**, **RAM** e **SWAP** em tempo real.

---

### 2. Na Máquina B (Cliente Atacante / Gerador de Carga L7)
Envie o tráfego HTTP autêntico através da rede direcionado ao IP da Máquina A:

```bash
# Carga Total (CPU + RAM) com 20 conexões simultâneas:
python3 client_flood.py http://192.168.1.50:8080 --mode both -c 20

# Somente CPU (rotinas criptográficas):
python3 client_flood.py http://192.168.1.50:8080 --mode cpu -c 20

# Somente RAM (alocação progressiva de memória):
python3 client_flood.py http://192.168.1.50:8080 --mode ram -c 20
```

---
### 3. Evidência Forense do Kernel Linux (OOM Killer)
Quando a RAM da Máquina A atinge 100%, o processo é encerrado pelo kernel. Para provar o colapso, execute na Máquina A:

```bash
dmesg -T | grep -i -E "oom|killed process"
# Caso a versão antiga do Linux não aceite a flag -T:
dmesg | grep -i -E "oom|killed process"
```
O Linux exibirá o registro do kernel comprovando o sacrifício do processo para evitar o travamento total da máquina:
> `Out of memory: Killed process (python3)...`

---
### 4. Proteger o Servidor Python contra o OOM Killer (NÃO RECOMENDADO!)
Se você quiser que o script Python nunca seja derrubado (forçando o Linux a matar outros programas ou congelar a interface gráfica):
```bash
echo -1000 | sudo tee /proc/$(pgrep -f "python3 app.py")/oom_score_adj
```
