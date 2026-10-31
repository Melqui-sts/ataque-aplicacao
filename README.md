# Laboratório de Carga e Resiliência (Camada de Aplicação - L7)

Aplicação web para simulação controlada de estresse de recursos computacionais (100% CPU e RAM) na camada de aplicação (HTTP Flood / L7 DoS). Possui painel visual com métricas em tempo real de CPU, RAM e memória SWAP.

---

## ⚠️ Riscos e Avisos Importantes

> [!CAUTION]
> **ESTE SCRIPT FOI PROJETADO EXCLUSIVAMENTE PARA AMBIENTES DE TESTE / MÁQUINAS VIRTUAIS (VMs).**
> - **Saturação de CPU a 100%:** O uso total de todos os núcleos pode deixar a interface gráfica (desktop) da VM temporariamente sem resposta.
> - **Exaustão de RAM a 100%:** Alocar toda a memória física pode provocar *Swap Thrashing* (congelamento por lentidão de disco) ou acionar o **Linux OOM Killer** (*Out of Memory Killer*), que derrubará processos para proteger o kernel.
> - **Recomendação de teste:** Comece configurando o limite de RAM em **85%** antes de tentar o modo extremo de **100%**.

---

## 📋 Pré-requisitos

- **Python 3.8+**
- Biblioteca `psutil` instalada:
  ```bash
  pip install psutil
  # ou em distribuições baseadas em Debian/Ubuntu:
  sudo apt install python3-psutil -y
  ```

---

## 🚀 Como Executar e Testar

### 1. Iniciar o Servidor
No terminal da sua Máquina Virtual (ou máquina de teste):
```bash
python3 app.py
```

### 2. Acessar o Painel Visual
Abra o navegador e acesse:
- Na própria máquina: `http://localhost:8080`

### 3. Configurar e Disparar o Teste
1. Escolha o **Tipo de Carga** (Carga Total, Somente CPU ou Somente RAM).
2. Ajuste o número de **Processos de CPU** (o app já detecta o número de núcleos disponíveis).
3. Ajuste o **Limite Alvo de RAM (%)**:
   - *85% (Seguro):* Permite observar o estresse com a interface da VM ainda responsiva.
   - *100% (Extremo):* Esgota fisicamente toda a memória RAM (exibirá alerta de confirmação).
4. Clique em **"Iniciar Teste de Carga"**.
5. Observe os gráficos em tempo real de CPU, RAM e SWAP.
6. Clique em **"Interromper Teste"** ou **"Limpar Memória Alocada"** para liberar os recursos.

---

## 💡 Dica de Laboratório (Teste sem SWAP)

Por padrão, quando a memória RAM atinge cerca de 80%, o Linux transfere dados para o disco (SWAP). Caso queira forçar a exaustão **estritamente na memória física RAM**, desative o SWAP da VM antes do teste:

```bash
# Desativar SWAP temporariamente
sudo swapoff -a

# Reativar após o teste
sudo swapon -a
```
