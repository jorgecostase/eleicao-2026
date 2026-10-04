# Apuração Eleições Presidenciais 2026 • Dashboard em Tempo Real

Dashboard web leve, neutro e em tempo real para acompanhamento da apuração presidencial 2026 conectado diretamente ao CDN do TSE (Tribunal Superior Eleitoral), com projeção fixa territorial baseada no histórico calibrado de 2022.

## Características

- **Metodologia Híbrida em Tempo Real**:
  - **Exterior (`ZZ`)**: Acompanhamento por seção via Boletins de Urna (`bu.dat`) decodificados localmente com parser ASN.1 BER.
  - **Brasil (27 UFs)**: Acompanhamento agregado por zona eleitoral via arquivos unificados oficiais do TSE.
- **Visual Neutro Broadcast**: Estilo editorial profissional de estúdio de notícias (Dark Navy/Slate), sem identidades partidárias.
- **Barra de Progresso Única**: Acompanhamento claro e proeminente dos eleitores apurados em relação ao eleitorado total.
- **Tabela de Projeção & Deltas**: Comparação de votos apurados, deltas (volume absoluto de votos e ritmo percentual p.p.) e resultado projetado final.
- **Cenários Eleitorais & Probabilidade**: Cálculo dinâmico em tempo real de probabilidade de 1º e 2º turno (modelo normal com redução de incerteza $\sigma \propto \sqrt{1 - f}$).
- **Baixa Latência & Conexões Seguras**: Conexões persistentes HTTP/2 com Keep-Alive e limite estrito abaixo de 20 requisições/segundo.

## Estrutura do Repositório

```text
eleicoes-2026/
├── app.py              # Servidor HTTP nativo e orquestrador do loop assíncrono do TSE
├── engine.py           # Motor estatístico O(1), TSE Client HTTP/2 e parser ASN.1
├── index.html          # Dashboard Web responsivo e neutro (HTML5 / Tailwind CSS / Vanilla JS)
├── iniciar_painel.bat  # Script para iniciar com 1 clique no Windows
├── requirements.txt    # Dependências mínimas de execução (httpx, h2, pandas)
└── data/               # Bases de dados necessárias
    ├── dados_zonas_2026.csv   # Base consolidada das 6.292 zonas eleitorais e votos esperados
    ├── candidatos_2026.csv    # Catálogo de candidatos à Presidência
    ├── zz_config_secoes.json  # Relação de seções eleitorais do exterior
    └── boletins_urna_zz/      # Cache dos Boletins de Urna baixados
```

## Como Executar

### 1. Instalar Dependências

```bash
pip install -r requirements.txt
```

### 2. Iniciar o Servidor

No Windows:
```powershell
py app.py
```
*(ou clique duas vezes em `iniciar_painel.bat`)*

No Linux / macOS:
```bash
python3 app.py
```

### 3. Acessar o Dashboard

Abra seu navegador em:
```text
http://localhost:8000
```
