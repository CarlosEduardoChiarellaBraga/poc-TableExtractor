# Desafio Técnico — Extração Estruturada de Itens de Licitações (ConLicitação)

Este repositório contém uma solução para o desafio técnico "Extração Estruturada de Itens de Licitações Públicas", cujo objetivo é, a partir de um conjunto de arquivos **JSON** (metadados) e suas respectivas pastas de **anexos**, **extrair automaticamente** os itens licitados no formato estruturado exigido (`ResultadoLicitacao` → `ItemExtraido`).

A solução foi desenhada para lidar com as principais dificuldades descritas no enunciado:
- itens em **documentos anexos** com layout variável (tabelas quebradas, linhas "continuadas", títulos de lote/grupo etc.);
- itens semi-estruturados no campo `data.itens` do próprio JSON;
- anexos ausentes, corrompidos e variações de nome/pasta;
- execução **determinística e reprodutível**.

**Observação Importante:** item foi implementado como sendo string para capturar os edge cases em que o item é subdividido no formato X.XX.

---

## O que a solução entrega

Para cada arquivo de licitação `downloads/<licitacao>.json`, a solução gera:

1) **Um JSON final por licitação** (mesmo nome do JSON de entrada), seguindo o schema esperado:
- `arquivo_json`
- `numero_pregao`
- `orgao`
- `cidade`
- `estado`
- `anexos_processados`
- `itens_extraidos[]` com `{ lote, item, objeto, quantidade, unidade_fornecimento }`

2) **Um JSON consolidado** com um objeto por licitação:
- `outputs/resultado.json`

Além disso, salva artefatos intermediários (úteis para auditoria e depuração):
- tabelas extraídas de anexos: `outputs/tabelas/<licitacao>/*.json`
- parsing por anexo: `outputs/pdf_parsed/<licitacao>/*_resultado.json`

---

## Estrutura esperada do dataset

Conforme o enunciado, o dataset fica em `downloads/` e cada licitação tem:
- um arquivo JSON com metadados
- uma pasta de anexos com o mesmo nome do JSON (sem `.json`)

Exemplo:
```
downloads/
├── 2024-08-15-09-33-44-conlicitacao-<hash>.json
├── 2024-08-15-09-33-44-conlicitacao-<hash>/
│   ├── edital.pdf
│   ├── anexo_1.pdf
│   ├── relacaoitens123.pdf
│   ├── termo_de_referencia.docx
│   └── ...
```

> Observação: na prática, alguns datasets vêm com variações (subpastas, anexos com nomes genéricos como `anexo_1.pdf`, etc.). O orquestrador foi implementado para ser tolerante a isso.

---

## Arquitetura da solução

A solução é composta por **três etapas principais**, com responsabilidades separadas:

### 1) Extractor (PDF/DOCX → `*_tables.json`)
Arquivo: `extractor.py`

- Varre anexos **PDF** e **DOCX**
- Para PDF, usa `pdfplumber` e `page.extract_tables()` para capturar tabelas por página
- Para DOCX, usa `python-docx` para leitura nativa de tabelas e texto, sem necessidade de Microsoft Word
- Salva um `*_tables.json` por anexo e também um consolidado por licitação

Saída (por anexo):
```json
[
  {
    "arquivo": "downloads/<licitacao>/anexo_1.pdf",
    "pagina": 2,
    "indice_tabela": 0,
    "dados": [[...], [...]]
  }
]
```

### 2) Parser (tabelas → `ItemExtraido`)
Arquivo: `parsers/pdf_parser.py`

- Lê os `*_tables.json`
- Detecta tabelas de itens por heurística de cabeçalho (ex.: "ITEM", "QUANTIDADE/QTD", "UNIDADE/UND", "DESCRIÇÃO/ESPECIFICAÇÃO")
- Extrai:
  - `item` (string; suporta flat `"1"` e hierárquico `"1.1"`, `"2.14"`)
  - `quantidade` (inteiro; normaliza `1.000,00` → `1000`)
  - `unidade_fornecimento` (string)
  - `objeto` (descrição; junta linhas quebradas/continuações)
  - `lote` quando identificado (ex.: "LOTE 01", "LOTE ÚNICO")
- Gera um `*_resultado.json` por anexo e retorna a lista de itens extraídos

### 3) Aggregator (JSON + Parser → `ResultadoLicitacao` final)
Arquivos: `aggregator.py` + `merger.py`

- Lê o JSON ConLicitação e extrai itens semi-estruturados do campo `data.itens`
  - Parser: `parsers/json_itens.py`
- Extrai e parseia anexos (etapas 1 e 2)
- **Unifica** as duas fontes (`data.itens` e anexos) em uma saída final:
  - deduplicação por chave `(lote, item)`
  - preenchimento de campos faltantes (ex.: o JSON tem item/descrição mas não tem unidade; o PDF tem unidade)
  - preserva/propaga metadados: `numero_pregao`, `orgao`, `cidade`, `estado`
- Em modo `--debug`, adiciona `fonte` em cada item para auditoria ("veio do JSON" vs "veio do anexo X").

---

## Estratégia de descoberta de anexos (robusta)

Como nem sempre os anexos estão perfeitamente referenciados pelo campo `data.anexos`, o orquestrador busca anexos em camadas:

1. **Lista oficial**: `data.anexos[]` (quando presente e consistente)
2. **Pasta padrão**: `downloads/<licitacao_stem>/` (qualquer `.pdf`/`.docx`)
3. **Fallback global**: índice de todos os `.pdf/.docx` sob `downloads/` (para casos com subpastas fora do padrão)

Isso evita o caso "JSON foi processado mas nenhum PDF foi extraído".

---

## Instalação

Requisitos:
- Python **3.10+**

```bash
pip install pdfplumber python-docx
```

---

## Execução

Execute na raiz do projeto (onde existe `downloads/`):

```bash
python src/main.py downloads outputs
```

Modo debug (inclui `"fonte"` em cada item):
```bash
python src/main.py downloads outputs --debug
```

O pipeline roda em duas fases automaticamente:
1. Extração e parsing → `outputs/pre_resultado_final.json`
2. Sanitização → `outputs/resultado.json`

---

## Saídas geradas

Após rodar, você terá:

```
outputs/
├── pre_resultado_final.json           # resultado bruto (pré-sanitização)
├── resultado.json                     # consolidado final sanitizado
├── resultado_parcial/
│   ├── <licitacao_1>.json             # resultado final por licitação
│   └── <licitacao_2>.json
├── tabelas/<licitacao>/..._tables.json
└── pdf_parsed/<licitacao>/..._resultado.json
```

---

## Decisões de design

- **Separação clara de responsabilidades**: extractor (captura), parser (interpreta), aggregator (unifica), sanitize (filtra)
- **Heurísticas simples e extensíveis**: o parser de itens é baseado em cabeçalhos e regras de normalização
- **Determinismo**:
  - ordenação consistente de arquivos e itens
  - merge com regras fixas e previsíveis
- **Robustez**:
  - exceções em anexos não interrompem o pipeline
  - anexos ausentes/corrompidos são ignorados e o resultado ainda é produzido com o que houver disponível
- **Auditabilidade**:
  - artefatos intermediários são salvos (tabelas e parsing por anexo)
  - `--debug` adiciona `fonte` por item (sempre importante saber de onde o script extrai as informações)

---

## Limitações conhecidas

- PDFs **escaneados** (imagem) não são processados por OCR nesta versão.
- Anexos do tipo **XLS/XLSX** não são processados.
- Bugs e falhas de parsing. Ex:
```json
      {
        "lote": null,
        "item": "1.95",
        "objeto": "TOTAIS:",
        "quantidade": 1095,
        "unidade_fornecimento": "TOTAIS:"
      }
```
```json

      {
        "lote": null,
        "item": "3",
        "objeto": "OUTSOURCING DE IMPRESSAO - PAGINAS A4 - MONOCROMATICO - DENTRO DA FRANQUIA SEM PAPEL",
        "quantidade": 26573,
        "unidade_fornecimento": "PREGÃO"
      },

```

---

## Como validar rapidamente o resultado

- Confirme que `outputs/resultado.json` existe e é um array com 1 entrada por licitação.
- Para uma licitação específica, confira:
  - `anexos_processados` inclui os anexos realmente lidos
  - `itens_extraidos` tem itens com `item` string, `quantidade` inteira, `unidade_fornecimento` string e `objeto` não vazio
- Rode com `--debug` para auditar a origem de cada item (`fonte`).

---

## Estrutura do código

```
main.py                 # orquestra o pipeline fim-a-fim (inclui sanitização automática)
extractor.py            # PDF/DOCX -> tables json
aggregator.py           # unifica itens do JSON + anexos
merger.py               # regras de merge/dedup/preenchimento
models.py               # dataclasses ItemExtraido/ResultadoLicitacao (+ debug)
sanitize.py             # filtro pós-processamento de itens incompletos
parsers/
  json_itens.py         # parser do campo data.itens (texto semi-estruturado)
  pdf_parser.py         # parser de tables json -> itens estruturados
```
