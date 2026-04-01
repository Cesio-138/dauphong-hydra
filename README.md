# Dauphong Hydra Source Crawler

Este projeto contém um crawler que gera e atualiza `sources/dauphong.json` a partir dos torrents publicados pelo usuário `dauphong` no The Pirate Bay, no formato compatível com o Hydra Launcher.

## O que o crawler faz

- Consulta a API pública `apibay.org` com paginação completa (`user:dauphong`, `user:dauphong:1`, `user:dauphong:2`, ...).
- Obtém o número total de páginas via `pcnt:dauphong` antes de iniciar.
- Grava o JSON de saída **após cada página** (escrita atômica via `.tmp`), então nunca perde progresso se interrompido.
- Mantém um arquivo de meta (`sources/dauphong_meta.json`) com a última execução e última página lida.
- Faz *merge* com o `sources/dauphong.json` existente usando `infohash` como chave, atualizando entradas já conhecidas.
- Filtra entradas **sem nenhuma seed** (apenas torrents com seeds ≥ 1 são gravados).
- Quando há múltiplos torrents com o mesmo nome, ordena pelo mais recente primeiro.

## Formato de saída

Compatível com o padrão Hydra Launcher (mesmo formato de `empress.json`):

```json
{
  "name": "Dauphong",
  "downloads": [
    {
      "title": "Nome do jogo",
      "uris": ["magnet:?xt=urn:btih:...&dn=...&tr=..."],
      "uploadDate": "2024-11-05T00:00:00.000Z",
      "fileSize": "16.9 GB"
    }
  ]
}
```

## Arquivo de meta

`sources/dauphong_meta.json` é atualizado após cada página:

```json
{
  "last_run": "2026-03-31T22:51:56Z",
  "last_page": 105,
  "total_entries": 5250
}
```

## Requisitos

- Python 3.8+
- Dependências listadas em `requirements.txt`

## Instalação

```bash
python3 -m pip install -r requirements.txt
```

## Uso

```bash
# Crawl completo a partir da página 1, sem limite de páginas (recomendado)
python3 -u scripts/dauphong_crawler.py --start-page 1 --sleep 0.5

# Limitar o número de páginas (ex.: primeiras 5 páginas)
python3 -u scripts/dauphong_crawler.py --start-page 1 --max-pages 5 --sleep 0.5

# Retomar de onde parou após uma interrupção (ver last_page em dauphong_meta.json)
python3 -u scripts/dauphong_crawler.py --start-page 106 --sleep 0.5

# Especificar arquivo de saída explicitamente
python3 -u scripts/dauphong_crawler.py --start-page 1 --output sources/dauphong.json
```

## Opções

| Opção | Padrão | Descrição |
|---|---|---|
| `--start-page N` | `1` | Página inicial (útil para retomar após interrupção) |
| `--max-pages N` | sem limite | Número máximo de páginas a buscar |
| `--sleep SECONDS` | `1.0` | Intervalo entre requisições (segundos) |
| `--output PATH` | `sources/dauphong.json` | Caminho do arquivo de saída |
| `--meta PATH` | `sources/dauphong_meta.json` | Caminho do arquivo de meta |

## Notas e boas práticas

- Ajuste `--sleep` para reduzir a carga no servidor e evitar bloqueios (recomendado: 0.5–1.0).
- O dauphong tem ~230 páginas (~11.500 torrents); uma execução completa leva alguns minutos.
- O script é seguro para reexecutar periodicamente — faz merge por `infohash` e atualiza seeds/leech.
- Use localmente ou numa runner confiável; dependendo da rede, pode ser necessário VPN ou proxy para acessar o TPB.

## Licença e avisos

Use o crawler de acordo com as leis locais e os termos dos serviços consultados.

---
Gerado pelo utilitário de manutenção de sources para Hydra Launcher.
