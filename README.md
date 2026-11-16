# CI Stream Worker (`workerhund003`)

Worker isolado para captura contínua de transmissões públicas da Twitch via GitHub Actions, segmentação sem recodificação (`ffmpeg -c copy` via MPEG-TS -> MP4 `+faststart`) e envio imediato com verificação de integridade (MD5 + tamanho) para o Google Drive.

## Pasta de destino no Drive

Por padrão, cada bloco é gravado em:

```text
Youtube-Farm-Clips/<projeto_drive>/<streamer>/00_ORIGINAIS/<YYYY-MM-DD>/
```

(usando fuso `America/Sao_Paulo`).

## Secrets necessários no repositório

- `GDRIVE_TOKEN_JSON_B64`: conteúdo do `token.json` OAuth em Base64 (`base64 -w0 token.json`).

## Disparo manual via GitHub CLI

```bash
gh workflow run ci-worker.yml \
  --repo workerhund003/ci-stream-worker \
  -f streamer=alanzoka \
  -f qualidade=best \
  -f tempo_corte=900 \
  -f projeto_drive=TWITCH_LIVES
```
