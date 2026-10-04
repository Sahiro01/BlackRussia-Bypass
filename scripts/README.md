# Скрипты

Кидай сюда свои `.py` с функцией `setup(api, events)` — байпасс подхватит их сам при старте (`core/loader.py`). Файлы вида `_*.py` игнорируются.

```python
def setup(api, events):
    def on_chat(cmd, text):
        if cmd == 'ping':
            api.send_samp_chat('pong!')
            return True  # True = съесть, дальше не пересылать
        return False

    events.on('OnCommand', on_chat)
```

## Хуки (`events.on`, верни True чтобы съесть пакет)

| Хук | Аргументы | Когда |
|---|---|---|
| `OnRpcSAMPReceive` | `(rpc_id, payload)` | RPC от клиента SAMP до трансляции |
| `OnRpcBRReceive` | `(rpc_id, payload)` | RPC от BR до трансляции |
| `OnPacketSAMPReceive` | `(event,)` | пакет от клиента SAMP до трансляции |
| `OnPacketBRReceive` | `(event,)` | пакет от BR до трансляции |
| `OnSendPacketBR` | `(entries,)` | перед отправкой в BR |
| `OnSendPacketSAMP` | `(entries,)` | перед отправкой клиенту SAMP |
| `OnCommand` | `(cmd, text)` | сообщение клиента, начинающееся с `/` |

## API (`core/api.py`)

`send_to_br(body)`, `send_to_samp(body)`, `send_br_rpc(...)`, `send_samp_rpc(...)`, `send_br_interface(gui_id, dict)`, `send_samp_chat(text, color)`, `log(msg)`.

Свои скрипты в этой папке локальные и в git не коммитятся (см. корневой `.gitignore`).
