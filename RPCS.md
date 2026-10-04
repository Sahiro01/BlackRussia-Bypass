# Релеимые RPC

Сгенерировано из кода (`relay/relay.py`, `br_side/constants.py`, `samp_side/constants.py`).
`1:1` — пейлоад идёт как есть со сменой ID. Остальное — конверсия в ветке `_translate_br_rpc` / `_translate_samp_rpc`.

## BR -> SAMP (таблица)

| BR ID | BR имя | SAMP ID | SAMP имя | Примечание |
|---|---|---|---|---|
| 303 | SCR_SET_WEAPON_AMMO | 145 | SET_WEAPON_AMMO | 1:1 |
| 304 | DISABLE_RACE_CHECKPOINT | 39 | DISABLE_RACE_CHECKPOINT | 1:1 |
| 309 | SCR_DELETE_3D_TEXT_LABEL | 58 | DELETE_3D_TEXT_LABEL | 1:1, 2B |
| 310 | SCR_SET_CAMERA_POS | 157 | SET_CAMERA_POS | 1:1 |
| 315 | SCR_SET_PLAYER_ARMOUR | 66 | SET_PLAYER_ARMOUR | 1:1, режется `damage_ignore` |
| 322 | SCR_DISABLE_MAP_ICON | 144 | REMOVE_MAP_ICON | 1:1 |
| 325 | WORLD_PLAYER_REMOVE | 163 | WORLD_PLAYER_REMOVE | 1:1 (делит ID с SET_VEHICLE_Z_ANGLE) |
| 326 | SERVER_QUIT | 138 | SERVER_QUIT | 1:1 |
| 329 | WORLD_VEHICLE_ADD | 164 | WORLD_VEHICLE_ADD | 1:1 + нейтральный хвост повреждений |
| 334 | SCR_SET_VEHICLE_VELOCITY | 91 | SET_VEHICLE_VELOCITY | 1:1 (делит ID с RESET_PLAYER_WEAPONS) |
| 335 | SCR_SET_PLAYER_VELOCITY | 90 | SET_PLAYER_VELOCITY | 1:1 |
| 338 | SET_RACE_CHECKPOINT | 38 | SET_RACE_CHECKPOINT | 1:1 |
| 341 | SCR_SET_PLAYER_POS_FIND_Z | 13 | SET_PLAYER_POS_FIND_Z | 1:1 |
| 344 | REQUEST_SPAWN | 129 | REQUEST_SPAWN | спец. обработка |
| 347 | SCR_DIALOG_BOX | 61 | SCR_DIALOG_BOX | utf-8 -> cp1251 |
| 353 | SCR_PUT_PLAYER_IN_VEHICLE | 70 | PUT_PLAYER_IN_VEHICLE | 1:1 |
| 360 | CLIENT_MESSAGE | 93 | CLIENT_MESSAGE | utf-8 -> cp1251 |
| 372 | SET_SPAWN_INFO | 68 | SET_SPAWN_INFO | спец. обработка (экран класса локальный) |
| 373 | DESTROY_PICKUP | 63 | DESTROY_PICKUP | 1:1 |
| 374 | UPDATE_SCORES_PINGS_IPS | 155 | UPDATE_SCORES_PINGS_IPS | 1:1 (делит ID с FLASH_GANG_ZONE) |
| 375 | CHAT_BUBBLE | 59 | CHAT_BUBBLE | utf-8 -> cp1251, длина пересчитывается |
| 376 | SCR_TOGGLE_SELECT_TEXTDRAW | 83 | TOGGLE_SELECT_TEXT_DRAW | 1:1, 5B |
| 377 | CREATE_PICKUP | 95 | CREATE_PICKUP | 1:1 |
| 382 | ENTER_VEHICLE | 26 | ENTER_VEHICLE | 1:1 |
| 387 | SCR_SET_PLAYER_HEALTH | 14 | SET_PLAYER_HEALTH | 1:1, режется `damage_ignore` |
| 397 | WORLD_VEHICLE_REMOVE | 165 | WORLD_VEHICLE_REMOVE | 1:1 |
| 401 | SERVER_JOIN | 137 | SERVER_JOIN | пересборка (цвет/имя) |
| 409 | EXIT_VEHICLE | 154 | PLAYER_EXIT_VEHICLE | 1:1 |
| 411 | SCR_VEHICLE_PARAMS | 24 | SET_VEHICLE_PARAMS_EX | 1:1 (делит ID с WEATHER) |
| 414 | CHAT | 101 | CHAT | пересборка |
| 415 | SCR_DISPLAY_GAME_TEXT | 73 | SHOW_GAME_TEXT | 1:1 |
| 417 | SCR_SET_MAP_ICON | 56 | SET_MAP_ICON | иконка u16 -> u8 |
| 419 | WORLD_PLAYER_ADD | 32 | WORLD_PLAYER_ADD | пересборка (team/color/скиллы) |
| 428 | SCR_TOGGLE_PLAYER_CONTROLLABLE | 15 | TOGGLE_PLAYER_CONTROLLABLE | дропается при `toggle_player_controllable=false` |
| 432 | SCR_CREATE_3D_TEXT_LABEL | 36 | CREATE_3D_TEXT_LABEL | utf-8 -> cp1251 (Huffman) |
| 434 | SCR_SET_INTERIOR | 156 | SET_INTERIOR | 1:1 |
| 440 | SET_CHECKPOINT | 107 | SET_CHECKPOINT | 1:1 |
| 448 | SCR_SET_PLAYER_POS | 12 | SET_PLAYER_POS | 1:1 |

## BR -> SAMP (коллизии: один ID, разбор по пейлоаду)

| BR ID | Варианты | SAMP ID | Правило |
|---|---|---|---|
| 309 | DELETE_3D_TEXT_LABEL | 58 | всегда (2B) |
| 319 | CONNECTION_REJECTED / APPLY_ANIMATION | 130 / 86 | `<=1B` -> 130, иначе 86 |
| 334 | RESET_PLAYER_WEAPONS / SET_VEHICLE_VELOCITY | 21 / 91 | `0B` -> 21, иначе 91 |
| 349 | TEXT_DRAW_SET_STRING | 105 | `u16 id + u16 len + текст`, utf-8 -> cp1251 |
| 354 | HIDE_TEXT_DRAW | 135 | всегда (2B) |
| 362 | REMOVE_PLAYER_FROM_VEHICLE / GIVE_MONEY | 71 / 18 | `0B` -> 71, `4B` -> 18 |
| 402 | — | — | не используется (см. 433) |
| 412 | WORLD_PLAYER_DEATH / SET_VEHICLE_POS | 166 / 159 | `2B` -> 166, иначе 159 |
| 418 | SHOW_TEXT_DRAW | 134 | `65B + u16 len + текст`, utf-8 -> cp1251 |
| 421 | DISABLE_CHECKPOINT / INIT_GAME | 37 / 139 | пустой -> 37, иначе 139 |
| 433 | GIVE_PLAYER_WEAPON | 22 | 1:1, строго 8B (`u32 + u32`) |

## SAMP -> BR

| SAMP ID | SAMP имя | BR ID | BR имя | Примечание |
|---|---|---|---|---|
| 25 | CLIENT_JOIN | 67 | CLIENT_JOIN | пересборка ника |
| 26 | ENTER_VEHICLE | 382 | ENTER_VEHICLE | 1:1 |
| 50 | SERVER_COMMAND | 378 | SERVER_COMMAND | пересборка |
| 52 | SPAWN | 441 | SPAWN | пересборка |
| 53 | DEATH | 371 | DEATH | проверка размера |
| 83 | TOGGLE_SELECT_TEXT_DRAW (клик) | 437 | CICK_TEXTDRAW | 1:1, 2B (`u16 clickedid`) |
| 101 | CHAT | 414 | CHAT | пересборка, `/y` и команды перехватываются |
| 129 | REQUEST_SPAWN | 344 | REQUEST_SPAWN | пересборка |
| 131 | PICKED_UP_PICKUP | 436 | PICKED_UP_PICKUP | проверка размера |
| 154 | EXIT_VEHICLE | 409 | EXIT_VEHICLE | проверка размера |
| 155 | UPDATE_SCORES_PINGS_IPS | 374 | UPDATE_SCORES_PINGS_IPS | 1:1 |
| 62 | DIALOG_RESPONSE | gui 10 (JSON) | — | `{"r","i","l"}` в USER_INTERFACE_SYNC; NPC (1001) -> gui 63 `{"bk"}`, спавн (1002) отдельно |
| 207/200/211/203/206 | синки | 0x1E/0x22/33/0x26/0x0C | — | onfoot с конверсией полей, остальные 1:1 |

## Пакеты (не RPC)

| SAMP ID | Направление | BR ID |
|---|---|---|
| 6 INTERNAL_PING | <-> | 2 |
| 9 CONNECTED_PONG | -> | 10 |
| 10 | <- | 10 CONNECTED_PONG |
| 11 CONNECTION_REQUEST | -> | 8 |
| 12 AUTH_KEY_RESPONSE | -> | 6 |
| 20 RPC | <-> | 11 |
| 30 NEW_INCOMING_CONNECTION | -> | 20 |
| 32 DISCONNECTION_NOTIFICATION | -> | 15 |
| 41 RECEIVED_STATIC_DATA | -> | 8 |
| 200/203/206/207/211 синки | -> | 0x22/0x26/0x0C/0x1E/33 |
| 0x1E/0x22/33/0x26/0x0C синки | <- | 207/200/211/203/206 |
| 14 CONNECTION_REQUEST_ACCEPTED | <- | синтезируется локально |
| 15/17/20/25/26 | <- | 15 DISCONNECTION / 17 LOST / 20 ATTEMPT_FAILED / 25 BANNED / 26 INVALPASSWORD |

## Не релеится (видно в логах как `no SAMP mapping`)

Наблюдались на вайре: `323`, `343`, `364`, `388`, `391`, `447` (разные размеры), `384 SET_PLAYER_NAME` (известный ID, живого обработчика нет). SAMP -> BR: `205 STATS_UPDATE`, `115 GiveTakeDamage`. Кидай ID + размер + hex — добавим.
