# 🛡️ Respaldo Maestro del Proyecto & Calibración IA — Stake Multi-Sport Ultra-Safe Bot

**ID de Conversación Antigravity:** `870c61b0-62e9-41c9-8f2a-933862b8ba26`  
**Última Actualización del Respaldo:** `2026-11-05`  
**Carpeta Local del Proyecto:** `c:\Users\Franyer Alb\Desktop\BOT STAKE`  
**Repositorio GitHub / Render Cloud 24/7:** `C:\Users\Franyer Alb\Documents\GitHub\bot-stake-ai` (`https://bot-stake-ai.onrender.com`)  
**Base de Datos SQLite en Vivo:** `c:\Users\Franyer Alb\Desktop\BOT STAKE\stake_paper_30k.db`

---

## 1. Arquitectura General del Sistema

1. **Motor Dual 24/7 (`main.py`)**:
   - **Modo Local (PC)**: Se conecta directamente a `https://stake.com/_api/graphql` usando `curl_cffi` (`impersonate="chrome124"`), escanea 8 deportes en vivo cada 6 segundos y envía automáticamente los datos en tiempo real a Render mediante el puente `/api/bridge_push` (`push_bridge_to_cloud`).
   - **Modo Cloud Autónomo 24/7 (Render)**: Si la PC está apagada y Cloudflare bloquea la IP del datacenter de Render en `stake.com`, conmuta sin demora al motor `fetch_kambi_cloud_fixtures` (`eu-offering-api.kambicdn.com`), manteniendo los 8 deportes y la liquidación activas las 24 horas.
2. **Base de Datos de Entrenamiento IA (`ai_observations`)**:
   - Captura automáticamente **todos los mercados candidatos con cuota `<= 1.45`** tanto cuando el bot **sí entra (`was_entered = 1`)** como cuando **los rechaza en modo sombra (`was_entered = 0`)**.
   - Al finalizar cada partido, liquida el resultado real (`WON`, `LOST`, `PUSH`) y calcula el `hypothetical_roi`, permitiendo exportar toda la telemetría desde los botones **Exportar CSV IA** (`/api/export_ai_csv`) y **JSON IA** (`/api/export_ai_json`).

---

## 2. Reglas Ultra-Seguras Calibradas con +27,000 Muestras Reales (`evaluate_fixture_safety`)

### ⚽ 1. Fútbol (`soccer`)
- **Doble Oportunidad (`1X / X2`) — Prioridad 99**:
  - `+2 goles` de ventaja: minuto `>= 75'`.
  - `+1 gol` de ventaja: minuto `>= 85'`, cuota `<= 1.04` y **partido cerrado (`total_score <= 2`, es decir `1-0` o `0-1`)**. Prohibido entrar con `+1 gol` en partidos abiertos (`2-1` / `1-2`).
- **Sin Empate (`Draw No Bet / DNB`) — Prioridad 98**:
  - Si empatan al final, **NO se pierde (`PUSH`, reembolso `0.00 USDT`)**.
  - `+2 goles` de ventaja: minuto `>= 75'`.
  - `+1 gol` de ventaja: minuto `>= 85'`, cuota `<= 1.04` y partido cerrado (`1-0` o `0-1`).
- **Menos de X.5 Goles (`Under X.5`) — Prioridad 96**:
  - Colchón de **3 goles (`cushion >= 2.5`)**: entre minuto `78'` y `89'`, con `total_score <= 2`.
  - Colchón de **2 goles (`cushion >= 1.5`)**: entre minuto `86'` y `89'`, prohibido en empates (`diff >= 1`), `total_score <= 2` y cuota `<= 1.03`.
  - Prohibido entrar en tiempo de descuento (`>= 90'`).
- **Ganador `1X2` — Prioridad 95**:
  - `+2 goles` al minuto `>= 83'` o `+3 goles` al minuto `>= 72'`.
- **Ambos Anotan: NO (`BTTS: No`)**:
  - **Deshabilitado para dinero real (`lock_ok = False`)**, guardado únicamente en modo sombra para la IA, porque 1 solo gol en contra causa pérdida total (`-500 USDT`), mientras que `DNB` reembolsa y `Doble Oportunidad` gana.

### 🎾 2. Tenis (`tennis`) — Regla de Doble Colchón de Set vs Set Final
- **Prohibido entrar en el 1er Set (`0-0 sets`)** y prohibido entrar en descansos (`has_active_set`).
- **En el 2do Set (`1-0` o `0-1` sets, con colchón de set)**: el líder del partido debe ir ganando también el 2do set por **`5-2 / 5-3 / 5-1 / 5-0`** (`cur_leader_games >= 5` y `game_diff >= 2`) o con **doble quiebre `4-1 / 4-0`** (`cur_leader_games >= 4` y `game_diff >= 3`). **Bloqueado el `4-2` (un solo quiebre)**.
- **En el 3er Set (`1-1` sets, muerte súbita sin colchón de set)**: **bloqueados el `5-3` y el `5-2`** (un solo break); solo entra con **doble quiebre de saque (`5-1` o `5-0`, `cur_leader_games >= 5` y `game_diff >= 4`)**.

### 🏓 3. Ping Pong (`table-tennis`) — Regla `2-0` vs `2-1` Sets
- Verificación obligatoria de **set activo en juego** (`has_active_set = (len(periods) > (h_score + a_score)) and (not is_paused_break)`), bloqueando cualquier entrada durante descansos entre sets (`Second break`, `Third break`).
- **Con `2-0` en sets (`diff >= 2`, doble colchón de set)**: `>= 8` puntos y ventaja `>= +3` puntos (`8-5`, `9-6`, `10-7`).
- **Con `2-1` en sets (`diff == 1`, 4to set)**: exige **`>= 9` puntos con ventaja `>= +3` (`9-6`, `10-7`)** o **`>= 8` puntos con ventaja `>= +4` (`8-4`, `9-5`)**, bloqueando remontadas desde `8-5`.

### 🏐 4. Voleibol (`volleyball`) — Regla `2-0` vs `2-1` Sets
- Verificación obligatoria de **set activo en juego** (`has_active_set`).
- **Con `2-0` en sets (`diff >= 2`, doble colchón de set)**: `>= 18` puntos y ventaja `>= +4` puntos.
- **Con `2-1` en sets (`diff == 1`, 4to set)**: exige **`>= 21` puntos con ventaja `>= +4` (`21-17`, `22-18`)** o **`>= 19` puntos con ventaja `>= +6` (`19-13`)**, bloqueando remontadas desde `18-14`.

### 🏀 5. Baloncesto (`basketball`)
- Solo en el **4to Cuarto (`4th quarter / Q4 / OT`)** verificando los **segundos reales restantes (`rem_seconds`)**:
  - `+12 pts` o más con **`<= 3:00` minutos restantes (`0 <= rem_s <= 180`)**.
  - `+18 pts` o más con **`<= 5:00` minutos restantes (`0 <= rem_s <= 300`)**.
  - `+24 pts` o más en el 4to cuarto.

### 🏒 6. Hockey sobre Hielo (`ice-hockey`)
- **Prohibido entrar con `+1 gol`** (por la regla de *Empty Net 6v5* sin portero en los últimos minutos).
- Solo en el **3er Período (`3rd period / P3 / OT`)**:
  - `+2 goles` de ventaja con **reloj real `<= 2:00` minutos restantes (`0 <= rem_s <= 120`)** y mercado **`Winner (Incl. Overtime and Penalties)`**.
  - `+3 goles` de ventaja en los últimos `8:00` minutos (`0 <= rem_s <= 480` o `minute >= 52'`).

---

## 3. Blindaje de Liquidación (`settle_pending_bets`)
1. **Conteo de sets desde `periodScores` (`_extract_set_counts_from_periods`)**:
   - En Tenis, Voleibol y Ping Pong, cuando Stake finaliza un partido y resetea `homeScore: 0, awayScore: 0`, el liquidador cuenta los sets ganados directamente desde `eventStatus.periodScores`.
2. **Protección contra transición prematura en partidos por sets**:
   - No liquida ni guarda en caché un partido por sets recién finalizado hasta que el ganador tenga los `2` sets reglamentarios (`max(final_h, final_a) >= 2`), evitando falsos negativos en el segundo de cierre.
3. **Protección Anti-Reset de Marcador en Vivo**:
   - `last_h` y `last_a` nunca se sobrescriben con `0-0` si el partido ya tenía puntuación acumulada.

---

## 4. Comandos Rápidos para Iniciar o Actualizar

### Iniciar el bot en tu PC:
```powershell
cd "c:\Users\Franyer Alb\Desktop\BOT STAKE"
.\.venv\Scripts\python.exe main.py
```
- Panel web local: `http://localhost:6111`
- Panel web Cloud 24/7: `https://bot-stake-ai.onrender.com`
