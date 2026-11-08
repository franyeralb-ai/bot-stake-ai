# 🛡️ Respaldo Maestro del Proyecto & Calibración IA — Stake Multi-Sport Ultra-Safe Bot

**ID de Conversación Antigravity:** `870c61b0-62e9-41c9-8f2a-933862b8ba26`  
**Última Actualización del Respaldo:** `2026-11-06`  
**Carpeta Local del Proyecto:** `c:\Users\Franyer Alb\Desktop\BOT STAKE`  
**Carpeta de Respaldo en Escritorio (`ASD`):** `C:\Users\Franyer Alb\Desktop\ASD`  
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

## 2. Reglas Ultra-Seguras Calibradas con +30,000 Muestras Reales (`evaluate_fixture_safety`)

### ⚽ 1. Fútbol (`soccer`)
- **Doble Oportunidad (`1X / X2`) — Prioridad 99**:
  - `+2 goles` de ventaja: minuto `>= 75'`.
  - `+1 gol` de ventaja: minuto `>= 85'`, cuota `<= 1.04` y `total_score <= 2` (`1-0` o `0-1`, prohibido entrar en partidos abiertos `2-1`/`3-2`).
- **Sin Empate (`Draw No Bet / DNB`) — Prioridad 98**:
  - `+2 goles` de ventaja: minuto `>= 75'`.
  - `+1 gol` de ventaja: minuto `>= 85'`, cuota `<= 1.04` y `total_score <= 2` (si empatan al 90', devuelve el 100% del capital: `PUSH = 0.00 USDT`).
- **Under `X.5` Goles — Prioridad 96**:
  - Colchón `>= 2.5 goles` (necesita 3 goles en contra para perder): minuto `78'` a `89'` y `total_score <= 2` (`100.00% Win Rate`).
  - Colchón `== 1.5 goles` (necesita 2 goles en contra para perder): minuto `86'` a `89'`, `diff == 1` y **`total_score == 1` (`1-0` o `0-1`, `Under 2.5 Goles` = `31G / 0P = 100.00% Win Rate`)**, cuota `<= 1.03`. Prohibido con `cushion == 1.5` en `2-0`/`0-2` (evita partidos rotos `0-2 -> 0-4` como `#357`).
  - Prohibido entrar en tiempo de descuento (`>= 90'`).
- **Ganador (`1X2`) — Prioridad 95**:
  - Con `+2 goles` entre los minutos `75'` y `86'`, solo entran `Doble Oportunidad` o `Sin Empate (DNB)` (que protegen ante un `0-2 -> 2-2` como `#232`).
  - En `1X2` puro: exige `+2 goles` con minuto `>= 87'` y cuota `<= 1.03`, o `+3 goles` de ventaja al minuto `>= 75'`.
- **Ambos Anotan: NO (`BTTS: No`) — Solo Sombra IA (`lock_ok = False`)**:
  - Deshabilitado para dinero real por riesgo de muerte súbita con 1 solo gol (`0-1 -> 1-1` pierde `-500 USDT`, mientras que en `DNB` reembolsa y en `1X/X2` gana).

### 🎾 2. Tenis (`tennis`)
- **Prohibido entrar en el 1er Set (`0-0` sets)** y **prohibido entrar en descansos entre sets (`has_active_set`)**.
- **En el 2do Set (`1-0` o `0-1` sets, con colchón de 3er set — `93 Ganadas / 0 Perdidas = 100.00% Win Rate`)**:
  - El mismo jugador que ganó el 1er set debe liderar el 2do set con:
    - **`5-2`, `5-1`, `5-0` o `4-0`** (doble quiebre de ventaja), o
    - **`4-1` siempre que el rival NO vaya `30`, `40` o `AD` arriba en su saque** (`trailer_g_pts in ["0", "15"]`), bloqueando `1-4 (30-0)` a punto de ser `2-4` (`#260`).
    - **Prohibido `5-3` (`game_diff == 2`, 1 solo quiebre)**, eliminando remontadas como `#236` y `#299` (*Taylor Fritz vs Carlos Alcaraz*).
- **En el 3er Set (`1-1` sets, muerte súbita sin colchón)**:
  - Bloqueados `5-3` y `5-2` (un solo quiebre).
  - Exige **doble quiebre de ventaja a 1 game del partido: `5-1` o `5-0`** (`cur_leader_games >= 5 and game_diff >= 4`).
- **Liquidación Oficial anti-bug `0-0` y Detección de Retiros (`_extract_set_counts_from_periods`)**:
  - Calcula los sets completados (`(max(ph, pa) >= 6 and abs(ph - pa) >= 2) or max(ph, pa) >= 7`) directamente desde `periodScores`.
  - Si un partido de Tenis finaliza con un set incompleto (`0 < max(ph, pa) < 6`, ej. `[6-7, 7-5, 2-1]` en `#238`, `#291`, `#293`), detecta **Retiro (`is_retired = True`)** y lo liquida oficialmente como **`PUSH / Reembolso (0.00 USDT)`**.

### 🏓 3. Ping Pong / Tenis de Mesa (`table-tennis`)
- **Filtro Anti-Descanso (`has_active_set`)**:
  - Exige `len(periods) > (h_score + a_score)` y que no esté en descanso (`Second break`, etc.), evitando leer el marcador del set anterior.
- **Con `2-0` en sets (`diff >= 2`, doble colchón de set)**:
  - Exige liderar el 3er set con **`>= 8` puntos y ventaja `>= +3` puntos** (`8-5`, `8-4`, `9-6`, `10-7`, etc.) — **`100.00% Win Rate`**.
- **Con `2-1` en sets (`diff == 1`, 4to set)**:
  - Bloqueados `8-4`, `8-5`, `9-6` y `10-7` (que sufrieron remontadas al 5to set en Kambi Cloud). Exige únicamente paliza extrema **`>= 10` puntos y ventaja `>= +5` (`10-5` o mejor)** — **`100.00% Win Rate`**.

### 🏐 4. Voleibol (`volleyball`)
- **Filtro Anti-Descanso (`has_active_set`)**:
  - Exige set en curso activo (`len(periods) > (h_score + a_score)`).
- **Con `2-0` en sets (`diff >= 2`, doble colchón de set)**:
  - Exige liderar el 3er set con **`>= 18` puntos y ventaja `>= +4` puntos** — **`100.00% Win Rate`**.
- **Con `2-1` en sets (`diff == 1`, 4to set)**:
  - Exige **`>= 21` puntos y ventaja `>= +4`** (`21-17`, `22-18`) o **`>= 19` puntos y ventaja `>= +6`** (`19-13`, `20-14`), bloqueando remontadas desde `18-14` en el 4to set — **`100.00% Win Rate`**.

### 🏀 5. Baloncesto (`basketball`)
- Solo en el **4to Cuarto (`Q4`) o Prórroga (`OT`)** con reloj restante real verificado (`rem_s >= 0`):
  - `<= 3:00` restantes (`rem_s <= 180`) con **`+12` puntos o más**.
  - `<= 5:00` restantes (`rem_s <= 300`) con **`+18` puntos o más**.
  - **`+24` puntos o más** en cualquier momento del 4to cuarto.

### 🏒 6. Hockey sobre Hielo (`ice-hockey`)
- **Prohibido entrar con `+1` gol** (por el ataque `6 contra 5` sin portero en los últimos 2 minutos).
- Solo en el **3er Período (`P3`)**:
  - Con **`+2` goles**: exige mercado **`Winner (Incl. Overtime and Penalties)`** y reloj restante real **`<= 2:00` (`0 <= rem_s <= 120`)**.
  - Con **`+3` goles o más**: últimos 8 minutos del 3er período (`minute >= 52` o `0 <= rem_s <= 480`).

### 🏈 7. Fútbol Americano (`american-football`) y ⚾ 8. Béisbol (`baseball`)
- **Fútbol Americano**: `+14` puntos (2 Touchdowns) en el `4to Cuarto (Q4)`, o `+21` puntos (3 Touchdowns) en el `3er Cuarto (Q3)`.
- **Béisbol**: `+3` carreras o más en la `8va` o `9na` entrada (`8th` / `9th inning`).

---

## 3. Ubicación de los Respaldos Permanentes de este Chat

1. **En la carpeta principal del proyecto (`BOT STAKE`)**:
   - Resumen Maestro y Reglas: `c:\Users\Franyer Alb\Desktop\BOT STAKE\RESPALDO_MAESTRO_CHAT_Y_CALIBRACION_IA.md`
   - Historial Completo Legible (Markdown): `c:\Users\Franyer Alb\Desktop\BOT STAKE\historial_chat_respaldo\HISTORIAL_COMPLETO_CONVERSACION.md`
   - Transcripción Íntegra Original (JSONL): `c:\Users\Franyer Alb\Desktop\BOT STAKE\historial_chat_respaldo\transcript_full.jsonl`
2. **En tu carpeta del Escritorio (`ASD`)**:
   - `C:\Users\Franyer Alb\Desktop\ASD\RESPALDO_MAESTRO_CHAT_Y_CALIBRACION_IA.md`
   - `C:\Users\Franyer Alb\Desktop\ASD\HISTORIAL_COMPLETO_CONVERSACION.md`
   - `C:\Users\Franyer Alb\Desktop\ASD\transcript_full.jsonl`
3. **En tu repositorio de GitHub (`bot-stake-ai`)**:
   - `C:\Users\Franyer Alb\Documents\GitHub\bot-stake-ai\RESPALDO_MAESTRO_CHAT_Y_CALIBRACION_IA.md`
   - `C:\Users\Franyer Alb\Documents\GitHub\bot-stake-ai\HISTORIAL_COMPLETO_CONVERSACION.md`
4. **En el directorio de Artefactos de Antigravity**:
   - `C:\Users\Franyer Alb\.gemini\antigravity\brain\870c61b0-62e9-41c9-8f2a-933862b8ba26\RESPALDO_MAESTRO_CHAT_Y_CALIBRACION_IA.md`
