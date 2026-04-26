# Rewardbot-Polymarket
Async market-making bot for Polymarket prediction markets — captures liquidity rewards via two-sided quoting with risk controls.
# Polymarket Liquidity Rewards Bot

Bot de **market making** para [Polymarket](https://polymarket.com) orientado a capturar **recompensas de liquidez (LP rewards)** mediante cotizaciones de doble lado (YES/NO) en mercados de predicción, con control de inventario y riesgo.

> El bot lee un *snapshot* de mercados, selecciona los más rentables por relación reward / competencia, coloca órdenes límite cerca del precio medio y reprecia/cancela por ciclos para mantenerse competitivo en el libro. Incluye tres modos de ejecución (`dry`, `paper`, `live`) y kill-switches por drawdown, pérdida diaria y errores.

---

## Tabla de contenidos

1. [Arquitectura](#arquitectura)
2. [Modos de operación](#modos-de-operación)
3. [Flujo de un ciclo](#flujo-de-un-ciclo)
4. [Estrategias de cotización](#estrategias-de-cotización)
5. [Mecanismos adaptativos](#mecanismos-adaptativos)
6. [Gestión de riesgo](#gestión-de-riesgo)
7. [Instalación](#instalación)
8. [Configuración](#configuración)
9. [Uso](#uso)
10. [Parámetros CLI clave](#parámetros-cli-clave)
11. [Estado y persistencia](#estado-y-persistencia)
12. [Reportes de rendimiento](#reportes-de-rendimiento)
13. [Tests](#tests)
14. [Aviso legal](#aviso-legal)

---

## Arquitectura

| Módulo | Responsabilidad |
|---|---|
| `liquidity_rewards_bot.py` | Bucle async principal, generación de quotes, simulación de fills, tracking de PnL (~1.943 líneas). |
| `execution_adapter.py` | Capa de ejecución agnóstica al modo; envuelve el cliente CLOB live y el stub dry-run. |
| `live_execution_client.py` | Cliente HTTP firmado contra el CLOB de Polymarket (modo `live`). |
| `rewards_market_selector.py` | Filtra y rankea mercados por liquidez, spread, rewards y tiempo a expiración. |
| `market_maker.py` | Helpers de sizing de órdenes. |
| `inventory_manager.py` | Posiciones por mercado y exposición global; aplica límites. |
| `risk_manager.py` | Kill-switches: drawdown, pérdida diaria, fill-rate, errores. |
| `order_manager.py` | Lifecycle de órdenes en dry-run. |
| `state_store.py` | Persistencia en SQLite (snapshots de ciclos, exposición, órdenes). |
| `build_live_snapshot.py` | Descarga datos live de Polymarket y genera `ranking_snapshot_live.json`. |
| `live_performance_report.py` | Reporte legible de rendimiento desde el SQLite. |
| `rewards_pnl_report.py` | Análisis de PnL ajustado por rewards. |
| `test_fixes.py` | Suite de tests unitarios con `unittest.mock`. |

---

## Modos de operación

| Modo | Descripción |
|---|---|
| `dry` | 100% local, simulación probabilística de fills, sin red. Ideal para iterar lógica. |
| `paper` | Misma simulación de fills pero usando datos de libro reales (si están disponibles). Realismo intermedio. |
| `live` | Órdenes reales vía `py-clob-client`. Requiere `--confirm-live YES` y `--enable-live-execution`. |

---

## Flujo de un ciclo

1. Carga `ranking_snapshot_live.json` y construye dataclasses `MarketSnapshot`.
2. `RewardsMarketSelector` filtra y rankea por la métrica `rewards / competencia`.
3. La rotación de mercados activos selecciona qué subconjunto cotizar este ciclo.
4. Para cada mercado seleccionado:
   - Refresca top-of-book (solo `live`).
   - `generate_quotes()` produce bid/ask al `--distance-to-mid-bps` configurado.
   - `InventoryManager` valida exposición por mercado y global.
   - `RiskManager` revisa drawdown, daily loss y fill efficiency.
   - `_replace_quotes()` cancela las órdenes obsoletas y coloca las nuevas.
5. `_simulate_cycle_fills()` (dry/paper) o fills reales (live) actualizan posiciones.
6. `_compute_live_trade_pnl_estimate()` actualiza PnL y equity.
7. `StateStore` graba el snapshot en `bot_state.sqlite`.
8. Duerme `--refresh-seconds`.

---

## Estrategias de cotización

Configurable con `--quote-style`:

- **`mid`** — Distancia fija en bps respecto al mid-price (por defecto).
- **`top_of_book`** — Cotiza justo dentro del best bid/ask.
- **`maker_cross_controlled`** — Maker-cross agresivo con control de agresividad.

---

## Mecanismos adaptativos

- **No-fill tightening** (`--no-fill-tighten-cycles`, `--tighten-step-bps`) — estrecha el spread cuando los fills se secan.
- **Mid-drift pause** (`--mid-drift-pause-bps`) — pausa cotizaciones tras un salto grande del mid.
- **No-fill rotation** (`--no-fill-rotate-cycles`) — rota mercados tras N ciclos sin fills.
- **Imbalance unwinding** (`--imbalance-threshold`) — sesga o reduce quotes para rebalancear inventario YES/NO.

---

## Gestión de riesgo

- Límite de posición neta por mercado (`--max-position-per-market`).
- Límite de exposición agregada (`--global-exposure-limit`).
- Kill-switch por drawdown (`--max-drawdown-pct`).
- Kill-switch por pérdida diaria (`--daily-loss-limit-usd`).
- Tope de fills/cancels por minuto.
- Pausa por bajo balance utilizable.
- Cancelación masiva si se detectan condiciones peligrosas.

---

## Instalación

Requisitos: Python 3.11+ (probado con 3.14).

```bash
git clone https://github.com/<tu-usuario>/<tu-repo>.git
cd <tu-repo>
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
source .venv/bin/activate

pip install py-clob-client httpx
```

> *No hay `requirements.txt` por ahora.* Las dependencias mínimas son `py-clob-client` y `httpx`. Añade lo que tu entorno requiera.

---

## Configuración

### 1. Credenciales CLOB

Copia el ejemplo y rellena con tus claves reales:

```bash
cp clob_creds.example.json clob_creds.json
```

```json
{
  "api_key": "TU_POLY_API_KEY",
  "api_secret": "TU_POLY_API_SECRET",
  "api_passphrase": "TU_POLY_API_PASSPHRASE"
}
```

### 2. Variables de entorno (modo `live`)

Copia `.env.example` a `.env` y rellena:

```ini
POLY_API_KEY=
POLY_API_SECRET=
POLY_API_PASSPHRASE=
PRIVATE_KEY=
FUNDER_ADDRESS=
POLY_BASE_URL=https://clob.polymarket.com
CHAIN_ID=137
SIGNATURE_TYPE=2
RELAYER_API_KEY=
RELAYER_API_KEY_ADDRESS=
```

> ⚠️ **Nunca subas estos archivos al repo.** Ya están en `.gitignore`.

---

## Uso

### Dry mode (sin red, sin órdenes reales)

```bash
python liquidity_rewards_bot.py \
  --ranking-file ranking_snapshot.json \
  --top-n 5 --min-liquidity 10000 \
  --min-spread-bps 15 --distance-to-mid-bps 8 \
  --quote-size 25 --log-level INFO
```

### Paper mode (simulación realista con datos live)

```bash
.venv/Scripts/python.exe liquidity_rewards_bot.py \
  --mode paper --ranking-file ranking_snapshot_live.json \
  --cycles 180 --refresh-seconds 0.20 \
  --top-n 8 --active-markets 2 \
  --quote-size 200 \
  --max-position-per-market 700 \
  --global-exposure-limit 1800
```

### Stress test

```bash
.venv/Scripts/python.exe liquidity_rewards_bot.py \
  --mode paper --ranking-file ranking_snapshot_live.json \
  --cycles 300 --refresh-seconds 0.10 \
  --top-n 15 --active-markets 4 \
  --quote-size 180 --max-drawdown-pct 18 \
  --daily-loss-limit-usd 120 --log-level INFO
```

### Run reproducible (verificar fixes)

```bash
.venv/Scripts/python.exe liquidity_rewards_bot.py \
  --mode paper --ranking-file ranking_snapshot_live.json \
  --cycles 180 --refresh-seconds 0.20 \
  --top-n 8 --active-markets 2 \
  --rotation-step 1 --market-hold-cycles 8 \
  --disable-state-persistence --random-seed 42
```

### Construir un snapshot live nuevo

```bash
python build_live_snapshot.py
```

---

## Parámetros CLI clave

El bot tiene 100+ flags. Los más impactantes:

| Flag | Efecto |
|---|---|
| `--top-n` | Mercados a seleccionar del snapshot. |
| `--active-markets` | Mercados cotizados simultáneamente por ciclo. |
| `--rotation-step` | Mercados intercambiados cada ciclo. |
| `--market-hold-cycles` | Ciclos mínimos antes de rotar un mercado. |
| `--distance-to-mid-bps` | Distancia de quote (más tight = más fills, más riesgo). |
| `--quote-size` | Tamaño por lado en USD. |
| `--max-position-per-market` | Tope de posición neta por mercado. |
| `--global-exposure-limit` | Tope de la suma de exposiciones. |
| `--max-drawdown-pct` | Umbral de kill-switch. |
| `--daily-loss-limit-usd` | Kill-switch de pérdida diaria. |
| `--disable-state-persistence` | Salta escrituras a SQLite (útil para tests). |
| `--random-seed` | Simulación reproducible de fills. |

---

## Estado y persistencia

`bot_state.sqlite` contiene tres tablas:

- **`cycle_snapshots`** — un registro por ciclo: timestamp, fills, PnL realizado, equity, drawdown.
- **`market_exposure`** — exposición YES/NO por mercado a lo largo del tiempo.
- **`open_orders`** — órdenes vivas con queue-ahead-ratio para análisis de prioridad.

`ranking_snapshot_live.json` (~732 KB) es el input de mercados; regéneralo con `build_live_snapshot.py`.

---

## Reportes de rendimiento

```bash
# PnL ajustado por rewards
python rewards_pnl_report.py --state-db bot_state.sqlite --hours 24.0 --rewards-usd 100.0 --mode live

# Reporte legible para humanos
python live_performance_report.py --state-db bot_state.sqlite --hours 24.0 --days-rewards 2
```

---

## Tests

```bash
.venv/Scripts/python.exe test_fixes.py
```

Cubre:

1. Atribución correcta de fills a `token_id`s.
2. Limpieza correcta en `_replace_quotes()` ante fallos parciales.
3. Cap de exposición usando `abs()` sobre la posición neta.

---

## Aviso legal

Este software se publica con fines **educativos y de investigación**. Operar market making en Polymarket implica:

- **Riesgo de pérdida de capital** (drawdown, fills adversos en eventos discretos, slippage).
- **Riesgo regulatorio** según tu jurisdicción.
- **Riesgo operativo** (latencia, desconexiones, bugs).

El autor **no garantiza ganancias** y **no se responsabiliza** del uso que hagas del código. Asegúrate de entender la lógica antes de ejecutar en `live` y empieza siempre con tamaños mínimos.
