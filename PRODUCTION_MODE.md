# SPX Options Flow Dashboard — Production Live Mode

## Verified Working Pipeline

Integration test confirmed live data flowing:
- **21,098** SPXW contracts in registry
- **2,239+** live option trades processed
- **0** unregistered trades (100% registry match)
- **720** strikes in full-chain matrix
- **CALL vol: 3,237 | PUT vol: 3,028** from real TCBBO

## Architecture

```
EQUS.MINI (SPY)     ──► SpotEstimator ──► SPX estimate
                              │
OPRA.PILLAR (TCBBO) ──► SymbolMapping ──► Contract Registry (instrument_id)
                              │
                              ▼
                     TCBBO (trade + NBBO) ──► BUY/SELL Classifier
                              │
                              ▼
                     Full Strike Matrix ──► Dashboard
```

## Root Causes Fixed

| Bug | Fix |
|-----|-----|
| Live definitions sent as `SymbolMappingMsg`, not `InstrumentDefMsg` | OCC parser + symbology handler |
| Empty registry → all trades dropped | Historical bootstrap + disk cache |
| `snapshot=True` not supported on live | Removed — use bootstrap + symbology |
| EQUS.MINI + OPRA on one connection fails | Two parallel connections (Databento requirement) |
| Connection limit from stale sessions | Proper `client.stop()`, 30s backoff, SPXW starts first |
| Bootstrap timeout on restart | Disk cache at `logs/contract_registry.json` |
| Empty matrix KeyError | Empty DataFrame with proper columns |

## Setup

```bash
pip install -r requirements.txt
```

Ensure `.env` contains:
```
DATABENTO_API_KEY=your_key_here
```

## Run

```bash
python -m streamlit run dashboard.py
```

**Important:** Close ALL other Databento live connections first:
- Old Streamlit tabs
- Previous test scripts
- Other dashboards using the same API key

Databento plans typically allow **2 simultaneous live connections** (SPY + SPXW).

If you see `connection limit` error → close everything → click **Reconnect Feed** in sidebar.

## What To Expect (first 60 seconds)

1. **0–10s:** Registry loads from cache OR historical bootstrap (~20k contracts)
2. **10–20s:** SPXW TCBBO feed connects, symbology mappings stream in
3. **20–40s:** First option trades appear in matrix and Time & Sales
4. **40s+:** SPY feed connects, live SPX spot updates from Databento ticks

## Health Indicators

| Indicator | Meaning |
|-----------|---------|
| 🟢 SPY feed LIVE | EQUS.MINI receiving ticks |
| 🟢 SPXW feed LIVE | OPRA TCBBO receiving trades |
| 🟡 STALE | Connected but no ticks for 30s |
| 🔴 OFFLINE | Connection failed |

## Configuration (`config.py`)

```python
OPTIONS_SYMBOL = "SPXW.OPT"      # stype_in=parent
OPTIONS_SCHEMA = "tcbbo"         # trade + NBBO
STALE_FEED_SECONDS = 30
```

## No Fake Data

If feed is offline → **🔴 LIVE FEED OFFLINE — NO SIMULATION**
No fallback prices, no random trades, no fake health metrics.
