"""Integration test for live pipeline."""
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")

from main import create_application

app = create_application()
app.start()
print("Waiting 45s for live data...")
for i in range(9):
    time.sleep(5)
    fs = app.get_feed_status()
    eng = app.flow_engine
    s = eng.get_market_summary()
    print(
        f"[{(i+1)*5}s] mode={fs['active_mode']} spy={fs['spy_status']} opt={fs['options_status']} "
        f"registry={fs['registry_count']} trades={eng.trades_received} unreg={eng.trades_unregistered} "
        f"call_vol={s['call_volume']:.0f} put_vol={s['put_volume']:.0f} "
        f"matrix={len(eng.get_matrix_df())} spy=${eng.spy_price:.2f} err={fs.get('last_error')}"
    )

app.stop()
print("DONE")
