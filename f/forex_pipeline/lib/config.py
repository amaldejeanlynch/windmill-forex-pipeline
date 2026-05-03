from datetime import datetime, timezone

INSTRUMENTS: list[str] = [
    # Major FX
    "EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF", "USD_CAD",
    "AUD_USD", "NZD_USD",
    # Metals
    "XAU_USD", "XAG_USD", "XCU_USD",
    # Energy
    "BCO_USD", "WTICO_USD", "NATGAS_USD",
    # Equity indices
    "SPX500_USD", "NAS100_USD", "US30_USD",
]

GRANULARITIES: list[str] = ["M5", "M15", "M30", "H1"]

PIPELINE_START_DATE: datetime = datetime(2026, 1, 1, tzinfo=timezone.utc)
