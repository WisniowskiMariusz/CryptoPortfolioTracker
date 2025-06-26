from app import crud

class DummyQuery:
    def __init__(self, exists):
        self._exists = exists
    def filter(self, *a, **kw): return self
    def first(self): return object() if self._exists else None

class DummySession:
    def __init__(self, exists=False):
        self._exists = exists
        self.added = []
        self.committed = False
        self.refreshed = []
    def query(self, *a, **kw): return DummyQuery(self._exists)
    def add(self, obj): self.added.append(obj)
    def commit(self): self.committed = True
    def refresh(self, obj): self.refreshed.append(obj)

class DummyPriceHistory:
    def __init__(self, symbol, interval, time, price, source):
        self.symbol = symbol
        self.interval = interval
        self.time = time
        self.price = price
        self.source = source

def test_candle_exists_true():
    session = DummySession(exists=True)
    result = crud.candle_exists(session, "BTCUSDT", "1d", "2024-01-01T00:00:00")
    assert result is True

def test_candle_exists_false():
    session = DummySession(exists=False)
    result = crud.candle_exists(session, "BTCUSDT", "1d", "2024-01-01T00:00:00")
    assert result is False

def test_create_candle(monkeypatch):
    # Patch models.PriceHistory to DummyPriceHistory
    monkeypatch.setattr("app.models.PriceHistory", DummyPriceHistory)
    session = DummySession()
    candle = {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "time": "2024-01-01T00:00:00",
        "price": 42000.0,
        "source": "binance"
    }
    db_candle = crud.create_candle(session, candle)
    assert isinstance(db_candle, DummyPriceHistory)
    assert session.added[0] == db_candle
    assert session.committed is True
    assert session.refreshed[0] == db_candle