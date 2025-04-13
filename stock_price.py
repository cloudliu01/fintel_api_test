import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

from common import OutputPathSingleton

def create_stock_data_model(db_url):
    """
    Dynamically creates a StockData ORM model using DateTime for PostgreSQL
    or String for SQLite. Returns (Base, StockData) where Base is the declarative_base
    and StockData is the ORM class.
    """
    Base = declarative_base()

    # Detect whether we are using PostgreSQL or SQLite
    if db_url.startswith("postgresql://") or db_url.startswith("postgresql+psycopg2://"):
        date_column_type = DateTime
    elif db_url.startswith("sqlite://"):
        # For SQLite, we'll store as String (ISO format) – 
        # or you could still use DateTime (SQLAlchemy will store it as TEXT).
        date_column_type = String
    else:
        raise ValueError(f"Unsupported DB URL scheme: {db_url}")

    class StockData(Base):
        __tablename__ = "stock_data"

        symbol = Column(String, primary_key=True)
        date = Column(date_column_type, primary_key=True)
        open = Column(Float)
        high = Column(Float)
        low = Column(Float)
        close = Column(Float)
        volume = Column(Float)

    return Base, StockData


class StockDataManager:
    """
    Manages downloading, storing, and retrieving stock data in either:
      - SQLite (using a textual date column), or
      - PostgreSQL (using a native DateTime column).
    """
    def __init__(self, db_url, symbols, default_start_date="2020-01-01"):
        """
        db_url: A full SQLAlchemy database URL, e.g.
                  'sqlite:///stocks.db'
                  'postgresql://user:pass@host:port/dbname'
        symbols: List of ticker symbols (e.g. ['aapl', 'msft']).
        default_start_date: Earliest date to fetch if no data is in the DB.
        """
        self.db_url = db_url
        self.symbols = [s.upper() for s in symbols]
        self.default_start_date = default_start_date

        # Dynamically create our ORM base and model class
        self.Base, self.StockData = create_stock_data_model(db_url)

        # Create the engine and session factory
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

        # Create table if it doesn't exist
        self.Base.metadata.create_all(self.engine)

    def _get_last_datetime_in_db(self, symbol):
        """
        Return the most recent datetime for the given symbol, as a Python datetime.
        
        For PostgreSQL: the date column is stored as a real DateTime.
        For SQLite: the date column is stored as a string (ISO), so we parse it.
        """
        session = self.Session()
        try:
            row = (
                session.query(self.StockData.date)
                .filter(self.StockData.symbol == symbol)
                .order_by(self.StockData.date.desc())
                .first()
            )
            if not row:
                return None

            latest_val = row[0]  # Could be a datetime (Postgres) or string (SQLite).
            
            # If it's a string (SQLite), parse it to a Python datetime.
            # If it's already a datetime (PostgreSQL), pd.to_datetime will leave it as is.
            return pd.to_datetime(latest_val)
        finally:
            session.close()

    def _store_data_in_db(self, symbol, df):
        """
        Insert or update records in the DB. The date is stored either as
        a real DateTime (Postgres) or a string (SQLite).
        """
        if df.empty:
            return
        
        session = self.Session()
        try:
            for idx, row_data in df.iterrows():
                dt_val = idx.to_pydatetime()  # Python datetime
                if "sqlite" in self.db_url:
                    # For SQLite, store as string in ISO or "YYYY-MM-DD HH:MM:SS"
                    date_str = dt_val.strftime("%Y-%m-%d %H:%M:%S")
                    key_val = date_str
                else:
                    # For Postgres, store as an actual datetime object
                    key_val = dt_val

                # Check for existing record
                existing = (
                    session.query(self.StockData)
                    .filter(self.StockData.symbol == symbol, self.StockData.date == key_val)
                    .one_or_none()
                )
                if existing:
                    existing.open   = row_data["Open"]
                    existing.high   = row_data["High"]
                    existing.low    = row_data["Low"]
                    existing.close  = row_data["Close"]
                    existing.volume = row_data["Volume"]
                else:
                    # Create new record
                    record = self.StockData(
                        symbol=symbol,
                        date=key_val,
                        open=row_data["Open"],
                        high=row_data["High"],
                        low=row_data["Low"],
                        close=row_data["Close"],
                        volume=row_data["Volume"],
                    )
                    session.add(record)
            
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def update_data(self):
        """
        For each symbol:
          1. Check the last date/time in the DB for that symbol.
          2. Decide the start date for new data (default_start_date, or last_date_in_db + 1 day).
          3. If we're up to or beyond 'today', skip. Otherwise, fetch from yfinance and store.
        """
        # "today" set to midnight
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        # If it's weekend, roll back to Friday
        if today.weekday() >= 5:  # 5=Sat, 6=Sun
            offset = today.weekday() - 4
            today = today - timedelta(days=offset)

        for idx, symbol in enumerate(self.symbols):
            if idx % 20 == 0:
                print('=====================================================')
                print(f"  {idx} of {len(self.symbols)} symbols processed...")
                print('=====================================================')
            print(f"\nUpdating data for {symbol}...")
            last_dt_in_db = self._get_last_datetime_in_db(symbol)

            if last_dt_in_db is None:
                # No data: fetch from default_start_date
                start_dt = datetime.strptime(self.default_start_date, "%Y-%m-%d")
            else:
                # If last_dt_in_db < today, that day is presumably complete
                if last_dt_in_db < today:
                    start_dt = last_dt_in_db + timedelta(days=1)
                else:
                    # last_dt_in_db == today
                    start_dt = last_dt_in_db

            if start_dt >= today:
                print(f"  Up to date. Last date in DB = {last_dt_in_db}")
                continue

            start_str = start_dt.strftime("%Y-%m-%d")
            end_str   = today.strftime("%Y-%m-%d")

            print(f"  Fetching from {start_str} to {end_str}")
            df = yf.download(symbol, start=start_str, end=end_str)

            if df.empty:
                print("  No new data returned by Yahoo Finance.")
            else:
                self._store_data_in_db(symbol, df)


    def get_data(self, symbols=[], start_date=None, end_date=None):
        """
        Retrieve data from the DB. Returns a pandas DataFrame with columns:
           [symbol, date, open, high, low, close, volume].

        symbols can be:
           - None: return all symbols
           - a single string, e.g. "aapl"
           - a list of strings, e.g. ["aapl", "msft"].

        start_date, end_date can be strings in the form "YYYY-MM-DD".
        """
        session = self.Session()
        try:
            query = session.query(self.StockData)

            # If 'symbols' is a single string, wrap it in a list
            if isinstance(symbols, str):
                symbols = [symbols]

            # If 'symbols' is a list, filter using the "IN" clause
            if isinstance(symbols, list) and len(symbols) > 0:
                query = query.filter(self.StockData.symbol.in_(symbols))

            rows = query.order_by(self.StockData.symbol, self.StockData.date).all()

            data = []
            for row in rows:
                # row.date might be a datetime (Postgres) or string (SQLite).
                # In either case, convert to a Python datetime:
                date_val = pd.to_datetime(row.date)

                data.append({
                    "symbol": row.symbol,
                    "date": date_val,
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": row.volume,
                })

            df = pd.DataFrame(data)

            # Python-level date filtering
            if not df.empty:
                if start_date:
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                    df = df[df["date"] >= start_dt]
                if end_date:
                    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                    df = df[df["date"] <= end_dt]

            return df
        finally:
            session.close()


if __name__ == "__main__":
    out_dir = OutputPathSingleton.get_path()
    # For SQLite:
    # db_url = f"sqlite:///{os.path.join(out_dir, 'stocks_price.db')}"
    #
    # For Postgres:
    # db_url = "postgresql://user:password@host:5432/your_database"

    # Example: set up for SQLite
    db_url = f"sqlite:///{os.path.join(out_dir, '../stocks_price.db')}"

    symbols_list = ["aapl", "msft", "tsla"]
    manager = StockDataManager(db_url=db_url, symbols=symbols_list, default_start_date="2024-01-01")

    manager.update_data()

    df_all = manager.get_data()
    print("\nAll data in DB:")
    print(df_all.dtypes)
    print(df_all.head())
