import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
TRADING_DAYS = 252
START_DATE = "2010-01-01"
MIN_OBS_PER_YEAR = 50          # skip partial years with fewer obs
ROLLING_WINDOWS = [30, 60, 120]
DPI = 200                      # image resolution
FIG_FORMAT = "png"             # "png" or "pdf"

# File map  –  ticker : filename
FILE_MAP = {
    "AAPL": "aapl_us_d.csv",
    "AMZN": "amzn_us_d.csv",
    "AMC":  "amc_us_d.csv",
    "BYND": "bynd_us_d.csv",
    "GME":  "gme_us_d.csv",
    "MSFT": "MSFT_us_d.txt",
    "NVDA": "nvda_us_d.csv",
    "RILY": "rily_us_d.csv",
    "SPCE": "spce_us_d.csv",
    "TSLA": "tsla_us_d.csv",
}

# ──────────────────────────────────────────────
# STYLE  — clean, spacious, easy to read
# ──────────────────────────────────────────────
plt.rcParams.update({
    "figure.figsize": (12, 5.5),
    "figure.dpi": DPI,
    "axes.titlesize": 15,
    "axes.titleweight": "bold",
    "axes.titlepad": 18,
    "axes.labelsize": 13,
    "axes.labelpad": 14,
    "xtick.labelsize": 11,
    "ytick.labelsize": 11,
    "xtick.major.pad": 8,
    "ytick.major.pad": 10,
    "axes.grid": True,
    "grid.alpha": 0.30,
    "grid.linewidth": 0.6,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "font.family": "sans-serif",
    "figure.autolayout": True,
})

# Colour palette
VOL_COLOR = "#2563EB"           # blue for volatility
RETURN_COLOR = "#10B981"        # green for returns
BAR_COLOR = "#3B82F6"           # slightly lighter blue for bars
BAR_EDGE = "#1E40AF"            # dark blue edge


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────
def find_data_dir() -> Path:
    """Return the directory this script lives in (where data files should be)."""
    return Path(os.path.dirname(os.path.abspath(__file__)))


def read_price_file(filepath: Path) -> pd.DataFrame:
    """Read a CSV or tab-delimited price file and return a DataFrame."""
    # Try comma first, fall back to tab
    try:
        df = pd.read_csv(filepath, sep=",")
        if df.shape[1] < 2:
            raise ValueError("only one column")
    except Exception:
        df = pd.read_csv(filepath, sep="\t")

    df.columns = [c.strip().lower() for c in df.columns]
    return df


def guess_columns(df: pd.DataFrame):
    """Return (date_col, close_col) names."""
    cols = list(df.columns)

    # Date column
    date_col = None
    for candidate in ["date", "time", "timestamp"]:
        if candidate in cols:
            date_col = candidate
            break
    if date_col is None:
        date_col = cols[0]

    # Close column
    close_col = None
    for candidate in ["close", "adj_close", "adjclose", "adj close"]:
        if candidate in cols:
            close_col = candidate
            break
    if close_col is None:
        raise ValueError(
            f"Cannot find a 'close' column in {cols}. "
            "Rename your close-price column to 'close'."
        )
    return date_col, close_col


def load_ticker(ticker: str, filepath: Path) -> pd.DataFrame:
    """Load one ticker's price data, compute daily returns, filter post-2010."""
    df = read_price_file(filepath)
    date_col, close_col = guess_columns(df)

    df["date"] = pd.to_datetime(df[date_col])
    df["close"] = pd.to_numeric(df[close_col], errors="coerce")
    df = df.sort_values("date").reset_index(drop=True)
    df = df[df["date"] >= START_DATE].copy()

    # Simple daily returns
    df["ret"] = df["close"].pct_change()
    df = df.dropna(subset=["ret"])
    df["ticker"] = ticker

    return df[["date", "close", "ret", "ticker"]].reset_index(drop=True)


# ──────────────────────────────────────────────
# VOLATILITY & RETURN COMPUTATIONS
# ──────────────────────────────────────────────
def add_rolling_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add rolling annualized volatility (%) and rolling returns (%) columns."""
    for w in ROLLING_WINDOWS:
        # Rolling volatility (annualized, %)
        vol_col = f"roll_{w}d_vol"
        df[vol_col] = (
            df["ret"]
            .rolling(window=w, min_periods=w)
            .std()
            * np.sqrt(TRADING_DAYS)
            * 100
        )
        
        # Rolling return over window (%, cumulative return over the window)
        ret_col = f"roll_{w}d_ret"
        df[ret_col] = (
            df["close"]
            .pct_change(periods=w)
            * 100
        )
    return df


def annual_vol(df: pd.DataFrame) -> pd.DataFrame:
    """Annualized volatility per calendar year (%)."""
    df = df.copy()
    df["year"] = df["date"].dt.year
    agg = (
        df.groupby("year")["ret"]
        .agg(["std", "count"])
        .rename(columns={"std": "daily_std", "count": "n_obs"})
    )
    agg = agg[agg["n_obs"] >= MIN_OBS_PER_YEAR]
    agg["annual_vol_pct"] = agg["daily_std"] * np.sqrt(TRADING_DAYS) * 100
    return agg.reset_index()


def daily_vol_by_year(df: pd.DataFrame) -> pd.DataFrame:
    """Average daily volatility (std dev of daily returns, NOT annualized) per year (%)."""
    df = df.copy()
    df["year"] = df["date"].dt.year
    agg = (
        df.groupby("year")["ret"]
        .agg(["std", "count"])
        .rename(columns={"std": "daily_std", "count": "n_obs"})
    )
    agg = agg[agg["n_obs"] >= MIN_OBS_PER_YEAR]
    agg["daily_vol_pct"] = agg["daily_std"] * 100
    return agg.reset_index()


# ──────────────────────────────────────────────
# PLOTTING FUNCTIONS
# ──────────────────────────────────────────────
def _pct_formatter(x, _):
    """Format tick as e.g. '25%'."""
    return f"{x:.0f}%"


def _pct_formatter_1dec(x, _):
    """Format tick as e.g. '2.5%'."""
    return f"{x:.1f}%"


def _save(fig, folder: Path, filename: str):
    fig.savefig(folder / f"{filename}.{FIG_FORMAT}", dpi=DPI, bbox_inches="tight")
    plt.close(fig)


def plot_cumulative_returns(df: pd.DataFrame, ticker: str, folder: Path):
    """Line chart of cumulative returns since start of sample."""
    cum = (1 + df["ret"]).cumprod() - 1  # fractional cumulative return
    cum_pct = cum * 100

    fig, ax = plt.subplots()
    ax.plot(df["date"], cum_pct, linewidth=0.9, color=RETURN_COLOR)
    ax.set_title(f"{ticker} — Cumulative Returns (Post {START_DATE[:4]})")
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative Return (%)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(_pct_formatter))
    ax.axhline(0, color="grey", linewidth=0.5, linestyle="--")
    _save(fig, folder, f"{ticker}_cumulative_returns")


def plot_rolling_vol_with_return(df: pd.DataFrame, ticker: str, window: int, folder: Path):
    """Dual-axis line chart: rolling volatility (left) and rolling return (right)."""
    vol_col = f"roll_{window}d_vol"
    ret_col = f"roll_{window}d_ret"
    
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Left axis: Rolling Volatility
    ax1.set_xlabel("Date")
    ax1.set_ylabel(f"{window}-Day Rolling Volatility (%)", color=VOL_COLOR)
    line1 = ax1.plot(df["date"], df[vol_col], linewidth=0.8, color=VOL_COLOR, label=f"{window}d Vol")
    ax1.tick_params(axis="y", labelcolor=VOL_COLOR)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(_pct_formatter))
    ax1.yaxis.set_major_locator(mticker.MaxNLocator(nbins=7))
    
    # Set vol y-axis to start at 0
    vol_max = df[vol_col].max()
    if not np.isnan(vol_max):
        ax1.set_ylim(bottom=0, top=vol_max * 1.15)
    
    # Right axis: Rolling Return
    ax2 = ax1.twinx()
    ax2.spines["right"].set_visible(True)  # show right spine for dual axis
    ax2.set_ylabel(f"{window}-Day Rolling Return (%)", color=RETURN_COLOR)
    line2 = ax2.plot(df["date"], df[ret_col], linewidth=0.8, color=RETURN_COLOR, alpha=0.85, label=f"{window}d Return")
    ax2.tick_params(axis="y", labelcolor=RETURN_COLOR)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(_pct_formatter))
    ax2.yaxis.set_major_locator(mticker.MaxNLocator(nbins=7))
    ax2.axhline(0, color="grey", linewidth=0.5, linestyle="--", alpha=0.5)
    
    # Title and legend
    ax1.set_title(f"{ticker} — {window}-Day Rolling Volatility & Return")
    
    # Combined legend
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left", framealpha=0.9)
    
    # Grid only on primary axis
    ax1.grid(True, alpha=0.3)
    ax2.grid(False)
    
    fig.tight_layout()
    _save(fig, folder, f"{ticker}_rolling_{window}d_vol_return")


def plot_annual_vol_bars(av: pd.DataFrame, ticker: str, folder: Path):
    """Bar chart of annualized volatility per calendar year."""
    fig, ax = plt.subplots()
    years = av["year"].astype(int)
    ax.bar(
        years, av["annual_vol_pct"],
        width=0.7, color=BAR_COLOR, edgecolor=BAR_EDGE, linewidth=0.6,
    )
    ax.set_title(f"{ticker} — Annualized Volatility by Year")
    ax.set_xlabel("Year")
    ax.set_ylabel("Annualized Volatility (%)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(_pct_formatter))
    ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=7))

    # Show every year or every-other if crowded
    if len(years) > 10:
        ax.xaxis.set_major_locator(mticker.MultipleLocator(2))
    else:
        ax.xaxis.set_major_locator(mticker.MultipleLocator(1))
    ax.set_xlim(years.min() - 0.6, years.max() + 0.6)

    # Add value labels on top of bars
    for y, v in zip(years, av["annual_vol_pct"]):
        ax.text(y, v + 0.5, f"{v:.1f}%", ha="center", va="bottom", fontsize=8.5)

    _save(fig, folder, f"{ticker}_annual_vol")


def plot_daily_vol_bars(dv: pd.DataFrame, ticker: str, folder: Path):
    """Bar chart of average daily volatility per calendar year (not annualized)."""
    fig, ax = plt.subplots()
    years = dv["year"].astype(int)
    ax.bar(
        years, dv["daily_vol_pct"],
        width=0.7, color=BAR_COLOR, edgecolor=BAR_EDGE, linewidth=0.6,
    )
    ax.set_title(f"{ticker} — Average Daily Volatility by Year (Not Annualized)")
    ax.set_xlabel("Year")
    ax.set_ylabel("Daily Volatility (%)")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(_pct_formatter_1dec))
    ax.yaxis.set_major_locator(mticker.MaxNLocator(nbins=7))

    if len(years) > 10:
        ax.xaxis.set_major_locator(mticker.MultipleLocator(2))
    else:
        ax.xaxis.set_major_locator(mticker.MultipleLocator(1))
    ax.set_xlim(years.min() - 0.6, years.max() + 0.6)

    for y, v in zip(years, dv["daily_vol_pct"]):
        ax.text(y, v + 0.02, f"{v:.2f}%", ha="center", va="bottom", fontsize=8.5)

    _save(fig, folder, f"{ticker}_daily_vol_by_year")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
def main():
    data_dir = find_data_dir()
    output_root = data_dir / "output"
    output_root.mkdir(exist_ok=True)

    print(f"Data directory : {data_dir}")
    print(f"Output directory: {output_root}\n")

    # Check all files exist
    missing = []
    for ticker, fname in FILE_MAP.items():
        fpath = data_dir / fname
        if not fpath.exists():
            missing.append(f"  {ticker}: {fpath}")
    if missing:
        print("ERROR — Missing data files:")
        print("\n".join(missing))
        print("\nPut all data files in the same folder as this script.")
        sys.exit(1)

    # Process each ticker
    for ticker in sorted(FILE_MAP.keys()):
        fname = FILE_MAP[ticker]
        fpath = data_dir / fname

        print(f"Processing {ticker} ...", end=" ", flush=True)

        # Load & compute
        df = load_ticker(ticker, fpath)
        df = add_rolling_metrics(df)
        av = annual_vol(df)
        dv = daily_vol_by_year(df)

        # Create ticker subfolder
        ticker_dir = output_root / ticker
        ticker_dir.mkdir(exist_ok=True)

        # Generate all charts
        plot_cumulative_returns(df, ticker, ticker_dir)

        for w in ROLLING_WINDOWS:
            plot_rolling_vol_with_return(df, ticker, w, ticker_dir)

        plot_annual_vol_bars(av, ticker, ticker_dir)
        plot_daily_vol_bars(dv, ticker, ticker_dir)

        n_charts = 2 + len(ROLLING_WINDOWS)  # returns + rolling + 2 bar charts
        print(f"✓  ({len(df)} obs, {n_charts + 1} charts saved to output/{ticker}/)")

    print(f"\nDone. All charts saved under: {output_root}")


if __name__ == "__main__":
    main()
