# visualize_porto.py
# Enkle EDA-visualiseringer for Porto Taxi-datasettet
# Forutsetter porto_loader.py i samme mappe og en porto.csv

import os
import math
import ast
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt

from data_loader import load_porto_data


def ensure_matplotlib_backend():
    """
    Unngå problemer på headless-miljøer ved å bruke en ikke-interaktiv backend.
    (Har ingen negativ effekt på vanlig kjøring i terminal/VS Code.)
    """
    import matplotlib
    if os.environ.get("MPLBACKEND") is None:
        matplotlib.use("Agg")


def parse_polyline_len(s):
    """
    Returnerer antall punkter i POLYLINE (liste med [lon, lat]-par) uten å kreve geopandas.
    """
    if pd.isna(s) or s == "" or s == "[]":
        return 0
    try:
        pts = ast.literal_eval(s)
        return len(pts)
    except Exception:
        return 0


def main():
    ensure_matplotlib_backend()

    # 1) Last data
    df = load_porto_data(csv_path="./porto.csv", pickle_path="./porto_data.pkl", verbose=True)

    # 2) Lag noen nyttige felt (robust ifht. hvilke kolonner som finnes)
    # a) Starttime basert på TIMESTAMP (loaderen din konverterer allerede til datetime hvis kolonnen finnes)
    if "TIMESTAMP" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["TIMESTAMP"]):
        df["TIMESTAMP"] = pd.to_datetime(df["TIMESTAMP"], unit="s", errors="coerce")

    if "TIMESTAMP" in df.columns:
        df["hour"] = df["TIMESTAMP"].dt.hour
        df["date"] = df["TIMESTAMP"].dt.date

    # b) Trip-lengde i punkter (15 sek mellom punkter i dette datasettet). Gir omtrentlig varighet.
    if "POLYLINE" in df.columns:
        df["poly_len"] = df["POLYLINE"].apply(parse_polyline_len)
        # Varighet (sekunder) = (n_punkter - 1) * 15, men ikke negativ
        df["duration_sec"] = (df["poly_len"].clip(lower=1) - 1) * 15

    # 3) Lag figurmappe
    outdir = "./figures"
    os.makedirs(outdir, exist_ok=True)

    # 4) Plot 1: Antall turer per time på døgnet
    if "hour" in df.columns:
        counts_by_hour = df["hour"].value_counts().sort_index()
        plt.figure()
        counts_by_hour.plot(kind="bar")
        plt.title("Antall turer per time på døgnet")
        plt.xlabel("Time (0–23)")
        plt.ylabel("Antall turer")
        plt.tight_layout()
        plt.savefig(os.path.join(outdir, "turer_per_time.png"))
        plt.close()

    # 5) Plot 2: Fordeling av CALL_TYPE (om finnes)
    if "CALL_TYPE" in df.columns:
        call_counts = df["CALL_TYPE"].value_counts()
        plt.figure()
        call_counts.plot(kind="bar")
        plt.title("Fordeling av CALL_TYPE")
        plt.xlabel("CALL_TYPE")
        plt.ylabel("Antall")
        plt.tight_layout()
        plt.savefig(os.path.join(outdir, "fordeling_call_type.png"))
        plt.close()

    # 6) Plot 3: Manglende data (MISSING_DATA) andel
    if "MISSING_DATA" in df.columns:
        miss_counts = df["MISSING_DATA"].value_counts(dropna=False)
        plt.figure()
        miss_counts.plot(kind="bar")
        plt.title("MISSING_DATA fordeling")
        plt.xlabel("MISSING_DATA (False/True)")
        plt.ylabel("Antall rader")
        plt.tight_layout()
        plt.savefig(os.path.join(outdir, "missing_data_fordeling.png"))
        plt.close()

    # 7) Plot 4: Daglige turer (om TIMESTAMP finnes)
    if "date" in df.columns:
        per_day = df.groupby("date").size()
        per_day = per_day.sort_index()
        plt.figure()
        per_day.plot(kind="line")
        plt.title("Antall turer per dag")
        plt.xlabel("Dato")
        plt.ylabel("Antall turer")
        plt.tight_layout()
        plt.savefig(os.path.join(outdir, "turer_per_dag.png"))
        plt.close()

    # 8) Plot 5: Varighet-distribusjon (hvis POLYLINE finnes)
    if "duration_sec" in df.columns:
        # Filtrer ut ekstreme verdier for en ryddigere histogram (95-persentil)
        cutoff = df["duration_sec"].quantile(0.95)
        plt.figure()
        df.loc[df["duration_sec"] <= cutoff, "duration_sec"].hist(bins=40)
        plt.title("Distribusjon av turvarighet (sek, toppet ved 95-persentil)")
        plt.xlabel("Varighet (sekunder)")
        plt.ylabel("Antall turer")
        plt.tight_layout()
        plt.savefig(os.path.join(outdir, "varighet_hist.png"))
        plt.close()

    # 9) Skriv en liten oppsummering til terminal
    print("=== VISUALISERINGER LAGRET ===")
    print(f"- Output-mappe: {outdir}")
    for f in sorted(os.listdir(outdir)):
        if f.endswith(".png"):
            print(f"  • {f}")

    # 10) (valgfritt) enkel tabellutskrift
    basic = {
        "rader": len(df),
        "kolonner": len(df.columns),
        "manglende_data_kolonne": "MISSING_DATA" in df.columns,
        "polyline_kolonne": "POLYLINE" in df.columns,
        "timestamp_kolonne": "TIMESTAMP" in df.columns,
    }
    print("=== GRUNNINFO ===")
    for k, v in basic.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
