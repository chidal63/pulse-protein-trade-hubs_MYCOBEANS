#!/usr/bin/env python3
"""
COMPARE — FAOSTAT primary system vs UN Comtrade HS210610 trade hubs (2020–2024)
Graphics-enhanced version (logic stable; plotting improved)

Key changes vs prior version (analysis unchanged)
- Scatter:
  * highlight Top10 FAO (by mean share) and Top10 Trade (by mean share)
  * show 4-category legend:
      - Top10 in both
      - Top10 FAO only
      - Top10 Trade only
      - Others
  * draw y=x reference, median lines
  * annotate only (Top10 union) by default to reduce clutter
- HHI heatmap:
  * smaller annotation font
  * cleaner label fonts and compact layout

Run
python compare_production_vs_trade_hubs.py \
  "/Users/chiaradallasta/Downloads/TradeData-3.xlsx" \
  "/Users/chiaradallasta/Downloads/FAOSTAT_data_en_2-1-2026.xls" \
  --outdir "/Users/chiaradallasta/Downloads" \
  --years 2020-2024 \
  --hs 210610 \
  --top10 10 \
  --make-yearly \
  --open
"""

from __future__ import annotations

from pathlib import Path
import argparse
import os
import platform
import re
import subprocess
from typing import Optional, List, Dict, Set

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# -----------------------------
# Helpers
# -----------------------------
def open_file_any(path: Path) -> None:
    path = path.expanduser().resolve()
    if not path.exists():
        return
    system = platform.system().lower()
    try:
        if system == "darwin":
            subprocess.run(["open", str(path)], check=False)
            return
        if system == "linux":
            subprocess.run(["xdg-open", str(path)], check=False)
            return
        if system == "windows":
            os.startfile(str(path))  # type: ignore[attr-defined]
            return
    except Exception:
        pass


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = list(df.columns)
    for c in candidates:
        if c in cols:
            return c
    low = {c.lower(): c for c in cols}
    for c in candidates:
        if c.lower() in low:
            return low[c.lower()]
    return None


def normalize_country(name: str) -> str:
    if name is None:
        return ""
    s = str(name).strip()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("&", "and").replace("\u00a0", " ")
    s = s.strip(" .")
    return s


# -----------------------------
# Country name harmonization
# -----------------------------
NAME_MAP: Dict[str, str] = {
    # US
    "United States of America": "United States",
    "USA": "United States",
    # UK
    "United Kingdom of Great Britain and Northern Ireland": "United Kingdom",
    "UK": "United Kingdom",
    # Russia
    "Russian Federation": "Russia",
    # Iran
    "Iran (Islamic Republic of)": "Iran",
    # China / SARs (keep SARs separate)
    "China, mainland": "China",
    "China (mainland)": "China",
    "China, Hong Kong SAR": "Hong Kong",
    "China, Macao SAR": "Macao",
    # Czechia
    "Czech Republic": "Czechia",
    # Türkiye
    "Turkey": "Türkiye",
    # Netherlands
    "Netherlands (Kingdom of the)": "Netherlands",
    # Viet Nam
    "Viet Nam": "Vietnam",
    # Variants
    "Venezuela (Bolivarian Republic of)": "Venezuela",
    "Bolivia (Plurinational State of)": "Bolivia",
    "United Republic of Tanzania": "Tanzania",
    "Syrian Arab Republic": "Syria",
    "Lao People's Democratic Republic": "Laos",
    "Micronesia (Federated States of)": "Micronesia",
    "Côte d'Ivoire": "Cote d'Ivoire",
}

# Fixes from mismatch debug (FAOSTAT vs Comtrade)
NAME_MAP.update({
    "Bosnia Herzegovina": "Bosnia and Herzegovina",
    "Central African Rep": "Central African Republic",
    "Dem. Rep. of the Congo": "Democratic Republic of the Congo",
    "Dominican Rep": "Dominican Republic",
    "Lao People's Dem. Rep": "Laos",
    "Rep. of Moldova": "Republic of Moldova",
    "United Rep. of Tanzania": "Tanzania",
    "State of Palestine": "Palestine",
    "Rep. of Korea": "Korea, Rep.",
    "Republic of Korea": "Korea, Rep.",
    "Democratic People's Republic of Korea": "Korea, Dem. People's Rep.",
    "Cura√ßao": "Curaçao",
    "UAE": "United Arab Emirates",
})


def harmonize_country(name: str) -> str:
    s = normalize_country(name)
    return NAME_MAP.get(s, s)


def hhi_from_share_pct(s: pd.Series) -> float:
    frac = (s.fillna(0) / 100.0)
    return float((frac ** 2).sum())


def topk_set(df: pd.DataFrame, share_col: str, k: int) -> Set[str]:
    return set(df.sort_values(share_col, ascending=False).head(k)["Country"])


def nice_axis_limits(x: np.ndarray, y: np.ndarray) -> List[float]:
    # Small padding around min/max for aesthetics.
    xmin, xmax = float(np.min(x)), float(np.max(x))
    ymin, ymax = float(np.min(y)), float(np.max(y))
    lo = min(xmin, ymin)
    hi = max(xmax, ymax)
    pad = 0.03 * (hi - lo) if hi > lo else 1.0
    return [max(0.0, lo - pad), hi + pad]


# -----------------------------
# CLI
# -----------------------------
parser = argparse.ArgumentParser(
    description="Compare FAOSTAT pulses vs UN Comtrade HS210610 using global shares."
)
parser.add_argument("comtrade_xlsx", type=Path, help="UN Comtrade Excel export (TradeData-*.xlsx)")
parser.add_argument("faostat_xls", type=Path, help="FAOSTAT export .xls")
parser.add_argument("--hs", type=int, default=210610, help="HS code (default: 210610)")
parser.add_argument("--years", type=str, default="2020-2024", help="Year range, e.g. 2020-2024")
parser.add_argument("--outdir", type=Path, default=Path.cwd(), help="Output directory")
parser.add_argument("--top10", type=int, default=10, help="Top-K for concentration + overlap and highlighting (default 10)")
parser.add_argument("--make-yearly", action="store_true", help="Also save one scatter per year")
parser.add_argument("--open", action="store_true", help="Attempt to open main PNG after saving")
args = parser.parse_args()

HS_CODE = args.hs
year_start, year_end = (int(x) for x in args.years.split("-", 1))
OUTDIR = args.outdir.expanduser().resolve()
OUTDIR.mkdir(parents=True, exist_ok=True)

COMTRADE_PATH = args.comtrade_xlsx.expanduser().resolve()
FAOSTAT_PATH = args.faostat_xls.expanduser().resolve()
if not COMTRADE_PATH.exists():
    raise FileNotFoundError(f"Comtrade file not found: {COMTRADE_PATH}")
if not FAOSTAT_PATH.exists():
    raise FileNotFoundError(f"FAOSTAT file not found: {FAOSTAT_PATH}")

# Outputs
OUT_MAIN_PNG = OUTDIR / f"Fig_compare_FAOSTAT_vs_Comtrade_HS{HS_CODE}_{year_start}_{year_end}.png"
OUT_HHI_PNG = OUTDIR / f"Fig_HHI_heatmap_FAOSTAT_vs_Comtrade_{year_start}_{year_end}.png"
OUT_MERGED_CSV = OUTDIR / f"Data_merged_FAOSTAT_vs_Comtrade_HS{HS_CODE}_{year_start}_{year_end}.csv"
OUT_METRICS_CSV = OUTDIR / f"Metrics_concentration_overlap_{year_start}_{year_end}.csv"
OUT_DEBUG_COUNTRY_CSV = OUTDIR / f"Debug_country_mismatches_{year_start}_{year_end}.csv"


# -----------------------------
# Load UN Comtrade
# -----------------------------
dfc = pd.read_excel(COMTRADE_PATH)

c_year = pick_col(dfc, ["period", "Year", "year"])
c_cmd = pick_col(dfc, ["cmdCode", "cmdcode", "CommodityCode", "hs", "hs6"])
c_partner = pick_col(dfc, ["partnerDesc", "partner", "Partner"])
c_reporter = pick_col(dfc, ["reporterDesc", "reporter", "Reporter"])
c_flow = pick_col(dfc, ["flowCode", "flow", "flowDesc"])
c_value = pick_col(dfc, ["primaryValue", "tradeValue", "TradeValue", "Value", "VALUE"])

missing = [k for k, v in {
    "year": c_year, "cmdCode": c_cmd, "partner": c_partner, "reporter": c_reporter, "value": c_value
}.items() if v is None]
if missing:
    raise KeyError(f"Comtrade required columns missing: {missing}. Available: {list(dfc.columns)[:60]}")

dfc[c_year] = to_numeric(dfc[c_year])
dfc[c_cmd] = to_numeric(dfc[c_cmd])
dfc[c_value] = to_numeric(dfc[c_value])

dfc_f = dfc[
    (dfc[c_cmd] == HS_CODE) &
    (dfc[c_year] >= year_start) &
    (dfc[c_year] <= year_end)
].copy()

# Partner=World
dfc_f = dfc_f[dfc_f[c_partner].astype(str).str.strip().str.lower().eq("world")].copy()
if dfc_f.empty:
    raise ValueError(
        f"Comtrade filter returned 0 rows. Check HS={HS_CODE}, years {year_start}-{year_end}, Partner=World."
    )

# Optional: M/X filter
if c_flow is not None:
    dfc_f[c_flow] = dfc_f[c_flow].astype(str).str.strip()
    if dfc_f[c_flow].isin(["M", "X"]).any():
        dfc_f = dfc_f[dfc_f[c_flow].isin(["M", "X"])].copy()

trade = (
    dfc_f.groupby([c_year, c_reporter], as_index=False)[c_value]
    .sum()
    .rename(columns={c_year: "Year", c_reporter: "CountryRaw", c_value: "TradeValueUSD"})
)
trade["Country"] = trade["CountryRaw"].map(harmonize_country)

trade_tot = trade.groupby("Year", as_index=False)["TradeValueUSD"].sum().rename(columns={"TradeValueUSD": "TradeGlobalUSD"})
trade = trade.merge(trade_tot, on="Year", how="left")
trade["TradeSharePct"] = 100.0 * trade["TradeValueUSD"] / trade["TradeGlobalUSD"]


# -----------------------------
# Load FAOSTAT (prefer production tonnes; fallback to yield)
# -----------------------------
dff = pd.read_excel(FAOSTAT_PATH)

f_area = pick_col(dff, ["Area", "area", "Area Name", "Country", "country"])
f_year = pick_col(dff, ["Year", "year"])
f_item = pick_col(dff, ["Item", "item", "Item Name"])
f_element = pick_col(dff, ["Element", "element"])
f_unit = pick_col(dff, ["Unit", "unit"])
f_value = pick_col(dff, ["Value", "value"])

missing_f = [k for k, v in {"Area": f_area, "Year": f_year, "Value": f_value}.items() if v is None]
if missing_f:
    raise KeyError(f"FAOSTAT required columns missing: {missing_f}. Available: {list(dff.columns)[:60]}")

dff[f_year] = to_numeric(dff[f_year])
dff[f_value] = to_numeric(dff[f_value])

dff_f = dff[(dff[f_year] >= year_start) & (dff[f_year] <= year_end)].copy()
if dff_f.empty:
    raise ValueError(f"FAOSTAT has no rows in years {year_start}-{year_end}.")

chosen_element_label = "FAOSTAT metric (unknown)"
warn_fallback_yield = False

if f_element is not None:
    el = dff_f[f_element].astype(str).str.strip().str.lower()
    prod_mask = el.str.contains("production")
    yield_mask = el.str.contains("yield")

    if f_unit is not None:
        un = dff_f[f_unit].astype(str).str.strip().str.lower()
        tonnes_mask = (
            un.str.fullmatch("t") |
            un.str.contains("tonne") |
            un.str.contains(r"\btons?\b")
        )
    else:
        tonnes_mask = pd.Series([True] * len(dff_f), index=dff_f.index)

    prod_tonnes = prod_mask & tonnes_mask
    if prod_tonnes.any():
        dff_f = dff_f[prod_tonnes].copy()
        chosen_element_label = "FAOSTAT Production (tonnes)"
    elif yield_mask.any():
        dff_f = dff_f[yield_mask].copy()
        chosen_element_label = "FAOSTAT Yield (kg/ha) — fallback"
        warn_fallback_yield = True
    else:
        chosen_element_label = "FAOSTAT (no Element filter)"

# Prefer Pulses, Total if present
if f_item is not None:
    item_s = dff_f[f_item].astype(str).str.strip().str.lower()
    pulses_total = item_s.str.fullmatch(r"pulses,\s*total")
    if pulses_total.any():
        dff_f = dff_f[pulses_total].copy()

if dff_f.empty:
    raise ValueError(
        f"FAOSTAT filter returned 0 rows for years {year_start}-{year_end}. "
        f"Your file may not contain Production (tonnes) nor Yield."
    )

print(f"\nFAOSTAT selection: {chosen_element_label}")
if warn_fallback_yield:
    print("WARNING: FAOSTAT file appears to contain Yield rather than Production. "
          "Interpret scatter as yield-share vs trade-share (not volume production).")

fao = (
    dff_f.groupby([f_year, f_area], as_index=False)[f_value]
    .sum()
    .rename(columns={f_year: "Year", f_area: "CountryRaw", f_value: "FAO_Value"})
)
fao["Country"] = fao["CountryRaw"].map(harmonize_country)

fao_tot = fao.groupby("Year", as_index=False)["FAO_Value"].sum().rename(columns={"FAO_Value": "FAO_Global"})
fao = fao.merge(fao_tot, on="Year", how="left")
fao["FAOSharePct"] = 100.0 * fao["FAO_Value"] / fao["FAO_Global"]


# -----------------------------
# Merge country-year
# -----------------------------
merged = fao.merge(
    trade,
    on=["Year", "Country"],
    how="outer",
)
for col in ["FAOSharePct", "TradeSharePct", "FAO_Value", "TradeValueUSD"]:
    merged[col] = merged[col].fillna(0)

merged.to_csv(OUT_MERGED_CSV, index=False)

# Debug mismatches (for transparency)
countries_fao = set(fao["Country"].unique())
countries_trade = set(trade["Country"].unique())
only_fao = sorted(countries_fao - countries_trade)
only_trade = sorted(countries_trade - countries_fao)
dbg = pd.DataFrame({
    "OnlyInFAOSTAT": pd.Series(only_fao, dtype="object"),
    "OnlyInComtrade": pd.Series(only_trade, dtype="object"),
})
dbg.to_csv(OUT_DEBUG_COUNTRY_CSV, index=False)


# -----------------------------
# Metrics: concentration + overlap by year
# -----------------------------
metrics_rows = []
for y in range(year_start, year_end + 1):
    f_y = fao[fao["Year"] == y].copy()
    t_y = trade[trade["Year"] == y].copy()

    if f_y.empty or t_y.empty:
        metrics_rows.append({
            "Year": y,
            "FAOTopKSharePct": np.nan,
            "FAOHHI_0_1": np.nan,
            "FAOHHI_0_10000": np.nan,
            "TradeTopKSharePct": np.nan,
            "TradeHHI_0_1": np.nan,
            "TradeHHI_0_10000": np.nan,
            "TopK_Jaccard": np.nan,
            "Note": "Missing year in FAOSTAT or Comtrade",
        })
        continue

    f_sorted = f_y.sort_values("FAOSharePct", ascending=False)
    t_sorted = t_y.sort_values("TradeSharePct", ascending=False)

    fao_topk = float(f_sorted["FAOSharePct"].head(args.top10).sum())
    trade_topk = float(t_sorted["TradeSharePct"].head(args.top10).sum())

    fao_hhi = hhi_from_share_pct(f_sorted["FAOSharePct"])
    trade_hhi = hhi_from_share_pct(t_sorted["TradeSharePct"])

    set_f = topk_set(f_sorted.rename(columns={"FAOSharePct": "Share"}), "Share", args.top10)
    set_t = topk_set(t_sorted.rename(columns={"TradeSharePct": "Share"}), "Share", args.top10)
    jacc = (len(set_f & set_t) / len(set_f | set_t)) if (set_f | set_t) else np.nan

    metrics_rows.append({
        "Year": y,
        "FAOTopKSharePct": fao_topk,
        "FAOHHI_0_1": fao_hhi,
        "FAOHHI_0_10000": fao_hhi * 10000.0,
        "TradeTopKSharePct": trade_topk,
        "TradeHHI_0_1": trade_hhi,
        "TradeHHI_0_10000": trade_hhi * 10000.0,
        "TopK_Jaccard": jacc,
        "Note": "",
    })

metrics = pd.DataFrame(metrics_rows)
metrics.to_csv(OUT_METRICS_CSV, index=False)


# -----------------------------
# Main figure: mean shares over period (highlight Top10 groups)
# -----------------------------
mean_by_country = (
    merged.groupby("Country", as_index=False)
    .agg(
        FAOSharePct_mean=("FAOSharePct", "mean"),
        TradeSharePct_mean=("TradeSharePct", "mean"),
        FAO_Value_sum=("FAO_Value", "sum"),
        TradeValueUSD_sum=("TradeValueUSD", "sum"),
    )
)
mean_by_country = mean_by_country[
    (mean_by_country["FAOSharePct_mean"] > 0) | (mean_by_country["TradeSharePct_mean"] > 0)
].copy()

# Define TopK groups based on mean shares (stable over period)
topk_fao = set(mean_by_country.sort_values("FAOSharePct_mean", ascending=False).head(args.top10)["Country"])
topk_trade = set(mean_by_country.sort_values("TradeSharePct_mean", ascending=False).head(args.top10)["Country"])
topk_both = topk_fao & topk_trade
topk_fao_only = topk_fao - topk_trade
topk_trade_only = topk_trade - topk_fao
others = set(mean_by_country["Country"]) - (topk_fao | topk_trade)

# Create boolean masks for plotting
mean_by_country["Group"] = "Other"
mean_by_country.loc[mean_by_country["Country"].isin(topk_fao_only), "Group"] = f"Top{args.top10} FAO only"
mean_by_country.loc[mean_by_country["Country"].isin(topk_trade_only), "Group"] = f"Top{args.top10} Trade only"
mean_by_country.loc[mean_by_country["Country"].isin(topk_both), "Group"] = f"Top{args.top10} in both"

x = mean_by_country["FAOSharePct_mean"].values
y = mean_by_country["TradeSharePct_mean"].values

plt.figure(figsize=(9.5, 6.5))

# Plot order: Others first, then highlighted groups on top
plot_order = [
    "Other",
    f"Top{args.top10} FAO only",
    f"Top{args.top10} Trade only",
    f"Top{args.top10} in both",
]

markers = {
    "Other": "o",
    f"Top{args.top10} FAO only": "s",
    f"Top{args.top10} Trade only": "^",
    f"Top{args.top10} in both": "D",
}

# Use default colors by cycling; we avoid manually setting colors.
for grp in plot_order:
    d = mean_by_country[mean_by_country["Group"] == grp]
    if d.empty:
        continue
    plt.scatter(
        d["FAOSharePct_mean"].values,
        d["TradeSharePct_mean"].values,
        marker=markers.get(grp, "o"),
        label=grp,
        alpha=0.85 if grp != "Other" else 0.45,
        edgecolors="none",
    )

# Reference lines: medians
x_med = float(np.median(mean_by_country["FAOSharePct_mean"].values))
y_med = float(np.median(mean_by_country["TradeSharePct_mean"].values))
plt.axvline(x_med, linestyle="--", linewidth=1)
plt.axhline(y_med, linestyle="--", linewidth=1)

# y=x reference
lims = nice_axis_limits(mean_by_country["FAOSharePct_mean"].values, mean_by_country["TradeSharePct_mean"].values)
plt.plot(lims, lims, linestyle=":", linewidth=1)
plt.xlim(lims)
plt.ylim(lims)

# Annotate: TopK union only (keeps it readable)
label_set = topk_fao | topk_trade
for _, row in mean_by_country.iterrows():
    c = row["Country"]
    if c in label_set:
        plt.text(row["FAOSharePct_mean"], row["TradeSharePct_mean"], c, fontsize=8)

plt.xlabel(f"{chosen_element_label}: share of global (%) — mean {year_start}–{year_end}")
plt.ylabel(f"UN Comtrade HS {HS_CODE}: share of global trade value (%) — mean {year_start}–{year_end}")
plt.title(
    f"Primary system vs derived-ingredient trade hubs\n"
    f"{chosen_element_label} vs UN Comtrade HS {HS_CODE}, {year_start}–{year_end} (global shares)"
)
plt.legend(frameon=False, loc="upper right")
plt.tight_layout()
plt.savefig(OUT_MAIN_PNG, dpi=300, bbox_inches="tight")
plt.close()


# -----------------------------
# Extra figure: HHI heatmap over time (smaller fonts)
# -----------------------------
hhi_tbl = metrics[["Year", "FAOHHI_0_10000", "TradeHHI_0_10000"]].copy().dropna()
years = hhi_tbl["Year"].astype(int).tolist()
mat = np.array(
    [hhi_tbl["FAOHHI_0_10000"].values, hhi_tbl["TradeHHI_0_10000"].values],
    dtype=float
)

plt.figure(figsize=(1.15 * max(5, len(years)), 2.4))
im = plt.imshow(mat, aspect="auto")

plt.yticks(
    [0, 1],
    [f"FAOSTAT HHI\n({chosen_element_label})", f"Comtrade HHI\n(HS {HS_CODE})"],
    fontsize=9,
)
plt.xticks(range(len(years)), years, fontsize=9)
plt.title(f"Concentration over time (HHI, 0–10,000), {year_start}–{year_end}", fontsize=10)

cbar = plt.colorbar(im)
cbar.set_label("HHI (0–10,000)", fontsize=9)
cbar.ax.tick_params(labelsize=8)

# Smaller annotation font (requested)
for i in range(mat.shape[0]):
    for j in range(mat.shape[1]):
        plt.text(j, i, f"{mat[i, j]:.0f}", ha="center", va="center", fontsize=7)

plt.tight_layout()
plt.savefig(OUT_HHI_PNG, dpi=300, bbox_inches="tight")
plt.close()


# -----------------------------
# Optional: yearly scatter plots (keep simple, highlight yearly TopK union)
# -----------------------------
if args.make_yearly:
    for y0 in range(year_start, year_end + 1):
        my = merged[merged["Year"] == y0].copy()
        my = my[(my["FAOSharePct"] > 0) | (my["TradeSharePct"] > 0)].copy()
        if my.empty:
            continue

        top_f = set(my.sort_values("FAOSharePct", ascending=False).head(args.top10)["Country"])
        top_t = set(my.sort_values("TradeSharePct", ascending=False).head(args.top10)["Country"])
        lab = top_f | top_t

        plt.figure(figsize=(9.5, 6.5))
        plt.scatter(my["FAOSharePct"].values, my["TradeSharePct"].values, alpha=0.45)

        # Highlight topK union for that year
        d_hi = my[my["Country"].isin(lab)]
        plt.scatter(d_hi["FAOSharePct"].values, d_hi["TradeSharePct"].values, alpha=0.9)

        plt.axvline(float(np.median(my["FAOSharePct"].values)), linestyle="--", linewidth=1)
        plt.axhline(float(np.median(my["TradeSharePct"].values)), linestyle="--", linewidth=1)

        lims = nice_axis_limits(my["FAOSharePct"].values, my["TradeSharePct"].values)
        plt.plot(lims, lims, linestyle=":", linewidth=1)
        plt.xlim(lims)
        plt.ylim(lims)

        for _, r in d_hi.iterrows():
            plt.text(r["FAOSharePct"], r["TradeSharePct"], r["Country"], fontsize=8)

        plt.xlabel(f"{chosen_element_label}: share of global (%)")
        plt.ylabel(f"UN Comtrade HS {HS_CODE}: share of global trade value (%)")
        plt.title(f"Year {y0}: FAOSTAT vs Comtrade hubs (global shares)\nHighlighted: Top{args.top10} union")
        plt.tight_layout()
        out_y = OUTDIR / f"Fig_scatter_{y0}_FAOSTAT_vs_Comtrade_HS{HS_CODE}.png"
        plt.savefig(out_y, dpi=300, bbox_inches="tight")
        plt.close()


# -----------------------------
# Terminal support block
# -----------------------------
print("\n" + "=" * 100)
print(f"COMPARE SUPPORT — {chosen_element_label} vs UN Comtrade HS {HS_CODE} (Partner=World), {year_start}–{year_end}")
print("=" * 100)

print("\nSaved outputs:")
print(f"  Main scatter:     {OUT_MAIN_PNG}")
print(f"  HHI heatmap:      {OUT_HHI_PNG}")
print(f"  Merged dataset:   {OUT_MERGED_CSV}")
print(f"  Metrics:          {OUT_METRICS_CSV}")
print(f"  Country debug:    {OUT_DEBUG_COUNTRY_CSV}")
if args.make_yearly:
    print("  Yearly figures:   Fig_scatter_<YEAR>_FAOSTAT_vs_Comtrade_HS*.png")

print("\nYearly metrics (TopK share + HHI + TopK overlap):")
fmt = {
    "FAOTopKSharePct": "{:.2f}".format,
    "FAOHHI_0_1": "{:.4f}".format,
    "FAOHHI_0_10000": "{:.1f}".format,
    "TradeTopKSharePct": "{:.2f}".format,
    "TradeHHI_0_1": "{:.4f}".format,
    "TradeHHI_0_10000": "{:.1f}".format,
    "TopK_Jaccard": (lambda v: "NA" if pd.isna(v) else f"{v:.2f}"),
}
print(metrics.to_string(index=False, formatters=fmt))

print("\nCountry harmonization check:")
print(f"  Countries only in FAOSTAT after mapping: {len(only_fao)}")
print(f"  Countries only in Comtrade after mapping: {len(only_trade)}")
print("  (See debug CSV; extend NAME_MAP if important countries are missing.)")

if args.open:
    open_file_any(OUT_MAIN_PNG)
    # open_file_any(OUT_HHI_PNG)  # uncomment if you want to auto-open both