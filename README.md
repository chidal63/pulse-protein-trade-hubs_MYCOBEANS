# pulse-protein-trade-hubs
# Primary pulse production and protein ingredient trade (2020–2024)

This repository contains data, code and figures supporting the comparison between
primary pulse production and trade in derived protein ingredients (HS 210610)
over the period 2020–2024.

The analysis combines FAOSTAT production data (tonnes) with UN Comtrade trade data
(imports and exports, Partner = World) to examine structural differences between
the spatial distribution of primary agricultural production and ingredient-level
trade activity.

## Repository structure

- `data/`
  - `FAOSTAT_data_en_2-1-2026.xls`: FAOSTAT export of pulse production (tonnes)
  - `TradeData-3.xlsx`: UN Comtrade export for HS 210610 (protein concentrates and
    textured protein substances)
- `code/`
  - `compare_production_vs_trade_hubs.py`: Python script used to process data and
    generate figures
- `figures/`
  - Main scatter plot comparing production and trade shares
  - Heatmap of yearly concentration (HHI)
  - Year-by-year scatter plots (supplementary)

## Analysis summary

Primary pulse production is moderately concentrated and stable over time, whereas
trade in protein ingredients is less concentrated and shows increasing dispersion
across reporting economies. Overlap between leading producers and leading trade hubs
is limited, indicating that aggregation occurs downstream of primary production.

## Reproducibility

Figures were generated using the script in `code/`. The script requires Python
(>=3.9) and the following packages:

- pandas
- numpy
- matplotlib
- xlrd (for legacy FAOSTAT .xls files)

Example usage:

```bash
python code/compare_production_vs_trade_hubs.py \
  data/TradeData-3.xlsx data/FAOSTAT_data_en_2-1-2026.xls \
  --outdir figures --years 2020-2024 --make-yearly

