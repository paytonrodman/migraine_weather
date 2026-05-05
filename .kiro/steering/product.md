# Product overview

Analysis of barometric pressure variability across weather stations worldwide to identify regions better or worse for weather-triggered migraine sufferers. Produces per-region and world map figures showing the fraction of high-variation days per station.

## Outputs

- **Per-station Parquet files** — `data/daily/<station_id>.parquet` with columns: `date`, `pres_min`, `pres_max`
- **Station metadata CSV** — `data/processed/stations.csv`
- **Map figures** — `figures/<region>.png` per continent plus world map

## Domain constraints

- **Pressure unit**: hPa throughout; no unit conversion is performed
- **Variability metric**: fraction of days per year where daily pressure range ≥ 10 hPa (`thresh` in `compute_frac_var`)
- **Station quality filters**: stations with < 50% overall data completeness or > 50% underreported days (< 6 hourly readings) are excluded
- **Outlier removal**: days with more than one hourly pressure change outside 3× IQR are dropped before computing daily min/max
- **Incremental updates**: existing station Parquet files are updated from their last recorded date rather than re-fetched in full
