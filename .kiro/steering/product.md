# Product overview

This project analyses barometric pressure variability across weather stations worldwide to identify regions that are better or worse for sufferers of weather-triggered migraine disease. It produces per-region and world map figures showing the fraction of high-variation days at each station.

## Inputs

- **Weather station data** — fetched via the `meteostat` library; hourly pressure readings (`pres` column, in hPa) per station
- **Station metadata** — station ID, name, country (ISO 3166-1 alpha-2), region, latitude, longitude, elevation, timezone
- **Date range** — configurable; defaults to 2010-01-01 through today
- **Country codes** — all ISO 3166-1 alpha-2 codes via `pycountry`

## Outputs

- **Per-station Parquet files** — `data/daily/<station_id>.parquet` with columns: `date`, `pres_min`, `pres_max`
- **Station metadata CSV** — `data/processed/stations.csv` listing all successfully processed stations
- **Map figures** — `figures/<region>.png` for each continent plus a world map

## Data flow

```
meteostat API
    → data_acquisition.py   (fetch hourly → daily min/max per station, parallel by country)
    → processing.py         (outlier removal, daily pressure range, fractional variability)
    → make_maps.py          (plot stations on geographic maps by variability score)
```

## Domain constraints

- **Pressure unit**: hPa throughout; no unit conversion is performed
- **Variability metric**: fraction of days per year where daily pressure range ≥ 10 hPa (`thresh` in `compute_frac_var`)
- **Station quality filters**: stations with < 50% overall data completeness or > 50% underreported days (< 6 hourly readings) are excluded
- **Outlier removal**: days with more than one hourly pressure change outside 3× IQR are dropped before computing daily min/max
- **Incremental updates**: existing station Parquet files are updated from their last recorded date rather than re-fetched in full
