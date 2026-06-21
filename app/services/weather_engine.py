# SPDX-License-Identifier: MIT

"""
Weather Trading Engine for Polymarket
======================================
Compares weather forecasts from ECMWF + HRRR + METAR against Polymarket
temperature market prices. Generates signals when forecast probability
disagrees with market pricing by 10%+ EV.

Based on the proven weatherbot strategy ($300 → $101K).
All APIs are free, no keys needed (except Visual Crossing for post-resolution).

Data sources:
- Open-Meteo: ECMWF (global) + HRRR/GFS (US) forecasts — free, no key
- Aviation Weather: METAR real-time station observations — free, no key
- Polymarket Gamma API: market data — free, no key
"""

import asyncio
import json
import logging
import math
import re
import time
from datetime import datetime, timezone, timedelta

import requests

from app.services.websocket_manager import ws_manager

logger = logging.getLogger(__name__)


async def _brain(msg: str):
    """Send brain narration for Bot Brain tab."""
    await ws_manager.send_log(msg, "brain")

# ══════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════

MIN_EV = 0.13           # 13% minimum EV — 10% base + ~3% taker fee buffer (Polymarket charges taker fees on weather markets)
MAX_PRICE = 0.45        # Never buy above 45c (same as weatherbot — strict risk/reward)
MIN_VOLUME = 100        # Minimum market volume (BeefSlayer-style deep longshots live in thin books)
MIN_VOLUME_DEEP = 50    # Even lower floor for penny entries where forecast strongly disagrees with market
DEEP_PRICE_CEIL = 0.05  # If YES price <= 5c, treat as deep longshot and use MIN_VOLUME_DEEP
MIN_HOURS = 2.0         # Don't bet on markets resolving within 2 hours
MAX_HOURS = 72.0        # Don't bet on markets resolving after 72 hours

# Concentration tier: cities where a high-volume whale (BeefSlayer, 1,541 trades,
# 67.7% WR, +$58K PnL) has shown persistent edge. Bump signal score for these
# cities so Kelly sizing leans in where the real-world whale proof exists.
CONCENTRATION_CITIES = {"nyc", "seattle", "chicago", "atlanta"}
CONCENTRATION_SCORE_BONUS = 10  # +10pt score → larger Kelly bet on these cities

# Default forecast uncertainty (degrees)
SIGMA_F = 2.0           # Fahrenheit
SIGMA_C = 1.2           # Celsius

# Airport station mapping — critical for accuracy
# Polymarket resolves based on specific weather stations, NOT city centers
LOCATIONS = {
    "nyc":          {"lat": 40.7772,  "lon":  -73.8726, "name": "New York City", "station": "KLGA", "unit": "F", "region": "us"},
    "chicago":      {"lat": 41.9742,  "lon":  -87.9073, "name": "Chicago",       "station": "KORD", "unit": "F", "region": "us"},
    "miami":        {"lat": 25.7959,  "lon":  -80.2870, "name": "Miami",         "station": "KMIA", "unit": "F", "region": "us"},
    "dallas":       {"lat": 32.8998,  "lon":  -97.0403, "name": "Dallas",        "station": "KDFW", "unit": "F", "region": "us"},
    "seattle":      {"lat": 47.4502,  "lon": -122.3088, "name": "Seattle",       "station": "KSEA", "unit": "F", "region": "us"},
    "atlanta":      {"lat": 33.6407,  "lon":  -84.4277, "name": "Atlanta",       "station": "KATL", "unit": "F", "region": "us"},
    "los-angeles":  {"lat": 33.9425,  "lon": -118.4081, "name": "Los Angeles",   "station": "KLAX", "unit": "F", "region": "us"},
    "london":       {"lat": 51.5048,  "lon":    0.0495, "name": "London",        "station": "EGLC", "unit": "C", "region": "eu"},
    "paris":        {"lat": 48.9962,  "lon":    2.5979, "name": "Paris",         "station": "LFPG", "unit": "C", "region": "eu"},
    "munich":       {"lat": 48.3537,  "lon":   11.7750, "name": "Munich",        "station": "EDDM", "unit": "C", "region": "eu"},
    "ankara":       {"lat": 40.1281,  "lon":   32.9951, "name": "Ankara",        "station": "LTAC", "unit": "C", "region": "eu"},
    "seoul":        {"lat": 37.4691,  "lon":  126.4505, "name": "Seoul",         "station": "RKSI", "unit": "C", "region": "asia"},
    "tokyo":        {"lat": 35.5494,  "lon":  139.7798, "name": "Tokyo",         "station": "RJTT", "unit": "C", "region": "asia"},
    "shanghai":     {"lat": 31.1443,  "lon":  121.8083, "name": "Shanghai",      "station": "ZSPD", "unit": "C", "region": "asia"},
    "singapore":    {"lat":  1.3502,  "lon":  103.9940, "name": "Singapore",     "station": "WSSS", "unit": "C", "region": "asia"},
    "beijing":      {"lat": 40.0799,  "lon":  116.6031, "name": "Beijing",       "station": "ZBAA", "unit": "C", "region": "asia"},
    "shenzhen":     {"lat": 22.6393,  "lon":  113.8107, "name": "Shenzhen",      "station": "ZGSZ", "unit": "C", "region": "asia"},
    "toronto":      {"lat": 43.6772,  "lon":  -79.6306, "name": "Toronto",       "station": "CYYZ", "unit": "C", "region": "ca"},
    "sao-paulo":    {"lat": -23.4356, "lon":  -46.4731, "name": "Sao Paulo",     "station": "SBGR", "unit": "C", "region": "sa"},
    "buenos-aires": {"lat": -34.8222, "lon":  -58.5358, "name": "Buenos Aires",  "station": "SAEZ", "unit": "C", "region": "sa"},
    # ── Expansion cities (added 2026-04-14) ──
    "houston":       {"lat": 29.9844,  "lon":  -95.3414, "name": "Houston",       "station": "KIAH", "unit": "F", "region": "us"},
    "phoenix":       {"lat": 33.4373,  "lon": -112.0078, "name": "Phoenix",       "station": "KPHX", "unit": "F", "region": "us"},
    "denver":        {"lat": 39.8561,  "lon": -104.6737, "name": "Denver",        "station": "KDEN", "unit": "F", "region": "us"},
    "las-vegas":     {"lat": 36.0800,  "lon": -115.1522, "name": "Las Vegas",     "station": "KLAS", "unit": "F", "region": "us"},
    "boston":        {"lat": 42.3631,  "lon":  -71.0065, "name": "Boston",        "station": "KBOS", "unit": "F", "region": "us"},
    "san-francisco": {"lat": 37.6213,  "lon": -122.3790, "name": "San Francisco", "station": "KSFO", "unit": "F", "region": "us"},
    "amsterdam":     {"lat": 52.3086,  "lon":    4.7639, "name": "Amsterdam",     "station": "EHAM", "unit": "C", "region": "eu"},
    "madrid":        {"lat": 40.4936,  "lon":   -3.5668, "name": "Madrid",        "station": "LEMD", "unit": "C", "region": "eu"},
    "rome":          {"lat": 41.7999,  "lon":   12.2462, "name": "Rome",          "station": "LIRF", "unit": "C", "region": "eu"},
    "berlin":        {"lat": 52.3667,  "lon":   13.5033, "name": "Berlin",        "station": "EDDB", "unit": "C", "region": "eu"},
    "bangkok":       {"lat": 13.6900,  "lon":  100.7501, "name": "Bangkok",       "station": "VTBS", "unit": "C", "region": "asia"},
    "dubai":         {"lat": 25.2532,  "lon":   55.3657, "name": "Dubai",         "station": "OMDB", "unit": "C", "region": "eu"},
    "hong-kong":     {"lat": 22.3080,  "lon":  113.9185, "name": "Hong Kong",     "station": "VHHH", "unit": "C", "region": "asia"},
    "mumbai":        {"lat": 19.0896,  "lon":   72.8656, "name": "Mumbai",        "station": "VABB", "unit": "C", "region": "asia"},
}

TIMEZONES = {
    "nyc": "America/New_York", "chicago": "America/Chicago",
    "miami": "America/New_York", "dallas": "America/Chicago",
    "seattle": "America/Los_Angeles", "atlanta": "America/New_York",
    "los-angeles": "America/Los_Angeles",
    "london": "Europe/London", "paris": "Europe/Paris",
    "munich": "Europe/Berlin", "ankara": "Europe/Istanbul",
    "seoul": "Asia/Seoul", "tokyo": "Asia/Tokyo",
    "shanghai": "Asia/Shanghai", "singapore": "Asia/Singapore",
    "beijing": "Asia/Shanghai", "shenzhen": "Asia/Shanghai",
    "toronto": "America/Toronto", "sao-paulo": "America/Sao_Paulo",
    "buenos-aires": "America/Argentina/Buenos_Aires",
    "houston": "America/Chicago", "phoenix": "America/Phoenix",
    "denver": "America/Denver", "las-vegas": "America/Los_Angeles",
    "boston": "America/New_York", "san-francisco": "America/Los_Angeles",
    "amsterdam": "Europe/Amsterdam", "madrid": "Europe/Madrid",
    "rome": "Europe/Rome", "berlin": "Europe/Berlin",
    "bangkok": "Asia/Bangkok", "dubai": "Asia/Dubai",
    "hong-kong": "Asia/Hong_Kong", "mumbai": "Asia/Kolkata",
}

MONTHS = ["january", "february", "march", "april", "may", "june",
          "july", "august", "september", "october", "november", "december"]


# ══════════════════════════════════════════════════════════════
# MATH
# ══════════════════════════════════════════════════════════════

def _norm_cdf(x: float) -> float:
    """Standard normal CDF."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _bucket_prob(forecast: float, t_low: float, t_high: float, sigma: float) -> float:
    """Probability that actual temp falls in bucket [t_low, t_high].

    CRITICAL: Matches the proven weatherbot logic exactly.
    - Edge buckets ("X or below", "X or higher"): normal CDF tails
    - Interior buckets: BINARY 1.0 or 0.0 (if forecast is in bucket = 100%)
    - No "near-miss" fallback — only bet on the bucket the forecast matches
    """
    if t_low == -999:
        # "X or below" bucket
        return _norm_cdf((t_high - forecast) / sigma)
    if t_high == 999:
        # "X or higher" bucket
        return 1.0 - _norm_cdf((t_low - forecast) / sigma)
    # Interior bucket — BINARY: forecast is in or out. Period.
    return 1.0 if _in_bucket(forecast, t_low, t_high) else 0.0


def _calc_ev(prob: float, price: float) -> float:
    """Expected value: how much you make per dollar risked."""
    if price <= 0 or price >= 1:
        return 0.0
    return round(prob * (1.0 / price - 1.0) - (1.0 - prob), 4)


def _in_bucket(forecast: float, t_low: float, t_high: float) -> bool:
    """Check if forecast falls within the bucket range."""
    if t_low == t_high:
        return round(forecast) == round(t_low)
    if t_low == -999:
        return forecast <= t_high
    if t_high == 999:
        return forecast >= t_low
    return t_low <= forecast <= t_high


# ══════════════════════════════════════════════════════════════
# FORECAST FETCHING (all free, no API keys)
# ══════════════════════════════════════════════════════════════

async def _get_ecmwf(city_slug: str, dates: list[str]) -> dict[str, float]:
    """ECMWF forecast via Open-Meteo (free, global coverage)."""
    loc = LOCATIONS[city_slug]
    temp_unit = "fahrenheit" if loc["unit"] == "F" else "celsius"
    tz = TIMEZONES.get(city_slug, "UTC")
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={loc['lat']}&longitude={loc['lon']}"
        f"&daily=temperature_2m_max&temperature_unit={temp_unit}"
        f"&forecast_days=7&timezone={tz}"
        f"&models=ecmwf_ifs025&bias_correction=true"
    )
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None, lambda: requests.get(url, timeout=10)
        )
        data = resp.json()
        if "error" in data:
            return {}
        result = {}
        for date, temp in zip(data["daily"]["time"], data["daily"]["temperature_2m_max"]):
            if date in dates and temp is not None:
                result[date] = round(temp, 1) if loc["unit"] == "C" else round(temp)
        return result
    except Exception as e:
        logger.debug(f"ECMWF {city_slug}: {e}")
        return {}


async def _get_hrrr(city_slug: str, dates: list[str]) -> dict[str, float]:
    """HRRR forecast via Open-Meteo (US cities only, 48h horizon).

    Uses the actual HRRR model (3km high-resolution) with bias correction,
    NOT gfs_seamless (13km global). HRRR is far more accurate for US cities.
    """
    loc = LOCATIONS[city_slug]
    if loc["region"] != "us":
        return {}
    tz = TIMEZONES.get(city_slug, "UTC")
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={loc['lat']}&longitude={loc['lon']}"
        f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
        f"&forecast_days=3&timezone={tz}"
        f"&models=hrrr&bias_correction=true"
    )
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None, lambda: requests.get(url, timeout=10)
        )
        data = resp.json()
        if "error" in data:
            return {}
        result = {}
        for date, temp in zip(data["daily"]["time"], data["daily"]["temperature_2m_max"]):
            if date in dates and temp is not None:
                result[date] = round(temp)
        return result
    except Exception as e:
        logger.debug(f"HRRR {city_slug}: {e}")
        return {}


async def _get_icon(city_slug: str, dates: list[str]) -> dict[str, float]:
    """ICON (DWD German Weather Service) forecast via Open-Meteo.

    Covers EU cities well — higher resolution than ECMWF for European locations.
    Used as second model for EU cities in the disagreement filter.
    """
    loc = LOCATIONS[city_slug]
    if loc["region"] not in ("eu", "ca", "sa"):
        return {}
    temp_unit = "fahrenheit" if loc["unit"] == "F" else "celsius"
    tz = TIMEZONES.get(city_slug, "UTC")
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={loc['lat']}&longitude={loc['lon']}"
        f"&daily=temperature_2m_max&temperature_unit={temp_unit}"
        f"&forecast_days=7&timezone={tz}"
        f"&models=icon_seamless&bias_correction=true"
    )
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None, lambda: requests.get(url, timeout=10)
        )
        data = resp.json()
        if "error" in data:
            return {}
        result = {}
        for date, temp in zip(data["daily"]["time"], data["daily"]["temperature_2m_max"]):
            if date in dates and temp is not None:
                result[date] = round(temp, 1) if loc["unit"] == "C" else round(temp)
        return result
    except Exception as e:
        logger.debug(f"ICON {city_slug}: {e}")
        return {}


async def _get_jma(city_slug: str, dates: list[str]) -> dict[str, float]:
    """JMA (Japan Meteorological Agency) forecast via Open-Meteo.

    Better accuracy for East/Southeast Asia (Japan, Korea, China, Singapore).
    Used as second model for Asia cities in the disagreement filter.
    """
    loc = LOCATIONS[city_slug]
    if loc["region"] != "asia":
        return {}
    temp_unit = "fahrenheit" if loc["unit"] == "F" else "celsius"
    tz = TIMEZONES.get(city_slug, "UTC")
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={loc['lat']}&longitude={loc['lon']}"
        f"&daily=temperature_2m_max&temperature_unit={temp_unit}"
        f"&forecast_days=7&timezone={tz}"
        f"&models=jma_seamless&bias_correction=true"
    )
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None, lambda: requests.get(url, timeout=10)
        )
        data = resp.json()
        if "error" in data:
            return {}
        result = {}
        for date, temp in zip(data["daily"]["time"], data["daily"]["temperature_2m_max"]):
            if date in dates and temp is not None:
                result[date] = round(temp, 1) if loc["unit"] == "C" else round(temp)
        return result
    except Exception as e:
        logger.debug(f"JMA {city_slug}: {e}")
        return {}


async def _get_metar(city_slug: str) -> float | None:
    """Current observed temp from METAR aviation weather station (free)."""
    loc = LOCATIONS[city_slug]
    station = loc["station"]
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: requests.get(
                f"https://aviationweather.gov/api/data/metar?ids={station}&format=json",
                timeout=8,
            ),
        )
        data = resp.json()
        if data and isinstance(data, list):
            temp_c = data[0].get("temp")
            if temp_c is not None:
                if loc["unit"] == "F":
                    return round(float(temp_c) * 9 / 5 + 32)
                return round(float(temp_c), 1)
    except Exception as e:
        logger.debug(f"METAR {city_slug}: {e}")
    return None


# ══════════════════════════════════════════════════════════════
# POLYMARKET WEATHER MARKET MATCHING
# ══════════════════════════════════════════════════════════════

def _parse_temp_range(question: str) -> tuple[float, float] | None:
    """Parse temperature bucket from market question text.

    Handles: "75°F or below", "80°F or higher", "between 75-79°F", "be 75°F on"
    """
    num = r'(-?\d+(?:\.\d+)?)'
    if re.search(r'or below', question, re.IGNORECASE):
        m = re.search(num + r'[°]?[FC\s] or below', question, re.IGNORECASE)
        if m:
            return (-999.0, float(m.group(1)))
    if re.search(r'or higher', question, re.IGNORECASE):
        m = re.search(num + r'[°]?[FC\s] or higher', question, re.IGNORECASE)
        if m:
            return (float(m.group(1)), 999.0)
    m = re.search(r'between ' + num + r'[-–]' + num + r'[°]?[FC]', question, re.IGNORECASE)
    if m:
        return (float(m.group(1)), float(m.group(2)))
    m = re.search(r'be ' + num + r'[°]?[FC\s] on', question, re.IGNORECASE)
    if m:
        v = float(m.group(1))
        return (v, v)
    return None


def _hours_to_resolution(end_date_str: str) -> float:
    """Hours until market resolves."""
    try:
        end = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        return max(0.0, (end - datetime.now(timezone.utc)).total_seconds() / 3600)
    except Exception:
        return 999.0


# ══════════════════════════════════════════════════════════════
# SIGNAL CACHE
# ══════════════════════════════════════════════════════════════

_weather_signals: list[dict] = []
_weather_last_scan: float = 0


# ══════════════════════════════════════════════════════════════
# MAIN SCAN: FIND WEATHER MISPRICINGS
# ══════════════════════════════════════════════════════════════

async def scan_weather_markets() -> list[dict]:
    """Scan all weather markets across 20 cities for forecast-based mispricings.

    For each city:
    1. Fetch weather forecasts from ECMWF + HRRR + METAR
    2. Find matching Polymarket temperature markets
    3. Calculate probability for each temp bucket
    4. Compare against market price → find EV > 8%
    5. Generate signal for auto-betting

    Returns list of actionable signals.
    """
    global _weather_signals, _weather_last_scan

    # On first call after a restart, try to restore from Postgres before scanning.
    if _weather_last_scan == 0 and not _weather_signals:
        try:
            from app.services import pg_store
            cached, cached_ts = await pg_store.load_signal_cache("weather")
            if cached and cached_ts > 0:
                _weather_signals = cached
                _weather_last_scan = cached_ts
                logger.info(f"WEATHER: Restored {len(cached)} signals from Postgres (scan was {(time.time()-cached_ts)/60:.1f}min ago)")
        except Exception as _e:
            logger.warning(f"WEATHER: Could not restore cache from Postgres: {_e}")

    # Adaptive scan interval — scan more frequently as markets approach resolution.
    # Compute real hours_left from end_date (not the stale cached value which
    # was frozen at scan time and drifts further wrong the longer we cache it).
    #   < 2h  until resolution → 5 min interval
    #   < 12h until resolution → 10 min interval
    #   otherwise              → 30 min interval
    elapsed = time.time() - _weather_last_scan
    if elapsed < 1800:
        if _weather_signals:
            now_ts = datetime.now(timezone.utc)
            min_hours = 999.0
            for s in _weather_signals:
                ed = s.get("end_date", "")
                if ed:
                    try:
                        end = datetime.fromisoformat(ed.replace("Z", "+00:00"))
                        h = (end - now_ts).total_seconds() / 3600
                        if h < min_hours:
                            min_hours = h
                    except Exception:
                        pass
            if min_hours < 2:
                ttl = 300      # 5 min
            elif min_hours < 12:
                ttl = 600      # 10 min
            else:
                ttl = 1800     # 30 min
            if elapsed < ttl:
                return _weather_signals
        else:
            # No signals last time — don't hammer API; respect 30 min cache
            return _weather_signals

    await ws_manager.send_log(f"[WEATHER] Scanning temperature markets across {len(LOCATIONS)} cities...", "info")
    await _brain(f"Fetching weather forecasts from ECMWF + HRRR/ICON/JMA + METAR for {len(LOCATIONS)} cities...")

    signals = []
    now = datetime.now(timezone.utc)
    dates = [(now + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(4)]
    cities_checked = 0
    markets_found = 0

    for city_slug, loc in LOCATIONS.items():
        try:
            # Fetch forecasts from all sources
            # Region-specific: HRRR for US, ICON for EU/CA/SA, JMA for Asia
            ecmwf = await _get_ecmwf(city_slug, dates)
            hrrr = await _get_hrrr(city_slug, dates)      # US only
            icon = await _get_icon(city_slug, dates)      # EU/CA/SA only
            jma  = await _get_jma(city_slug, dates)       # Asia only

            # Get METAR for today only
            today = now.strftime("%Y-%m-%d")
            metar_temp = await _get_metar(city_slug) if today in dates else None

            # Warn if all forecast sources failed for this city
            if not ecmwf and not hrrr and not icon and not jma and metar_temp is None:
                await ws_manager.send_log(
                    f"[WEATHER] All forecast sources failed for {loc['name']} — skipping",
                    "warning",
                )
                continue

            cities_checked += 1

            for date in dates:
                # Pick best forecast by region:
                #   US  → HRRR (3km high-res) > ECMWF
                #   EU  → ICON (DWD) > ECMWF
                #   Asia → JMA > ECMWF
                #   CA/SA → ICON > ECMWF
                dt = datetime.strptime(date, "%Y-%m-%d")
                forecast_temp = None
                source = None
                second_model = {}   # for disagreement filter

                if loc["region"] == "us":
                    if date in hrrr:
                        forecast_temp = hrrr[date]
                        source = "hrrr"
                    second_model = hrrr
                    second_model_name = "hrrr"
                elif loc["region"] == "asia":
                    if date in jma:
                        forecast_temp = jma[date]
                        source = "jma"
                    elif date in ecmwf:
                        forecast_temp = ecmwf[date]
                        source = "ecmwf"
                    second_model = jma
                    second_model_name = "jma"
                elif loc["region"] in ("eu", "ca", "sa"):
                    if date in icon:
                        forecast_temp = icon[date]
                        source = "icon"
                    second_model = icon
                    second_model_name = "icon"
                else:
                    second_model = {}
                    second_model_name = "second"

                # Fall back to ECMWF if primary source didn't have this date
                if forecast_temp is None and date in ecmwf:
                    forecast_temp = ecmwf[date]
                    source = "ecmwf"

                # Override with METAR for today — but ONLY after 13:00 local time.
                # METAR is the current observation, not the daily high. Using it at
                # 2 AM local time gives the overnight low, which is useless as a
                # daily-max proxy. After 1 PM the temperature has usually peaked.
                if date == today and metar_temp is not None:
                    try:
                        from zoneinfo import ZoneInfo
                        tz_name = TIMEZONES.get(city_slug, "UTC")
                        local_hour = datetime.now(ZoneInfo(tz_name)).hour
                    except Exception:
                        local_hour = datetime.now(timezone.utc).hour  # fallback
                    if local_hour >= 13:
                        # After 1 PM local time the daily high has likely occurred —
                        # METAR observation is now a reliable daily-max proxy
                        forecast_temp = metar_temp
                        source = "metar"

                if forecast_temp is None:
                    continue

                # Model disagreement filter — compare ECMWF against the region-specific
                # second model (HRRR for US, ICON for EU, JMA for Asia).
                # If the two models diverge significantly the forecast is unreliable.
                if date in ecmwf and date in second_model:
                    disagreement = abs(ecmwf[date] - second_model[date])
                    max_disagreement = 4 if loc["unit"] == "F" else 2
                    if disagreement > max_disagreement:
                        await _brain(
                            f"Skip {loc['name']} {date}: ECMWF {ecmwf[date]}° vs {second_model_name.upper()} {second_model[date]}° "
                            f"— {disagreement}° model disagreement exceeds {max_disagreement}° threshold"
                        )
                        continue

                # Find Polymarket event for this city/date
                month = MONTHS[dt.month - 1]
                event_slug = f"highest-temperature-in-{city_slug}-on-{month}-{dt.day}-{dt.year}"

                loop = asyncio.get_running_loop()
                try:
                    resp = await loop.run_in_executor(
                        None,
                        lambda s=event_slug: requests.get(
                            f"https://gamma-api.polymarket.com/events?slug={s}",
                            timeout=8,
                        ),
                    )
                    event_data = resp.json()
                    if not event_data or not isinstance(event_data, list) or len(event_data) == 0:
                        continue
                    event = event_data[0]
                except Exception:
                    continue

                end_date = event.get("endDate", "")
                hours = _hours_to_resolution(end_date) if end_date else 999

                if hours < MIN_HOURS or hours > MAX_HOURS:
                    continue

                # Get sigma for this city/source
                # Use CALIBRATED sigma if available (learned from past forecast errors)
                from app.services.self_learner import get_calibrated_sigma
                default_sigma = SIGMA_F if loc["unit"] == "F" else SIGMA_C
                sigma = get_calibrated_sigma(city_slug, default_sigma)

                # CRITICAL: Find exactly ONE bucket matching the forecast
                # This is how the original weatherbot works — don't scan all buckets
                matched_market = None
                for market in event.get("markets", []):
                    question = market.get("question", "")
                    rng = _parse_temp_range(question)
                    if not rng:
                        continue
                    t_low, t_high = rng
                    if _in_bucket(forecast_temp, t_low, t_high):
                        matched_market = market
                        break

                if not matched_market:
                    continue

                question = matched_market.get("question", "")
                market_id = str(matched_market.get("id", ""))
                slug = matched_market.get("slug", "")
                volume = float(matched_market.get("volume", 0) or 0)
                rng = _parse_temp_range(question)

                # Absolute volume floor — below this the book is too thin to trust
                if not rng or volume < MIN_VOLUME_DEEP:
                    continue

                t_low, t_high = rng

                # Get market price (use ask price = what we'd pay to buy)
                try:
                    prices = json.loads(matched_market.get("outcomePrices", "[0.5,0.5]"))
                    if not prices or not isinstance(prices, list):
                        continue
                    price = float(prices[0])
                except (json.JSONDecodeError, ValueError, IndexError, TypeError):
                    continue

                if price >= MAX_PRICE or price <= 0.02:
                    continue

                # Tiered volume gate: standard markets need MIN_VOLUME, but deep
                # longshots (YES <= 5c) can be thinner — that's where BeefSlayer's
                # biggest wins live (0.2c, 2c, 5.3c entries on illiquid penny buckets).
                if volume < MIN_VOLUME and price > DEEP_PRICE_CEIL:
                    continue

                # Bucket boundary distance filter — interior and point buckets.
                # If the forecast is within 1°F (0.5°C) of a bucket edge, the actual
                # daily high could easily land in the adjacent bucket. Skip these
                # "knife-edge" bets — they look like 100% probability but aren't.
                if t_low != -999 and t_high != 999:
                    boundary_margin = 1.0 if loc["unit"] == "F" else 0.5
                    if t_low == t_high:
                        # Point bucket: rounding boundary is at ±0.5° from center.
                        # Skip if forecast is within point_margin of the rounding threshold
                        # (e.g. 18.6° for bucket 19° is only 0.1° from the 18.5° boundary → skip).
                        point_margin = 0.3 if loc["unit"] == "F" else 0.15
                        dist_to_rounding = 0.5 - abs(forecast_temp - t_low)
                        if dist_to_rounding < point_margin:
                            await _brain(
                                f"Skip {loc['name']} {date}: forecast {forecast_temp}° too close to "
                                f"rounding boundary of point bucket {t_low}° — boundary risk"
                            )
                            continue
                    elif (forecast_temp - t_low) < boundary_margin or (t_high - forecast_temp) < boundary_margin:
                        await _brain(
                            f"Skip {loc['name']} {date}: forecast {forecast_temp}° too close to "
                            f"bucket edge [{t_low}-{t_high}] — boundary risk"
                        )
                        continue

                # Calculate probability from forecast (binary for interior buckets)
                prob = _bucket_prob(forecast_temp, t_low, t_high, sigma)

                # NO-side evaluation: if forecast is clearly outside this bucket,
                # buying NO (at 1-price) may have strong EV even when YES prob is low
                if prob < 0.10:
                    no_price = round(1.0 - price, 4)
                    prob_no = 1.0 - prob
                    if 0.05 < no_price <= MAX_PRICE:
                        ev_no = _calc_ev(prob_no, no_price)
                        if ev_no >= MIN_EV:
                            unit_sym = "F" if loc["unit"] == "F" else "C"
                            if t_low == -999:
                                bucket_label = f"{t_high}{unit_sym} or below"
                            elif t_high == 999:
                                bucket_label = f"{t_low}{unit_sym} or higher"
                            else:
                                bucket_label = f"{t_low}-{t_high}{unit_sym}"
                            await _brain(
                                f"Weather NO edge: {loc['name']} {date} — "
                                f"forecast {forecast_temp}° outside bucket {bucket_label}, "
                                f"NO@{no_price*100:.0f}c, prob_NO {prob_no*100:.0f}%, EV +{ev_no*100:.1f}%"
                            )
                            signals.append({
                                "type": "weather_forecast",
                                "market": question[:80],
                                "slug": slug or market_id,
                                "market_url": f"https://polymarket.com/market/{slug}" if slug else "",
                                "city": loc["name"],
                                "city_slug": city_slug,
                                "date": date,
                                "bucket": bucket_label,
                                "forecast_temp": forecast_temp,
                                "forecast_source": source,
                                "probability": round(prob_no * 100, 1),
                                "market_price": round(no_price * 100, 1),
                                "ev": round(ev_no * 100, 1),
                                "side": "NO",
                                "price": no_price,
                                "volume": round(volume, 0),
                                "hours_left": round(hours, 1),
                                "score": min(95, 40 + ev_no * 200 + (CONCENTRATION_SCORE_BONUS if city_slug in CONCENTRATION_CITIES else 0)),
                                "timestamp": time.time(),
                                "end_date": end_date,
                            })
                            markets_found += 1
                    continue  # Skip YES eval — prob too low for YES side

                if prob <= 0.01:
                    continue

                # Calculate expected value (YES side)
                ev = _calc_ev(prob, price)
                if ev < MIN_EV:
                    continue

                markets_found += 1

                await _brain(
                    f"Weather edge: {loc['name']} {date} \u2014 "
                    f"forecast {forecast_temp}\u00b0 ({source.upper()}), "
                    f"bucket {t_low}-{t_high}, market {price*100:.0f}c, "
                    f"prob {prob*100:.0f}%, EV +{ev*100:.1f}%"
                )

                # Build temperature range label
                unit_sym = "F" if loc["unit"] == "F" else "C"
                if t_low == -999:
                    bucket_label = f"{t_high}{unit_sym} or below"
                elif t_high == 999:
                    bucket_label = f"{t_low}{unit_sym} or higher"
                elif t_low == t_high:
                    bucket_label = f"{t_low}{unit_sym}"
                else:
                    bucket_label = f"{t_low}-{t_high}{unit_sym}"

                signals.append({
                    "type": "weather_forecast",
                    "market": question[:80],
                    "slug": slug or market_id,
                    "market_url": f"https://polymarket.com/market/{slug}" if slug else "",
                    "city": loc["name"],
                    "city_slug": city_slug,
                    "date": date,
                    "bucket": bucket_label,
                    "forecast_temp": forecast_temp,
                    "forecast_source": source,
                    "probability": round(prob * 100, 1),
                    "market_price": round(price * 100, 1),
                    "ev": round(ev * 100, 1),
                    "side": "YES",
                    "price": round(price, 4),
                    "volume": round(volume, 0),
                    "hours_left": round(hours, 1),
                    "score": min(95, 40 + ev * 200 + (CONCENTRATION_SCORE_BONUS if city_slug in CONCENTRATION_CITIES else 0)),
                    "timestamp": time.time(),
                    "end_date": end_date,
                })

            await asyncio.sleep(0.3)  # Rate limit between cities

        except Exception as e:
            logger.debug(f"Weather scan {city_slug}: {e}")

    # Sort by EV descending
    signals.sort(key=lambda s: s["ev"], reverse=True)
    _weather_signals = signals[:20]
    _weather_last_scan = time.time()

    # Push notifications for new moonshots — fires immediately on scan, not dependent on betting loop
    # Persist to Postgres so signals survive redeploys
    try:
        from app.services import pg_store
        await pg_store.save_signal_cache("weather", _weather_signals, _weather_last_scan)
    except Exception as _e:
        logger.warning(f"WEATHER: Could not persist cache to Postgres: {_e}")

    if signals:
        await ws_manager.send_log(
            f"[WEATHER] Found {len(signals)} signals across {cities_checked} cities "
            f"(best: {signals[0]['city']} {signals[0]['bucket']} EV +{signals[0]['ev']}%)",
            "success",
        )
        await _brain(
            f"Weather scan complete: {len(signals)} profitable opportunities found "
            f"across {cities_checked} cities. Best: {signals[0]['city']} "
            f"{signals[0]['bucket']} at {signals[0]['market_price']}c (EV +{signals[0]['ev']}%)"
        )
    else:
        await ws_manager.send_log(
            f"[WEATHER] Checked {cities_checked} cities, {markets_found} markets — no EV > {MIN_EV*100:.0f}% found",
            "info",
        )
        await _brain(f"Weather scan: checked {cities_checked} cities — no mispricings above {MIN_EV*100:.0f}% EV right now.")

    return _weather_signals


def get_weather_signals() -> list[dict]:
    """Return cached weather signals for auto-betting."""
    return list(_weather_signals)


async def fetch_actual_high_temp(city_slug: str, date: str) -> float | None:
    """Fetch the actual observed daily high temperature from Open-Meteo archive API.

    Called when a weather bet resolves, to store the real temperature for
    sigma calibration in self_learner.py. Uses ERA5 reanalysis (archive).

    Args:
        city_slug: e.g. "nyc", "munich"
        date: ISO date string "YYYY-MM-DD"

    Returns:
        Actual daily high temperature in the city's unit (F or C), or None.
    """
    loc = LOCATIONS.get(city_slug)
    if not loc:
        return None

    temp_unit = "fahrenheit" if loc["unit"] == "F" else "celsius"
    tz = TIMEZONES.get(city_slug, "UTC")
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={loc['lat']}&longitude={loc['lon']}"
        f"&daily=temperature_2m_max&temperature_unit={temp_unit}"
        f"&start_date={date}&end_date={date}&timezone={tz}"
    )
    loop = asyncio.get_running_loop()
    try:
        resp = await loop.run_in_executor(
            None, lambda: requests.get(url, timeout=10)
        )
        data = resp.json()
        temps = data.get("daily", {}).get("temperature_2m_max", [])
        if temps and temps[0] is not None:
            val = float(temps[0])
            return round(val) if loc["unit"] == "F" else round(val, 1)
    except Exception as e:
        logger.debug(f"Archive temp {city_slug} {date}: {e}")
    return None
