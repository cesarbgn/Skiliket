"""Modelo meteorológico con estado (climatología aproximada de Guadalajara).

Todo el ruido es AR(1) con coeficiente phi = exp(-dt/tau) e innovación
sigma * sqrt(1 - phi²): la varianza marginal y la autocorrelación en tiempo físico no
dependen del intervalo. Los eventos de lluvia usan tasas de Poisson integradas sobre dt y
la precipitación se acumula por intervalo en un pluviómetro de balancín de 0.2 mm, así que
los valores son coherentes con cualquier interval_s entre 10 y 3600.

El punto de rocío es solo estado interno: se usa para la HR (Magnus) y no se envía."""

import math
import random
from statistics import NormalDist
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from station import FIRMWARE, solar

SCHEMA_VERSION = "1.0"

# Climatología mensual aproximada de GDL (ene–dic). Conviene contrastar con normales del SMN.
_TMAX_C = (24.5, 26.5, 29.0, 31.3, 32.8, 30.5, 27.3, 27.0, 26.8, 26.5, 25.8, 24.5)
_TMIN_C = (6.8, 7.9, 10.0, 12.0, 14.5, 16.5, 16.0, 15.8, 15.5, 13.0, 9.8, 7.8)
_DEW_POINT_C = (3.0, 2.0, 2.0, 3.0, 6.0, 13.0, 15.0, 15.0, 15.0, 12.0, 7.0, 4.0)
_RAIN_DAY_PROB = (0.05, 0.03, 0.02, 0.03, 0.10, 0.50, 0.70, 0.70, 0.55, 0.25, 0.05, 0.05)
_CLOUD_MEAN = (0.15, 0.12, 0.10, 0.12, 0.18, 0.45, 0.55, 0.55, 0.50, 0.35, 0.20, 0.15)

# Magnus (Alduchov y Eskridge, 1996).
_MAGNUS_A, _MAGNUS_B = 17.625, 243.04

# Constantes de tiempo (s) y desviaciones de los procesos AR(1).
_TAU_T_ANOMALY, _SD_T_ANOMALY = 3 * 86400, 1.5  # anomalía día a día
_TAU_T_NOISE, _SD_T_NOISE = 1200, 0.2
# Inercia térmica: la temperatura responde a nubes y anomalía filtradas (paso bajo), no al
# valor instantáneo; si no, un cambio de nubosidad en la tarde movería la hora de Tmax.
_TAU_THERMAL = 3 * 3600
_TAU_CLOUD, _SD_CLOUD = 3 * 3600, 0.25
_TAU_DEW, _SD_DEW = 12 * 3600, 1.5
_TAU_SYNOPTIC, _SD_SYNOPTIC, _MAX_SYNOPTIC = 2 * 86400, 1.5, 3.5
_TAU_P_NOISE, _SD_P_NOISE = 1800, 0.1
_TAU_WIND, _TAU_DIR, _SD_DIR = 1800, 3 * 3600, 35.0
_TAU_DIR_NOISE, _SD_DIR_NOISE = 300, 10.0
_TAU_RAIN_RATE, _SD_RAIN_RATE = 900, 0.6

# Lluvia: 75 % de los inicios entre las 16 y las 22 h locales; duración media 1.5 h.
_RAIN_PEAK_START_H, _RAIN_PEAK_END_H, _RAIN_PEAK_SHARE = 16, 22, 0.75
_RAIN_MEAN_DURATION_S = 5400
_RAIN_MAX_RATE_MMH = 60.0
_BUCKET_MM = 0.2

_WEIBULL_K = 2.0
_WEIBULL_MEAN_FACTOR = math.gamma(1 + 1 / _WEIBULL_K)
_MAX_WIND_MS, _MAX_GUST_MS = 10.0, 20.0
_CALM_MS = 0.5

_BATTERY_MIN_V, _BATTERY_MAX_V = 12.0, 13.8
_BATTERY_CHARGE_PER_H, _BATTERY_DRAIN_PER_H = 0.6, 0.05


@dataclass(frozen=True)
class Site:
    lat: float = 20.67
    lon: float = -103.35
    elevation_m: float = 1550.0
    tz_offset_h: float = -6.0
    prevailing_wind_deg: int = 250


def format_timestamp(ts: datetime) -> str:
    """RFC 3339 en UTC con sufijo Z, a segundos."""
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _round(value: float, digits: int) -> float:
    # round() devuelve el double más cercano al decimal (repr "24.7", no "24.700000000000003");
    # + 0.0 convierte -0.0 en 0.0.
    return round(value, digits) + 0.0


def _phi(dt: float, tau: float) -> float:
    return math.exp(-dt / tau)


def _relax(value: float, target: float, dt: float, tau: float) -> float:
    """Relajación exponencial exacta hacia `target` durante dt."""
    return target + (value - target) * _phi(dt, tau)


_STD_NORMAL = NormalDist()


def _clamp_unit(p: float) -> float:
    return min(1 - 1e-12, max(1e-12, p))


def relative_humidity(temp_c: float, dew_point_c: float) -> float:
    """HR (%) con la fórmula de Magnus a partir de T y punto de rocío."""
    gamma_t = _MAGNUS_A * temp_c / (_MAGNUS_B + temp_c)
    gamma_d = _MAGNUS_A * dew_point_c / (_MAGNUS_B + dew_point_c)
    return 100 * math.exp(gamma_d - gamma_t)


def station_pressure_base_hpa(elevation_m: float) -> float:
    """Presión estándar a la altura de la estación (fórmula barométrica, ISA)."""
    return 1013.25 * (1 - 2.25577e-5 * elevation_m) ** 5.25588


def _climatology(local: datetime) -> tuple[float, float, float, float, float]:
    """(Tmax, Tmin, punto de rocío, prob. de lluvia diaria, nubosidad) interpolados por día."""
    day = local.timetuple().tm_yday - 1 + (local.hour + local.minute / 60) / 24
    month_pos = (day - 15.2) / (365.25 / 12)  # valores mensuales centrados a mitad de mes
    i = math.floor(month_pos)
    frac = month_pos - i
    a, b = i % 12, (i + 1) % 12

    def lerp(table: tuple[float, ...]) -> float:
        return table[a] + (table[b] - table[a]) * frac

    return (
        lerp(_TMAX_C),
        lerp(_TMIN_C),
        lerp(_DEW_POINT_C),
        lerp(_RAIN_DAY_PROB),
        lerp(_CLOUD_MEAN),
    )


def diurnal_shape(hour: float, h_min: float, h_max: float) -> float:
    """Ciclo diurno asimétrico normalizado: 0 en h_min (amanecer), 1 en h_max (tarde).

    Subida rápida por la mañana (seno). La bajada mezcla un decaimiento exponencial
    (enfriamiento de ~1 °C/h desde el pico, más rápido al anochecer) con un coseno que
    conserva pendiente hasta el amanecer: así el máximo no queda en una meseta por la tarde
    ni el mínimo en una meseta nocturna, y el ruido no los desplaza horas."""
    if h_min <= hour <= h_max:
        return math.sin(math.pi / 2 * (hour - h_min) / (h_max - h_min))
    x = ((hour - h_max) % 24) / (24 - (h_max - h_min))
    exponential = (math.exp(-2.5 * x) - math.exp(-2.5)) / (1 - math.exp(-2.5))
    cosine = 0.35 * math.cos(math.pi / 2 * x) + 0.65 * (1 + math.cos(math.pi * x)) / 2
    return 0.5 * exponential + 0.5 * cosine


def _wind_mean_ms(hour: float) -> float:
    """Velocidad media según la hora: mínima ~04 h, máxima ~16 h."""
    return 1.0 + 3.2 * 0.5 * (1 - math.cos(2 * math.pi * (hour - 4) / 24))


def _rain_start_rate_per_s(hour: float, day_prob: float) -> float:
    daily_rate = -math.log(1 - min(day_prob, 0.99))
    peak_hours = _RAIN_PEAK_END_H - _RAIN_PEAK_START_H
    if _RAIN_PEAK_START_H <= hour < _RAIN_PEAK_END_H:
        weight = _RAIN_PEAK_SHARE / peak_hours
    else:
        weight = (1 - _RAIN_PEAK_SHARE) / (24 - peak_hours)
    return daily_rate * weight / 3600


class WeatherModel:
    """Avanza por pasos: step(timestamp) -> measurements (dict listo para el payload)."""

    def __init__(self, site: Site, seed: int, interval_s: int) -> None:
        self.site = site
        self.interval_s = interval_s
        self._rng = random.Random(seed)
        rng = self._rng
        self._last_ts: datetime | None = None
        # Estado inicial tomado de la distribución estacionaria de cada proceso.
        self._t_anomaly = rng.gauss(0, _SD_T_ANOMALY)
        self._t_noise = rng.gauss(0, _SD_T_NOISE)
        self._cloud_z = rng.gauss(0, 1)
        self._cloud_thermal: float | None = None  # se inicia con la nubosidad del primer paso
        self._t_anomaly_thermal = self._t_anomaly
        self._dew_anomaly = rng.gauss(0, _SD_DEW)
        self._synoptic = rng.gauss(0, _SD_SYNOPTIC)
        self._p_noise = rng.gauss(0, _SD_P_NOISE)
        self._wind_z = rng.gauss(0, 1)
        self._dir_dev = rng.gauss(0, _SD_DIR)
        self._dir_noise = rng.gauss(0, _SD_DIR_NOISE)
        self._rain_z = 0.0
        self._raining = False
        self._rain_base_mmh = 0.0
        self._rain_cool_target = 0.0
        self._rain_cool = 0.0
        self._wetness = 0.0
        self._bucket_mm = 0.0
        self._battery_v = 12.8
        self.dew_point_c = 0.0
        self.battery_v = self._battery_v

    @property
    def raining(self) -> bool:
        """Si llovió en el último paso (estado interno, para pruebas)."""
        return self._raining

    def _ar1(self, value: float, dt: float, tau: float, sd: float) -> float:
        phi = _phi(dt, tau)
        return value * phi + self._rng.gauss(0, sd * math.sqrt(1 - phi * phi))

    def step(self, ts: datetime) -> dict[str, Any]:
        if ts.tzinfo is None:
            raise ValueError("timestamp sin zona horaria")
        if self._last_ts is not None and ts <= self._last_ts:
            raise ValueError("el tiempo del modelo debe avanzar")
        dt = float(self.interval_s if self._last_ts is None else (ts - self._last_ts).total_seconds())
        self._last_ts = ts
        site, rng = self.site, self._rng

        local = ts.astimezone(UTC) + timedelta(hours=site.tz_offset_h)
        hour = local.hour + local.minute / 60 + local.second / 3600
        tmax, tmin, dew_mean, rain_day_prob, cloud_mean = _climatology(local)

        # Procesos lentos y ruido.
        self._t_anomaly = self._ar1(self._t_anomaly, dt, _TAU_T_ANOMALY, _SD_T_ANOMALY)
        self._t_noise = self._ar1(self._t_noise, dt, _TAU_T_NOISE, _SD_T_NOISE)
        self._cloud_z = self._ar1(self._cloud_z, dt, _TAU_CLOUD, 1.0)
        self._dew_anomaly = self._ar1(self._dew_anomaly, dt, _TAU_DEW, _SD_DEW)
        self._synoptic = self._ar1(self._synoptic, dt, _TAU_SYNOPTIC, _SD_SYNOPTIC)
        self._synoptic = max(-_MAX_SYNOPTIC, min(_MAX_SYNOPTIC, self._synoptic))
        self._p_noise = self._ar1(self._p_noise, dt, _TAU_P_NOISE, _SD_P_NOISE)
        self._wind_z = self._ar1(self._wind_z, dt, _TAU_WIND, 1.0)
        self._dir_dev = self._ar1(self._dir_dev, dt, _TAU_DIR, _SD_DIR)
        self._dir_noise = self._ar1(self._dir_noise, dt, _TAU_DIR_NOISE, _SD_DIR_NOISE)
        self._rain_z = self._ar1(self._rain_z, dt, _TAU_RAIN_RATE, _SD_RAIN_RATE)

        # Lluvia: cadena de Markov en tiempo continuo (inicio y fin con tasas de Poisson).
        u_transition = rng.random()
        u_rate, u_cool = rng.random(), rng.random()
        rain_onset = False
        if self._raining:
            if u_transition < 1 - math.exp(-dt / _RAIN_MEAN_DURATION_S):
                self._raining = False
        elif u_transition < 1 - math.exp(-_rain_start_rate_per_s(hour, rain_day_prob) * dt):
            self._raining = rain_onset = True
            # Intensidad lognormal (mediana 3 mm/h) y enfriamiento de 3 a 6 °C.
            self._rain_base_mmh = 3.0 * math.exp(0.8 * _STD_NORMAL.inv_cdf(_clamp_unit(u_rate)))
            self._rain_cool_target = -(3.0 + 3.0 * u_cool)

        tips = 0
        if self._raining:
            rate = min(_RAIN_MAX_RATE_MMH, self._rain_base_mmh * math.exp(self._rain_z))
            self._bucket_mm += rate * dt / 3600
            tips = int(self._bucket_mm / _BUCKET_MM + 1e-9)
            self._bucket_mm = max(0.0, self._bucket_mm - tips * _BUCKET_MM)
            self._rain_cool = _relax(self._rain_cool, self._rain_cool_target, dt, 1200)
            self._wetness = _relax(self._wetness, 1.0, dt, 900)
        else:
            self._rain_cool = _relax(self._rain_cool, 0.0, dt, 7200)
            self._wetness = _relax(self._wetness, 0.0, dt, 3 * 3600)

        cloud = min(1.0, max(0.0, cloud_mean + _SD_CLOUD * self._cloud_z))
        if self._raining:
            cloud = max(cloud, 0.85)
        if self._cloud_thermal is None:
            self._cloud_thermal = cloud
        self._cloud_thermal = _relax(self._cloud_thermal, cloud, dt, _TAU_THERMAL)
        self._t_anomaly_thermal = _relax(self._t_anomaly_thermal, self._t_anomaly, dt, _TAU_THERMAL)

        # Temperatura: media estacional + ciclo diurno (amplitud menor con nubes) + ruido.
        sun = solar.sun_position(ts, site.lat, site.lon)
        noon = sun.solar_noon_utc_h + site.tz_offset_h
        sunrise = noon - sun.half_day_h
        h_min, h_max = sunrise + 0.15, noon + 2.2
        amplitude = (tmax - tmin) * (1 - 0.45 * self._cloud_thermal)
        temp = (
            (tmax + tmin) / 2
            + self._t_anomaly_thermal
            + amplitude * (diurnal_shape(hour, h_min, h_max) - 0.5)
            + self._t_noise
            + self._rain_cool
        )
        temp = max(-39.0, min(59.0, temp))

        # Punto de rocío (lento) <= T; la lluvia lo acerca a T, así que la HR sube a >= 90 %.
        dew = dew_mean + self._dew_anomaly
        dew = dew + self._wetness * max(0.0, (temp - 0.5) - dew)
        if self._raining:
            dew = max(dew, temp - 1.0)
        dew = min(dew, temp)
        self.dew_point_c = dew
        rh = min(100.0, max(0.0, relative_humidity(temp, dew)))

        # Presión: base barométrica + marea semidiurna (máx. ~10 y ~22 h) + deriva sinóptica.
        pressure = (
            station_pressure_base_hpa(site.elevation_m)
            + 1.2 * math.cos(2 * math.pi * (hour - 10) / 12)
            + self._synoptic
            + self._p_noise
        )

        # Viento: Weibull (k=2) con autocorrelación (AR(1) gaussiano transformado por cuantil).
        mean_wind = _wind_mean_ms(hour)
        u_wind = _clamp_unit(_STD_NORMAL.cdf(self._wind_z))
        speed = mean_wind / _WEIBULL_MEAN_FACTOR * (-math.log(1 - u_wind)) ** (1 / _WEIBULL_K)
        speed = min(_MAX_WIND_MS, speed)
        # Ráfaga = máximo del intervalo: el factor crece un poco con intervalos largos.
        gust_factor = 1.3 + 0.1 * math.log10(dt / 10) + 0.25 * rng.random()
        if rain_onset:
            gust_factor += 0.4  # frente de racha al inicio de la tormenta
        speed_r = _round(speed, 1)
        gust_r = _round(min(_MAX_GUST_MS, max(speed, speed * gust_factor)), 1)
        direction = None
        if speed_r >= _CALM_MS:
            direction = int(round(site.prevailing_wind_deg + self._dir_dev + self._dir_noise)) % 360

        # Radiación: cielo despejado x nubosidad (Kasten-Czeplak); 0 de noche.
        clear = solar.clear_sky_ghi(sun.elevation_deg, site.elevation_m)
        radiation = clear * (1 - 0.75 * cloud**3.4) * (0.3 if self._raining else 1.0)

        # Batería: carga solar hacia 13.8 V, descarga hacia 12.0 V (integración exacta en dt).
        charge = _BATTERY_CHARGE_PER_H * radiation / 1000
        k = (charge + _BATTERY_DRAIN_PER_H) / 3600
        target = (charge * _BATTERY_MAX_V + _BATTERY_DRAIN_PER_H * _BATTERY_MIN_V) / (
            charge + _BATTERY_DRAIN_PER_H
        )
        self._battery_v = target + (self._battery_v - target) * math.exp(-k * dt)
        self._battery_v = max(_BATTERY_MIN_V, min(_BATTERY_MAX_V, self._battery_v))
        self.battery_v = _round(self._battery_v, 2)

        return {
            "air_temperature_c": _round(temp, 1),
            "relative_humidity_pct": int(round(rh)),
            "station_pressure_hpa": _round(pressure, 1),
            "wind_speed_ms": speed_r,
            "wind_gust_ms": gust_r,
            "wind_direction_deg": direction,
            # Entero de vueltas x 0.2 y luego round: 3 vueltas -> 0.6, no 0.6000000000000001.
            "precipitation_mm": _round(tips * _BUCKET_MM, 1),
            "solar_radiation_wm2": int(round(radiation)),
        }


class ReadingGenerator:
    """Construye payloads v1. `sequence` empieza en 0 y sube en cada lectura generada."""

    def __init__(self, station_id: str, interval_s: int, seed: int, site: Site) -> None:
        self.station_id = station_id
        self.interval_s = interval_s
        self.model = WeatherModel(site, seed, interval_s)
        self.sequence = 0

    def next(self, ts: datetime) -> dict[str, Any]:
        measurements = self.model.step(ts)
        payload = {
            "schema_version": SCHEMA_VERSION,
            "station_id": self.station_id,
            "timestamp": format_timestamp(ts),
            "sequence": self.sequence,
            "interval_s": self.interval_s,
            "measurements": measurements,
            "device": {"battery_v": self.model.battery_v, "firmware": FIRMWARE},
        }
        self.sequence += 1
        return payload
