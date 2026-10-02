# Revisión Fase B — código

Volcado literal generado por script desde los bytes de cada archivo. Cada bloque está delimitado por una cerca de backticks más larga que cualquier secuencia de backticks del archivo. N = líneas según `wc -l` (saltos de línea); sha256 = hash de los bytes del archivo.

Archivos nuevos de la estación simulada: dependencias, configuración y paquete `station`. No se modificó ningún archivo de `ingest_api/`, `schema/` ni `scripts/`.

=== station/requirements.txt === (11 líneas, sha256: c566b742d854babc7f56e501627fe057d6c127cedd09243421c1ee292e08741a)

```text
# Dependencias de ejecución, versiones exactas (incluye transitivas).
# Probadas con Python 3.13.5 en el mismo venv que ingest_api/requirements-dev.txt.
# httpx (no httpx2): el API usa httpx2 solo como cliente de pruebas de Starlette; son
# módulos distintos (httpx / httpx2) y conviven sin conflicto.
httpx==0.28.1
anyio==4.15.1
certifi==2026.7.22
h11==0.16.0
httpcore==1.0.9
idna==3.20
typing_extensions==4.16.0
```

=== station/requirements-dev.txt === (8 líneas, sha256: f94017334c96f4be6cf5db49e2f2eef0e465ffe30c2315dfd976c8d233713bb9)

```text
# Dependencias de pruebas, versiones exactas (incluye transitivas).
-r requirements.txt
pytest==9.1.1
colorama==0.4.6; sys_platform == "win32"
iniconfig==2.3.0
packaging==26.3
pluggy==1.6.0
Pygments==2.21.0
```

=== station/pytest.ini === (5 líneas, sha256: 8bbad0254015471de2585c2fad181f7c30b88f2076e238c2425850e45c56e25b)

```ini
[pytest]
pythonpath = .
testpaths = tests
# Temporales de pruebas dentro del repo (ignorados por git); pytest los vacía al iniciar.
addopts = --basetemp=.pytest_tmp
```

=== station/.env.example === (43 líneas, sha256: f7a3cc5de30277a0eb280bcf81b632ba761547417bfff78fc4e669e0920655f4)

```dotenv
# Variables de la estación simulada (una estación por proceso). Copia a .env y ajusta.
# Ni Python ni la estación leen .env por sí solos: define las variables en la terminal
# (ver README) o deja que docker compose las cargue (Fase C).
#
# STATION_KEY es un secreto: va en .env (ignorado por git) o en la terminal, nunca en
# archivos versionados. Se genera con scripts/gen_station_key.py.

# Obligatorias
STATION_ID=GDL-001
STATION_KEY=

# Destino y ritmo
INGEST_URL=http://127.0.0.1:8000/v1/readings
# Segundos entre lecturas, 10 a 3600 (mismos límites que interval_s en el API).
INTERVAL_S=60

# Semilla del modelo (entero >= 0). Vacía: se deriva de sha256(STATION_ID).
SEED=

# Sitio (Guadalajara). TZ_OFFSET_H es un offset fijo: México no usa horario de verano desde 2022.
LAT=20.67
LON=-103.35
ELEVATION_M=1550
TZ_OFFSET_H=-6
# Dirección predominante del viento, grados (0-359).
PREVAILING_WIND_DEG=250

# Réplica: horas hacia atrás a generar al arrancar (0 = apagado, máximo 167) y minutos
# reales en que se envían (0 = lo más rápido posible). Luego continúa en modo vivo.
REPLAY_HOURS=0
REPLAY_DURATION_MIN=2

# Probabilidad (0.0 a 1.0) de mutar una lectura con una falla que el API debe rechazar con 422.
FAULT_RATE=0

# Lecturas máximas en el búfer en memoria; al llenarse se descarta la más vieja.
BUFFER_MAX=1000
# Termina tras generar N lecturas (0 = sin límite). Útil para pruebas y demos.
MAX_READINGS=0

# Timeouts HTTP en segundos (mayores que 0, máximo 120).
HTTP_CONNECT_TIMEOUT_S=5
HTTP_READ_TIMEOUT_S=10
```

=== station/station/__init__.py === (3 líneas, sha256: f7b26176e96e09387dad7be337dba4c322893cd58a4a6e7c7751751b94bcb705)

```python
"""Estación meteorológica simulada: genera lecturas v1 y las envía al API de ingesta."""

FIRMWARE = "sim-0.1.0"
```

=== station/station/config.py === (169 líneas, sha256: 402f668f6df10a3eb3d5ac2f1b61a6f09f08dd100e793f2641c1a139f84be903)

```python
"""Configuración de la estación leída de variables de entorno (fail-fast).

Los mensajes de error nunca incluyen valores: así una clave pegada por error en otra
variable tampoco termina en la salida."""

import hashlib
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit

STATION_ID_PATTERN = r"^[A-Z]{3}-[0-9]{3}$"
# ASCII visible sin espacios: es seguro como valor de cabecera HTTP.
_KEY_PATTERN = r"^[\x21-\x7e]{16,256}$"

DEFAULT_INGEST_URL = "http://127.0.0.1:8000/v1/readings"
INTERVAL_MIN_S, INTERVAL_MAX_S = 10, 3600  # mismos límites que interval_s en el API
# El API rechaza timestamps de más de 7 días; 167 h deja 1 h de margen para enviar la réplica.
REPLAY_MAX_HOURS = 167

_STATION_ID = re.compile(STATION_ID_PATTERN)
_KEY = re.compile(_KEY_PATTERN)


class ConfigError(ValueError):
    """Configuración inválida; la estación no debe arrancar."""


class Secret:
    """Envuelve la clave para que repr(), str() y f-strings no la muestren."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "Secret('***')"

    __str__ = __repr__

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Secret) and other._value == self._value

    def __hash__(self) -> int:
        return hash(self._value)


@dataclass(frozen=True)
class Config:
    station_id: str
    station_key: Secret = field(repr=False)
    ingest_url: str = DEFAULT_INGEST_URL
    interval_s: int = 60
    seed: int = 0
    lat: float = 20.67
    lon: float = -103.35
    elevation_m: float = 1550.0
    tz_offset_h: float = -6.0
    prevailing_wind_deg: int = 250
    replay_hours: int = 0
    replay_duration_min: float = 2.0
    fault_rate: float = 0.0
    buffer_max: int = 1000
    max_readings: int = 0
    http_connect_timeout_s: float = 5.0
    http_read_timeout_s: float = 10.0


def default_seed(station_id: str) -> int:
    """Semilla estable entre ejecuciones y plataformas (hash() de str cambia por proceso)."""
    return int.from_bytes(hashlib.sha256(station_id.encode("ascii")).digest()[:8], "big")


def _get(env: Mapping[str, str], name: str) -> str | None:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return None
    return raw.strip()


def _int(env: Mapping[str, str], name: str, default: int, lo: int, hi: int) -> int:
    raw = _get(env, name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{name} debe ser un entero entre {lo} y {hi}") from None
    if not lo <= value <= hi:
        raise ConfigError(f"{name} debe ser un entero entre {lo} y {hi}")
    return value


def _float(env: Mapping[str, str], name: str, default: float, lo: float, hi: float) -> float:
    raw = _get(env, name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ConfigError(f"{name} debe ser un número entre {lo} y {hi}") from None
    if not math.isfinite(value) or not lo <= value <= hi:
        raise ConfigError(f"{name} debe ser un número entre {lo} y {hi}")
    return value


def _timeout(env: Mapping[str, str], name: str, default: float) -> float:
    value = _float(env, name, default, 0.0, 120.0)
    if value <= 0:
        raise ConfigError(f"{name} debe ser mayor que 0 y como máximo 120")
    return value


def _url(env: Mapping[str, str]) -> str:
    raw = _get(env, "INGEST_URL")
    if raw is None:
        return DEFAULT_INGEST_URL
    try:
        parts = urlsplit(raw)
        _ = parts.port  # lanza ValueError si el puerto no es válido
    except ValueError:
        raise ConfigError("INGEST_URL no es una URL válida") from None
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ConfigError("INGEST_URL debe ser una URL http(s) con host")
    if parts.username or parts.password:
        raise ConfigError("INGEST_URL no debe llevar credenciales; la clave va en STATION_KEY")
    return raw


def load_config(env: Mapping[str, str]) -> Config:
    station_id = _get(env, "STATION_ID")
    if station_id is None:
        raise ConfigError("Falta STATION_ID (formato AAA-000, p. ej. GDL-001)")
    if not _STATION_ID.fullmatch(station_id):
        raise ConfigError("STATION_ID debe cumplir ^[A-Z]{3}-[0-9]{3}$ (p. ej. GDL-001)")

    key = env.get("STATION_KEY")
    if not key:
        raise ConfigError("Falta STATION_KEY (genérala con scripts/gen_station_key.py)")
    if not _KEY.fullmatch(key):
        raise ConfigError(
            "STATION_KEY debe tener entre 16 y 256 caracteres ASCII visibles, sin espacios"
        )

    return Config(
        station_id=station_id,
        station_key=Secret(key),
        ingest_url=_url(env),
        interval_s=_int(env, "INTERVAL_S", 60, INTERVAL_MIN_S, INTERVAL_MAX_S),
        seed=_int(env, "SEED", default_seed(station_id), 0, 2**64 - 1),
        lat=_float(env, "LAT", 20.67, -90.0, 90.0),
        lon=_float(env, "LON", -103.35, -180.0, 180.0),
        # 5,000 m da ~540 hPa: dentro del rango 500–1100 que acepta el API.
        elevation_m=_float(env, "ELEVATION_M", 1550.0, -400.0, 5000.0),
        tz_offset_h=_float(env, "TZ_OFFSET_H", -6.0, -12.0, 14.0),
        prevailing_wind_deg=_int(env, "PREVAILING_WIND_DEG", 250, 0, 359),
        replay_hours=_int(env, "REPLAY_HOURS", 0, 0, REPLAY_MAX_HOURS),
        replay_duration_min=_float(env, "REPLAY_DURATION_MIN", 2.0, 0.0, 1440.0),
        fault_rate=_float(env, "FAULT_RATE", 0.0, 0.0, 1.0),
        buffer_max=_int(env, "BUFFER_MAX", 1000, 1, 1_000_000),
        max_readings=_int(env, "MAX_READINGS", 0, 0, 10_000_000),
        http_connect_timeout_s=_timeout(env, "HTTP_CONNECT_TIMEOUT_S", 5.0),
        http_read_timeout_s=_timeout(env, "HTTP_READ_TIMEOUT_S", 10.0),
    )
```

=== station/station/solar.py === (75 líneas, sha256: a256aba68399c63b0b78345b5d5b9ba3a7616393254c25b8e92b2c55d6a54ec9)

```python
"""Posición del sol y radiación de cielo despejado, sin dependencias externas.

Declinación y ecuación del tiempo: series de Fourier de Spencer (1971), error < 0.0006 rad
y < 0.6 min. Ángulo horario a partir de UTC y longitud. Radiación global de cielo
despejado: modelo de Haurwitz con una corrección lineal por altitud."""

import math
from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class SunPosition:
    elevation_deg: float  # negativa de noche
    solar_noon_utc_h: float  # hora UTC del mediodía solar de ese día
    half_day_h: float  # mitad de la duración del día (sol sobre el horizonte)


def _utc_hours(ts: datetime) -> float:
    return ts.hour + ts.minute / 60 + ts.second / 3600


def _declination_eot(ts: datetime) -> tuple[float, float]:
    """Declinación (rad) y ecuación del tiempo (min), calculadas una sola vez por instante."""
    day_of_year = ts.timetuple().tm_yday
    g = 2 * math.pi * (day_of_year - 1 + (_utc_hours(ts) - 12) / 24) / 365
    cos1, sin1 = math.cos(g), math.sin(g)
    cos2, sin2 = math.cos(2 * g), math.sin(2 * g)
    decl = (
        0.006918
        - 0.399912 * cos1
        + 0.070257 * sin1
        - 0.006758 * cos2
        + 0.000907 * sin2
        - 0.002697 * math.cos(3 * g)
        + 0.00148 * math.sin(3 * g)
    )
    eot = 229.18 * (
        0.000075 + 0.001868 * cos1 - 0.032077 * sin1 - 0.014615 * cos2 - 0.040849 * sin2
    )
    return decl, eot


def sun_position(ts: datetime, lat: float, lon: float) -> SunPosition:
    ts = ts.astimezone(UTC)
    decl, eot = _declination_eot(ts)
    phi = math.radians(lat)
    # Hora solar verdadera -> ángulo horario (15° por hora desde el mediodía solar).
    solar_time = (_utc_hours(ts) + lon / 15 + eot / 60) % 24
    hour_angle = math.radians(15 * (solar_time - 12))
    sin_e = math.sin(phi) * math.sin(decl) + math.cos(phi) * math.cos(decl) * math.cos(hour_angle)
    elevation = math.degrees(math.asin(max(-1.0, min(1.0, sin_e))))
    cos_h = -math.tan(phi) * math.tan(decl)
    if cos_h <= -1:
        half_day = 12.0  # sol de medianoche
    elif cos_h >= 1:
        half_day = 0.0  # noche polar
    else:
        half_day = math.degrees(math.acos(cos_h)) / 15
    return SunPosition(elevation, 12 - lon / 15 - eot / 60, half_day)


def elevation_deg(ts: datetime, lat: float, lon: float) -> float:
    """Elevación del sol sobre el horizonte, en grados (negativa de noche)."""
    return sun_position(ts, lat, lon).elevation_deg


def clear_sky_ghi(elevation: float, elevation_m: float) -> float:
    """Radiación global horizontal de cielo despejado (W/m²); 0 con el sol bajo el horizonte."""
    if elevation <= 0:
        return 0.0
    cos_z = math.sin(math.radians(elevation))
    # Haurwitz (nivel del mar) + ~6 % por km de altitud (menos masa de aire).
    ghi = 1098.0 * cos_z * math.exp(-0.057 / cos_z)
    return ghi * (1 + 0.06 * max(0.0, elevation_m) / 1000)
```

=== station/station/weather_model.py === (354 líneas, sha256: 909663624629ca2666107adc577136026bb6e062fd9f8c48ded60e3c9f9cdff4)

```python
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
```

=== station/station/faults.py === (98 líneas, sha256: 49907dfa31127286b26773eb2b1e96c5a725ee6c0b6572637e04cd19970c4d25)

```python
"""Inyección de fallas: muta un payload ya generado para provocar un 422 conocido.

Usa su propio random.Random, independiente del clima: cambiar FAULT_RATE no altera los
valores meteorológicos de las lecturas que no se mutan."""

import copy
import hashlib
import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from station.weather_model import format_timestamp

Payload = dict[str, Any]


@dataclass(frozen=True)
class Fault:
    name: str
    field: str  # campo esperado en errors[].field del 422
    code: str  # código esperado en errors[].code
    apply: Callable[[Payload, datetime], None]


def _humidity_over_100(p: Payload, now: datetime) -> None:
    p["measurements"]["relative_humidity_pct"] = 101


def _temperature_below_min(p: Payload, now: datetime) -> None:
    p["measurements"]["air_temperature_c"] = -41


def _gust_below_speed(p: Payload, now: datetime) -> None:
    m = p["measurements"]
    m["wind_speed_ms"] = max(1.0, m["wind_speed_ms"])
    m["wind_gust_ms"] = round(m["wind_speed_ms"] - 0.5, 1)


def _extra_field(p: Payload, now: datetime) -> None:
    p["unexpected_field"] = 1


def _missing_required(p: Payload, now: datetime) -> None:
    del p["measurements"]["station_pressure_hpa"]


def _string_in_number(p: Payload, now: datetime) -> None:
    p["measurements"]["air_temperature_c"] = str(p["measurements"]["air_temperature_c"])


def _future_timestamp(p: Payload, now: datetime) -> None:
    # +10 min sobre el reloj actual, no sobre el timestamp de la lectura: en réplica o con
    # intervalos largos, timestamp + 10 min puede seguir en el pasado y el API lo aceptaría.
    p["timestamp"] = format_timestamp(now + timedelta(minutes=10))


def _bad_schema_version(p: Payload, now: datetime) -> None:
    p["schema_version"] = "2.0"


def _float_in_integer(p: Payload, now: datetime) -> None:
    p["interval_s"] = float(p["interval_s"])  # 60 -> 60.0


CATALOG: tuple[Fault, ...] = (
    Fault("humidity_over_100", "measurements.relative_humidity_pct", "less_than_equal", _humidity_over_100),
    Fault("temperature_below_min", "measurements.air_temperature_c", "greater_than_equal", _temperature_below_min),
    Fault("gust_below_speed", "measurements.wind_gust_ms", "gust_less_than_speed", _gust_below_speed),
    Fault("extra_field", "unexpected_field", "extra_forbidden", _extra_field),
    Fault("missing_required", "measurements.station_pressure_hpa", "missing", _missing_required),
    Fault("string_in_number", "measurements.air_temperature_c", "float_type", _string_in_number),
    Fault("future_timestamp", "timestamp", "timestamp_in_future", _future_timestamp),
    Fault("bad_schema_version", "schema_version", "literal_error", _bad_schema_version),
    Fault("float_in_integer", "interval_s", "int_type", _float_in_integer),
)
BY_NAME = {fault.name: fault for fault in CATALOG}


def fault_seed(seed: int) -> int:
    """Semilla de las fallas derivada de la del clima, pero en otro flujo aleatorio."""
    return int.from_bytes(hashlib.sha256(f"faults:{seed}".encode("ascii")).digest()[:8], "big")


class FaultInjector:
    def __init__(self, rate: float, seed: int) -> None:
        self.rate = rate
        self._rng = random.Random(fault_seed(seed))

    def maybe_inject(self, payload: Payload, now: datetime | None = None) -> tuple[Payload, Fault | None]:
        """Devuelve (payload, falla). Con falla, el payload es una copia mutada."""
        if self._rng.random() >= self.rate:
            return payload, None
        fault = self._rng.choice(CATALOG)
        mutated = copy.deepcopy(payload)
        fault.apply(mutated, now or datetime.now(UTC))
        return mutated, fault
```

=== station/station/sender.py === (286 líneas, sha256: 255cd45ca1b890d5d24db5e1a9c81e9763f04d05591ae8ddffc486e4c3ff7c98)

```python
"""Envío de lecturas: POST con clasificación de respuestas, búfer FIFO acotado y backoff.

Política:
- 2xx (201) y 409 = entregada (409: ya estaba guardada; se cuenta como duplicate).
- 5xx, 408, 429, timeouts y errores de conexión = reintentable: la lectura se queda al
  frente del búfer y se reintenta con backoff exponencial con jitter (tope 60 s).
- Cualquier otro 4xx (400, 401, 403, 413, 415, 422...) = no reintentable: se descarta.
Los logs llevan station_id, sequence, status, intento y un resumen del problem+json
(type y pares field/code); nunca el payload completo ni la clave."""

import json
import logging
import random
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

import httpx

from station.config import Secret
from station.faults import Fault

logger = logging.getLogger("skilliket.station")

STATION_KEY_HEADER = "X-Station-Key"
BACKOFF_BASE_S = 1.0
BACKOFF_MAX_S = 60.0
_MAX_ERRORS_LOGGED = 20
_MAX_TEXT = 100

Outcome = Literal["created", "duplicate", "retry", "rejected"]


@dataclass
class Pending:
    payload: dict[str, Any]
    fault: Fault | None = None
    attempts: int = 0

    @property
    def sequence(self) -> int | None:
        return self.payload.get("sequence")

    @property
    def timestamp(self) -> Any:
        return self.payload.get("timestamp")


@dataclass
class Stats:
    generated: int = 0
    created: int = 0
    duplicates: int = 0
    dropped_by_status: dict[str, int] = field(default_factory=dict)
    dropped_overflow: int = 0
    retries: int = 0
    faults_injected: int = 0
    faults_confirmed: int = 0
    faults_unexpected: int = 0
    unsent_at_exit: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "generated": self.generated,
            "created": self.created,
            "duplicates": self.duplicates,
            "dropped_by_status": dict(sorted(self.dropped_by_status.items())),
            "dropped_overflow": self.dropped_overflow,
            "retries": self.retries,
            "faults_injected": self.faults_injected,
            "faults_confirmed": self.faults_confirmed,
            "faults_unexpected": self.faults_unexpected,
            "unsent_at_exit": self.unsent_at_exit,
        }


@dataclass(frozen=True)
class Attempt:
    outcome: Outcome
    status: int | None = None
    error_type: str | None = None  # clase de la excepción de transporte
    problem_type: str | None = None
    errors: tuple[tuple[str, str], ...] = ()  # (field, code) del 422


def _short(value: Any) -> str:
    return str(value)[:_MAX_TEXT]


def _problem_summary(response: httpx.Response) -> tuple[str | None, tuple[tuple[str, str], ...]]:
    """type y pares (field, code) del problem+json; nunca los valores devueltos."""
    try:
        body = response.json()
    except ValueError:
        return None, ()
    if not isinstance(body, dict):
        return None, ()
    problem_type = _short(body["type"]) if isinstance(body.get("type"), str) else None
    pairs = []
    errors = body.get("errors")
    if isinstance(errors, list):
        for error in errors[:_MAX_ERRORS_LOGGED]:
            if isinstance(error, dict):
                pairs.append((_short(error.get("field")), _short(error.get("code"))))
    return problem_type, tuple(pairs)


def classify(status: int) -> Outcome:
    if 200 <= status < 300:
        return "created"
    if status == 409:
        return "duplicate"
    if status in (408, 429) or status >= 500:
        return "retry"
    return "rejected"


class Sender:
    """Un POST por llamada; nunca lanza por errores de red: los devuelve como Attempt."""

    def __init__(self, client: httpx.Client, url: str, key: Secret) -> None:
        self._client = client
        self._url = url
        self._key = key

    def post(self, payload: dict[str, Any], timeout: float | None = None) -> Attempt:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers = {"Content-Type": "application/json", STATION_KEY_HEADER: self._key.reveal()}
        kwargs: dict[str, Any] = {}
        if timeout is not None:
            kwargs["timeout"] = httpx.Timeout(max(0.1, timeout))
        try:
            response = self._client.post(self._url, content=body, headers=headers, **kwargs)
        except httpx.RequestError as exc:
            # Solo el tipo: el mensaje de algunas excepciones incluye la URL o el request.
            return Attempt("retry", error_type=type(exc).__name__)
        outcome = classify(response.status_code)
        problem_type, errors = (None, ())
        if outcome in ("rejected", "retry", "duplicate"):
            problem_type, errors = _problem_summary(response)
        return Attempt(outcome, response.status_code, problem_type=problem_type, errors=errors)


class Outbox:
    """Búfer FIFO acotado. El frente se reintenta hasta entregarse o descartarse, así que
    el orden de entrega se conserva. Si se llena, se descarta la lectura más vieja."""

    def __init__(
        self,
        sender: Sender,
        station_id: str,
        max_size: int,
        stats: Stats,
        monotonic: Callable[[], float],
        jitter: random.Random | None = None,
    ) -> None:
        self._sender = sender
        self._station_id = station_id
        self._max_size = max_size
        self._stats = stats
        self._monotonic = monotonic
        self._jitter = jitter or random.Random()
        self._queue: deque[Pending] = deque()
        self._failures = 0  # fallos reintentables seguidos
        self.retry_at = 0.0  # instante monotónico a partir del cual se puede enviar

    def __len__(self) -> int:
        return len(self._queue)

    def sequences(self) -> list[int | None]:
        return [p.sequence for p in self._queue]

    def put(self, pending: Pending) -> None:
        if len(self._queue) >= self._max_size:
            dropped = self._queue.popleft()
            self._stats.dropped_overflow += 1
            logger.warning(
                "buffer_overflow",
                extra={
                    "event": "buffer_overflow",
                    "station_id": self._station_id,
                    "dropped_sequence": dropped.sequence,
                    "buffered": len(self._queue),
                },
            )
        self._queue.append(pending)

    def retry_now(self) -> None:
        self.retry_at = 0.0

    def backoff_delay(self) -> float:
        base = min(BACKOFF_MAX_S, BACKOFF_BASE_S * 2 ** (self._failures - 1))
        return base * (0.5 + 0.5 * self._jitter.random())

    def send_next(self, timeout: float | None = None) -> Attempt | None:
        """Intenta enviar el frente del búfer. Devuelve el resultado o None si está vacío."""
        if not self._queue:
            return None
        pending = self._queue[0]
        pending.attempts += 1
        attempt = self._sender.post(pending.payload, timeout)
        if attempt.outcome == "retry":
            self._failures += 1
            self._stats.retries += 1
            delay = self.backoff_delay()
            self.retry_at = self._monotonic() + delay
            logger.warning(
                "send_retry",
                extra={
                    "event": "send_retry",
                    "station_id": self._station_id,
                    "sequence": pending.sequence,
                    "status": attempt.status,
                    "error_type": attempt.error_type,
                    "attempt": pending.attempts,
                    "delay_s": round(delay, 2),
                    "buffered": len(self._queue),
                },
            )
            return attempt

        self._queue.popleft()
        self._failures = 0
        self.retry_at = 0.0
        if attempt.outcome == "created":
            self._stats.created += 1
        elif attempt.outcome == "duplicate":
            self._stats.duplicates += 1
        else:
            key = str(attempt.status)
            self._stats.dropped_by_status[key] = self._stats.dropped_by_status.get(key, 0) + 1
        self._log_result(pending, attempt)
        if pending.fault is not None:
            self._check_fault(pending, pending.fault, attempt)
        return attempt

    def _log_result(self, pending: Pending, attempt: Attempt) -> None:
        extra: dict[str, Any] = {
            "event": "reading",
            "station_id": self._station_id,
            "sequence": pending.sequence,
            "timestamp": pending.timestamp,
            "status": attempt.status,
            "attempt": pending.attempts,
            "outcome": attempt.outcome,
        }
        if attempt.outcome in ("created", "duplicate"):
            logger.info("reading_delivered", extra=extra)
            return
        extra["problem_type"] = attempt.problem_type
        extra["errors"] = [list(pair) for pair in attempt.errors]
        if attempt.status == 401:
            extra["reason"] = "check_station_key"
            logger.error(
                "reading_rejected: 401, el API no reconoce STATION_KEY; revisa la clave "
                "(y que su hash esté en STATION_KEY_HASHES del API). No se reintenta.",
                extra=extra,
            )
        elif attempt.status == 403:
            extra["reason"] = "check_station_id"
            logger.error(
                "reading_rejected: 403, la clave pertenece a otra estación; revisa STATION_ID "
                "y STATION_KEY. No se reintenta.",
                extra=extra,
            )
        else:
            logger.warning("reading_rejected", extra=extra)

    def _check_fault(self, pending: Pending, fault: Fault, attempt: Attempt) -> None:
        expected = [fault.field, fault.code]
        received = {"status": attempt.status, "errors": [list(pair) for pair in attempt.errors]}
        extra = {
            "event": "fault_check",
            "station_id": self._station_id,
            "sequence": pending.sequence,
            "fault": fault.name,
            "expected": {"status": 422, "errors": [expected]},
            "received": received,
        }
        if attempt.status == 422 and set(attempt.errors) == {(fault.field, fault.code)}:
            self._stats.faults_confirmed += 1
            logger.info("fault_confirmed", extra=extra)
        else:
            self._stats.faults_unexpected += 1
            logger.warning("fault_mismatch", extra=extra)
```

=== station/station/log.py === (55 líneas, sha256: 43a7c89badbc46f294790da0f9bf58f6c792ac17c421c61098c1b1ae6bb377ae)

```python
"""Logs estructurados (una línea JSON por evento). Nunca incluyen la clave ni el payload completo."""

import json
import logging
import sys
from datetime import UTC, datetime

_EXTRA_FIELDS = (
    "event",
    "station_id",
    "sequence",
    "timestamp",
    "status",
    "attempt",
    "outcome",
    "reason",
    "error_type",
    "problem_type",
    "errors",
    "fault",
    "expected",
    "received",
    "delay_s",
    "buffered",
    "dropped_sequence",
    "summary",
    "config",
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for name in _EXTRA_FIELDS:
            if hasattr(record, name):
                payload[name] = getattr(record, name)
        # ASCII escapado: la consola de Windows puede ser cp1252.
        return json.dumps(payload)


def configure_logging() -> None:
    logger = logging.getLogger("skilliket")
    if any(getattr(h, "_skilliket", False) for h in logger.handlers):
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler._skilliket = True  # type: ignore[attr-defined]
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
```

=== station/station/main.py === (252 líneas, sha256: d04bda8e1436963e371c9b4ccaa0fa62651090205ce12b960596fd6191b69936)

```python
"""Bucle principal de la estación. Arranque: python -m station.main

Modo vivo: un tick por intervalo, programado sobre el reloj monotónico a partir del
arranque (sin deriva acumulada); el timestamp de cada tick es un múltiplo del intervalo
(piso del reloj de pared) y nunca queda en el futuro. Modo réplica (REPLAY_HOURS > 0):
primero las lecturas de las últimas N horas repartidas en REPLAY_DURATION_MIN, luego el
modo vivo con el mismo modelo y la misma secuencia. La réplica tiene contrapresión: su
lectura siguiente se genera cuando el búfer está vacío, así que nunca lo desborda.

Fin: con MAX_READINGS, al entregar o descartar todas las lecturas generadas; con
SIGINT/SIGTERM/SIGBREAK, tras intentar vaciar el búfer durante 10 s como máximo."""

import logging
import os
import signal
import sys
import threading
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Protocol

import httpx

from station.config import Config, ConfigError, load_config
from station.faults import FaultInjector
from station.log import configure_logging
from station.sender import Outbox, Pending, Sender, Stats
from station.weather_model import ReadingGenerator, Site

logger = logging.getLogger("skilliket.station")

DRAIN_TIMEOUT_S = 10.0
_WAIT_SLICE_S = 0.5  # la espera se parte para atender señales pronto (también en Windows)
_WALL_SKEW_WAIT_S = 0.05


class Clock(Protocol):
    def monotonic(self) -> float: ...

    def time(self) -> float: ...

    def wait(self, seconds: float) -> None:
        """Espera hasta `seconds`; vuelve antes si se pidió detener la estación."""

    def sleep(self, seconds: float) -> None:
        """Espera `seconds` aunque se haya pedido detener (para el vaciado final)."""


class SystemClock:
    def __init__(self, stop: threading.Event) -> None:
        self._stop = stop

    def monotonic(self) -> float:
        return time.monotonic()

    def time(self) -> float:
        return time.time()

    def wait(self, seconds: float) -> None:
        end = time.monotonic() + seconds
        while not self._stop.is_set():
            remaining = end - time.monotonic()
            if remaining <= 0:
                return
            self._stop.wait(min(remaining, _WAIT_SLICE_S))

    def sleep(self, seconds: float) -> None:
        end = time.monotonic() + seconds
        while (remaining := end - time.monotonic()) > 0:
            time.sleep(min(remaining, _WAIT_SLICE_S))


class Schedule:
    """Secuencia de (instante monotónico en que toca, timestamp en segundos epoch)."""

    def __init__(self, cfg: Config, start_wall: float, start_mono: float) -> None:
        self._interval = cfg.interval_s
        self._start_wall = start_wall
        self._start_mono = start_mono
        self.floor_start = int(start_wall // cfg.interval_s) * cfg.interval_s
        self.replay_count = cfg.replay_hours * 3600 // cfg.interval_s
        self._spacing = (
            cfg.replay_duration_min * 60 / self.replay_count if self.replay_count else 0.0
        )
        self.index = 0

    def peek(self) -> tuple[float, int]:
        k = self.index
        if k < self.replay_count:
            ts = self.floor_start - (self.replay_count - 1 - k) * self._interval
            return self._start_mono + k * self._spacing, ts
        # Sin réplica, la primera lectura viva es el piso del arranque (se emite de inmediato);
        # con réplica, ese instante ya lo cubrió la última lectura de la réplica.
        j = k - self.replay_count + (1 if self.replay_count else 0)
        ts = self.floor_start + j * self._interval
        return self._start_mono + (ts - self._start_wall), ts

    @property
    def in_replay(self) -> bool:
        return self.index < self.replay_count

    def advance(self) -> None:
        self.index += 1


def _drain(outbox: Outbox, clock: Clock, read_timeout: float) -> None:
    """Al detenerse por señal, intenta vaciar el búfer durante DRAIN_TIMEOUT_S como máximo."""
    deadline = clock.monotonic() + DRAIN_TIMEOUT_S
    outbox.retry_now()
    while len(outbox):
        now = clock.monotonic()
        if now >= deadline:
            return
        if now < outbox.retry_at:
            clock.sleep(min(outbox.retry_at, deadline) - now)
            continue
        outbox.send_next(timeout=min(read_timeout, deadline - now))


def run(cfg: Config, client: httpx.Client, clock: Clock, stop: threading.Event) -> tuple[Stats, str]:
    stats = Stats()
    site = Site(cfg.lat, cfg.lon, cfg.elevation_m, cfg.tz_offset_h, cfg.prevailing_wind_deg)
    generator = ReadingGenerator(cfg.station_id, cfg.interval_s, cfg.seed, site)
    injector = FaultInjector(cfg.fault_rate, cfg.seed)
    sender = Sender(client, cfg.ingest_url, cfg.station_key)
    outbox = Outbox(sender, cfg.station_id, cfg.buffer_max, stats, clock.monotonic)
    schedule = Schedule(cfg, clock.time(), clock.monotonic())
    logger.info(
        "station_started",
        extra={
            "event": "station_started",
            "station_id": cfg.station_id,
            "config": {
                "ingest_url": cfg.ingest_url,
                "interval_s": cfg.interval_s,
                "seed": cfg.seed,
                "replay_readings": schedule.replay_count,
                "replay_duration_min": cfg.replay_duration_min,
                "fault_rate": cfg.fault_rate,
                "buffer_max": cfg.buffer_max,
                "max_readings": cfg.max_readings,
            },
        },
    )

    exit_reason = "signal"
    while not stop.is_set():
        done_generating = bool(cfg.max_readings) and stats.generated >= cfg.max_readings
        if done_generating and not len(outbox):
            # Con MAX_READINGS se termina al entregar (o descartar) todo, no al generar la última.
            exit_reason = "max_readings"
            break
        now = clock.monotonic()
        wall = clock.time()
        waits: list[float] = []
        if not done_generating:
            due, ts_epoch = schedule.peek()
            # Contrapresión solo en la réplica: su lectura siguiente espera a que el búfer se
            # vacíe; si no, una réplica rápida llenaría el búfer y desbordaría las más viejas.
            # Las lecturas en vivo no esperan: dependen del reloj.
            replay_blocked = schedule.in_replay and len(outbox) > 0
            if not replay_blocked:
                if now >= due and wall >= ts_epoch:
                    ts = datetime.fromtimestamp(ts_epoch, UTC)
                    payload = generator.next(ts)
                    stats.generated += 1
                    now_utc = datetime.fromtimestamp(wall, UTC)
                    payload, fault = injector.maybe_inject(payload, now_utc)
                    if fault is not None:
                        stats.faults_injected += 1
                        logger.info(
                            "fault_injected",
                            extra={
                                "event": "fault_injected",
                                "station_id": cfg.station_id,
                                "sequence": payload["sequence"],
                                "fault": fault.name,
                            },
                        )
                    outbox.put(Pending(payload, fault))
                    schedule.advance()
                    continue
                if now >= due:  # toca por el reloj monotónico, pero el de pared va atrás
                    waits.append(max(_WALL_SKEW_WAIT_S, ts_epoch - wall))
                else:
                    waits.append(due - now)
        if len(outbox):
            if now >= outbox.retry_at:
                outbox.send_next()
                continue
            waits.append(outbox.retry_at - now)
        clock.wait(max(0.0, min(waits)))

    _drain(outbox, clock, cfg.http_read_timeout_s)
    stats.unsent_at_exit = len(outbox)
    if len(outbox):
        logger.warning(
            "unsent_at_exit",
            extra={
                "event": "unsent_at_exit",
                "station_id": cfg.station_id,
                "buffered": len(outbox),
                "dropped_sequence": outbox.sequences()[:50],
            },
        )
    return stats, exit_reason


def install_signal_handlers(stop: threading.Event) -> None:
    def handler(signum: int, frame: Any) -> None:
        stop.set()  # nada de logging aquí: no es seguro dentro de un manejador de señal

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is not None:
            signal.signal(sig, handler)


def main(env: Mapping[str, str] | None = None) -> int:
    configure_logging()
    try:
        cfg = load_config(os.environ if env is None else env)
    except ConfigError as exc:
        logger.error(str(exc), extra={"event": "config_error", "reason": "invalid_config"})
        return 2

    stop = threading.Event()
    install_signal_handlers(stop)
    timeout = httpx.Timeout(cfg.http_read_timeout_s, connect=cfg.http_connect_timeout_s)
    try:
        with httpx.Client(timeout=timeout) as client:
            stats, exit_reason = run(cfg, client, SystemClock(stop), stop)
    except Exception as exc:  # noqa: BLE001 — se registra solo el tipo, nunca el mensaje
        logger.error(
            "station_crashed", extra={"event": "station_crashed", "error_type": type(exc).__name__}
        )
        return 1
    logger.info(
        "summary",
        extra={
            "event": "summary",
            "station_id": cfg.station_id,
            "reason": exit_reason,
            "summary": stats.as_dict(),
        },
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
```
