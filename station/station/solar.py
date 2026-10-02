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
