"""Posición del sol y radiación de cielo despejado, sin dependencias externas.

Declinación y ecuación del tiempo: series de Fourier de Spencer (1971), error < 0.0006 rad
y < 0.6 min. Ángulo horario a partir de UTC y longitud. Radiación global de cielo
despejado: modelo de Haurwitz con una corrección lineal por altitud."""

import math
from datetime import UTC, datetime

SOLAR_CONSTANT_WM2 = 1361.0


def _day_angle(ts: datetime) -> float:
    ts = ts.astimezone(UTC)
    day_of_year = ts.timetuple().tm_yday
    hours = ts.hour + ts.minute / 60 + ts.second / 3600
    return 2 * math.pi * (day_of_year - 1 + (hours - 12) / 24) / 365


def declination_rad(ts: datetime) -> float:
    g = _day_angle(ts)
    return (
        0.006918
        - 0.399912 * math.cos(g)
        + 0.070257 * math.sin(g)
        - 0.006758 * math.cos(2 * g)
        + 0.000907 * math.sin(2 * g)
        - 0.002697 * math.cos(3 * g)
        + 0.00148 * math.sin(3 * g)
    )


def equation_of_time_min(ts: datetime) -> float:
    g = _day_angle(ts)
    return 229.18 * (
        0.000075
        + 0.001868 * math.cos(g)
        - 0.032077 * math.sin(g)
        - 0.014615 * math.cos(2 * g)
        - 0.040849 * math.sin(2 * g)
    )


def solar_time_h(ts: datetime, lon: float) -> float:
    """Hora solar verdadera (0–24) en la longitud dada."""
    ts = ts.astimezone(UTC)
    utc_h = ts.hour + ts.minute / 60 + ts.second / 3600
    return (utc_h + lon / 15 + equation_of_time_min(ts) / 60) % 24


def elevation_deg(ts: datetime, lat: float, lon: float) -> float:
    """Elevación del sol sobre el horizonte, en grados (negativa de noche)."""
    decl = declination_rad(ts)
    hour_angle = math.radians(15 * (solar_time_h(ts, lon) - 12))
    phi = math.radians(lat)
    sin_e = math.sin(phi) * math.sin(decl) + math.cos(phi) * math.cos(decl) * math.cos(hour_angle)
    return math.degrees(math.asin(max(-1.0, min(1.0, sin_e))))


def solar_noon_local_h(ts: datetime, lon: float, tz_offset_h: float) -> float:
    """Hora local (reloj con offset fijo) del mediodía solar del día de `ts`."""
    return 12 - lon / 15 - equation_of_time_min(ts) / 60 + tz_offset_h


def half_day_length_h(ts: datetime, lat: float) -> float:
    """Mitad de la duración del día (sol sobre el horizonte), en horas."""
    decl = declination_rad(ts)
    cos_h = -math.tan(math.radians(lat)) * math.tan(decl)
    if cos_h <= -1:
        return 12.0  # sol de medianoche
    if cos_h >= 1:
        return 0.0  # noche polar
    return math.degrees(math.acos(cos_h)) / 15


def clear_sky_ghi(elevation: float, elevation_m: float) -> float:
    """Radiación global horizontal de cielo despejado (W/m²); 0 con el sol bajo el horizonte."""
    if elevation <= 0:
        return 0.0
    cos_z = math.sin(math.radians(elevation))
    # Haurwitz (nivel del mar) + ~6 % por km de altitud (menos masa de aire).
    ghi = 1098.0 * cos_z * math.exp(-0.057 / cos_z)
    return ghi * (1 + 0.06 * max(0.0, elevation_m) / 1000)
