"""
plot_config.py
==============
Zentrale Konfigurationsdatei für alle Plot-Funktionen.

Alle Plot-Dateien (plots.py, PlotsICDDiagzuAlter.py,
SecondPaper_NebenDiagnosen_Plots.py, data_processing.py)
importieren von hier aus.

JMIR-Anforderungen:
- Hochauflösende PNG/JPG mit minimaler Kompression
- Hochformat, große lesbare Schrift
- 300 DPI / ≥1200 px Breite
"""

from __future__ import annotations

import matplotlib.cm as cm
import matplotlib.pyplot as plt
import numpy as np

# ══════════════════════════════════════════════════════════════════════════════
# HAUPTKONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

PLOT_CONFIG = {
    # Ausgabe
    "dpi": 300,                      # JMIR: hochauflösend (≥300)
    "save_format": "png",           # tiff/png/jpg – verlustarm
    # Schriftgrößen (größer als Matplotlib-Default für bessere Lesbarkeit)
    "font_family": "DejaVu Sans",
    "fontsize_base": 14,             # Achsen-Ticks, Text
    "fontsize_axis_label": 16,       # Achsenbeschriftung
    "fontsize_subplot_title": 16,    # Subplot-Titel
    "fontsize_legend": 13,
    "fontsize_bar_label": 12,        # Zahlen an/neben Balken
    # Zusätzliche Schriftgrößen für Annotationen
    "fontsize_annotation_large": 11, # Untertitel, Gini-Text
    "fontsize_annotation": 10,       # Achsentitel, Kontexttext
    "fontsize_annotation_small": 8,  # Footer, kleine Annotationen
    "fontsize_annotation_tiny": 7,   # Sehr kleine Labels
    # Farbskala – tab20b für ALLES (überschreibt semantische Stage-Farben)
    "colormap": "tab20b",
    # Globale Schalter
    "show_titles": False,            # plt.title/Plottitel global aus (außer Subplot-Titel)
    "show_legend": True,             # Legenden anzeigen (einzeln überschreibbar)
    "show_bar_numbers": True,       # Zahlen über Balken global aus
}


# ══════════════════════════════════════════════════════════════════════════════
# UICC & ECOG KONFIGURATIONEN
# ══════════════════════════════════════════════════════════════════════════════

UICC_MAPPING = {
    "0": "0",
    "0a": "0",
    "0is": "0",
    "I": "I",
    "IA": "I",
    "IA1": "I",
    "IA2": "I",
    "IA3": "I",
    "IB": "I",
    "IB1": "I",
    "IB2": "I",
    "IC": "I",
    "II": "II",
    "IIA": "II",
    "IIA1": "II",
    "IIA2": "II",
    "IIB": "II",
    "IIC": "II",
    "III": "III",
    "IIIA": "III",
    "IIIB": "III",
    "IIIC": "III",
    "IIIC1": "III",
    "IIIC2": "III",
    "IIID": "III",
    "IV": "IV",
    "IVA": "IV",
    "IVA1": "IV",
    "IVB": "IV",
    "IVC": "IV",
}

UICC_ORDER = ["missing", "0", "I", "II", "III", "IV"]
ECOG_ORDER = ["0", "1", "2", "3", "4"]

UICC_GROUPS = {
    "0": {
        "substages": ["0", "0a", "0is"],
        "cmap": "Greys",
        "cmap_range": (0.4, 0.75),
        "header_color": "#555555",
    },
    "I": {
        "substages": ["I", "IA", "IA1", "IA2", "IA3", "IB", "IB1", "IB2", "IC"],
        "cmap": "Blues",
        "cmap_range": (0.35, 0.90),
        "header_color": "#08519c",
    },
    "II": {
        "substages": ["II", "IIA", "IIA1", "IIA2", "IIB", "IIC"],
        "cmap": "Greens",
        "cmap_range": (0.35, 0.90),
        "header_color": "#006d2c",
    },
    "III": {
        "substages": ["III", "IIIA", "IIIB", "IIIC", "IIIC1"],
        "cmap": "Oranges",
        "cmap_range": (0.35, 0.90),
        "header_color": "#a63603",
    },
    "IV": {
        "substages": ["IV", "IVA", "IVB", "IVC"],
        "cmap": "Purples",
        "cmap_range": (0.35, 0.90),
        "header_color": "#54278f",
    },
}

UICC_GROUP_ORDER = ["missing", "0", "I", "II", "III", "IV"]
_DEFAULT_COLORS = ["#4C78A8", "#F58518", "#54A24B", "#B279A2", "#E45756", "#72B7B2"]


# ══════════════════════════════════════════════════════════════════════════════
# HILFSFUNKTIONEN
# ══════════════════════════════════════════════════════════════════════════════

def tab20b_colors(n: int) -> list:
    """
    Liefert n Farben aus der globalen Farbskala (tab20b).

    Args:
        n: Anzahl der gewünschten Farben

    Returns:
        Liste mit n Matplotlib-Farbobjekten
    """
    cmap = cm.get_cmap(PLOT_CONFIG["colormap"])
    if n <= 0:
        return []
    # tab20b hat 20 diskrete Farben; bei mehr → linspace
    if n <= getattr(cmap, "N", 20):
        return [cmap(i / max(getattr(cmap, "N", 20) - 1, 1)) for i in range(n)]
    return [cmap(v) for v in np.linspace(0, 1, n)]


def map_uicc_to_main_stage(val):
    """
    Mappt eine beliebige UICC-(Sub-)Stufe auf die Hauptstufe 0/I/II/III/IV.

    Unbekanntes/U/X/leer → 'missing'.
    Zentrale Funktion für ALLE UICC-Plots.

    Args:
        val: UICC-Wert (beliebiger Typ)

    Returns:
        str: 'missing', '0', 'I', 'II', 'III', oder 'IV'
    """
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "missing"
    s = str(val).strip().upper()
    if s in ("", "MISSING", "U", "UNKNOWN", "X", "NAN"):
        return "missing"
    if s in UICC_MAPPING:
        return UICC_MAPPING[s]
    if s.startswith("0"):
        return "0"
    for prefix in ("IV", "III", "II", "I"):
        if s.startswith(prefix):
            return prefix
    return "missing"


def map_ecog_value(val):
    """
    Normalisiert ECOG auf 0–4; alles andere → 'U'.

    Args:
        val: ECOG-Wert (beliebiger Typ)

    Returns:
        str: '0', '1', '2', '3', '4', oder 'U' (unbekannt)
    """
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return "U"
    s = str(val).strip().upper()
    if s in ("0", "1", "2", "3", "4"):
        return s
    return "U"


def apply_plot_config(cfg: dict | None = None) -> dict:
    """
    Wendet die globale Konfiguration auf matplotlib rcParams an
    und gibt die effektive Konfiguration zurück.

    Args:
        cfg: Optional: Übergeben Sie ein Dict mit Überrides

    Returns:
        dict: Die verwendete Konfiguration
    """
    c = dict(PLOT_CONFIG)
    if cfg:
        c.update(cfg)
    plt.rcParams.update({
        "figure.dpi": c["dpi"],
        "savefig.dpi": c["dpi"],
        "font.family": c["font_family"],
        "font.size": c["fontsize_base"],
        "axes.titlesize": c["fontsize_subplot_title"],
        "axes.labelsize": c["fontsize_axis_label"],
        "xtick.labelsize": c["fontsize_base"],
        "ytick.labelsize": c["fontsize_base"],
        "legend.fontsize": c["fontsize_legend"],
    })
    print("  [PLOT-CONFIG] Verwendete Optionen (zum Teilen mit Kollegen):")
    for k, v in c.items():
        print(f"      {k:<22}: {v}")
    return c
