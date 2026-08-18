# provided by Johannes Meier, UKR
# extended by Jasmin Ziegler

from functools import reduce

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from lifelines import KaplanMeierFitter
from lifelines.plotting import add_at_risk_counts
from lifelines.statistics import logrank_test
from matplotlib.patches import FancyBboxPatch


def bonferroni_correction(p_value, number_tests, alpha=[0.05, 0.01, 0.001]):
    """
    Calculates the Bonferroni correction for multiple testing and returns the appropriate symbol and text based on the p-value.

    Parameters:
    - p_value: The p-value to be evaluated.
    - number_tests: The total number of tests being performed.
    - alpha: A list of significance levels for which to calculate the Bonferroni correction (default is [0.05, 0.01, 0.001]).
    """

    alpha_bonferroni = [a / number_tests for a in alpha]

    if p_value < alpha_bonferroni[2]:
        symbol = "***"
        text = f"{symbol} (Bonferroni corrected alpha = {alpha_bonferroni[2]:.4f})"
    elif p_value < alpha_bonferroni[1]:
        symbol = "**"
        text = f"{symbol} (Bonferroni corrected alpha = {alpha_bonferroni[1]:.4f})"
    elif p_value < alpha_bonferroni[0]:
        symbol = "*"
        text = f"{symbol} (Bonferroni corrected alpha = {alpha_bonferroni[0]:.4f})"
    else:
        symbol = "n.s"
        text = f"{symbol} (Bonferroni corrected alpha = {alpha_bonferroni[0]:.4f})"

    return text, symbol


def km_lifeline_logRank(data, event_col, time_col, cat_col, cat_names):
    """
    Perform the log-rank test for survival analysis using lifelines.

    Parameters:
    - data: DataFrame containing the survival data.
    - event_col: Column name for the event/censoring indicator.
    - time_col: Column name for the time-to-event data.
    - cat_col: Column name for the categorical variable.
    - cat_names: List of tuples with category names and labels.

    """

    anzahl_tests = len(cat_names) * (len(cat_names) - 1) // 2

    # Perform log-rank test
    results = []
    for i in range(len(cat_names)):
        for j in range(i + 1, len(cat_names)):
            if len(cat_names[i]) == 2:
                # if cat_names[i] is a tuple with category and name
                group1 = data[data[cat_col] == cat_names[i][0]]
                group2 = data[data[cat_col] == cat_names[j][0]]
                cat_no = 1
            elif len(cat_names[i]) == 3:
                # if cat_names[i] is a tuple with lower, upper and name
                group1 = data[
                    (data[cat_col] > cat_names[i][0]) & (data[cat_col] <= cat_names[i][1])
                ]
                group2 = data[
                    (data[cat_col] > cat_names[j][0]) & (data[cat_col] <= cat_names[j][1])
                ]
                cat_no = 2
            else:
                raise ValueError(
                    "cat_names must be a list of tuples with category and name or lower, upper and name."
                )

            r = logrank_test(
                group1[time_col],
                group2[time_col],
                event_observed_A=group1[event_col],
                event_observed_B=group2[event_col],
            )

            text, symbol = bonferroni_correction(r.p_value, anzahl_tests)

            print(
                f"'{cat_names[i][cat_no]}' vs. '{cat_names[j][cat_no]}' "
                f"(Log-rank): p-value = {r.p_value:.4f} - {text}"
            )
            results.append(r.p_value)

    return results, symbol


def add_survival_markers(kmf, ax, times=None, color=None):
    if times is None:
        times = [60, 120]

    surv = kmf.survival_function_

    for t in times:
        # letzte bekannte Zeit <= t
        idx = surv.index[surv.index <= t].max()

        y = surv.loc[idx].values[0]

        ax.scatter(t, y, color=color, zorder=5)
        ax.axvline(t, linestyle="--", linewidth=0.7, alpha=0.3)


def km_lifeline(
    data,
    event_col,
    time_col,
    cat_col=None,
    cat_names=None,
    fig_size=(8, 5),
    xl="Survival time [months]",
    yl="Survival probability",
    title=None,
    stat=False,
    show_stat=False,
    censors=False,
    ci=True,
    cmap="tab20b",
    colors=None,
    ylim=None,
    xlim=None,  # für 5y/10y "zoom" bzw Achse abschneiden
    ax=None,
    show_plot=True,
):

    if ax is None:
        fig, ax = plt.subplots(figsize=fig_size)

    kmfs = []

    total_n = len(data)

    color_list = None

    if cat_names is not None:
        n = len(cat_names)

        if colors is not None:
            color_list = list(colors) if isinstance(colors, (list, tuple)) else [colors] * n

        elif cmap is not None:
            cmap_obj = plt.get_cmap(cmap)
            color_list = [cmap_obj(i / max(n - 1, 1)) for i in range(n)]

    # no groups
    if cat_col is None:
        kmf = KaplanMeierFitter()

        kmf.fit(
            durations=data[time_col],
            event_observed=data[event_col],
            label=f"Overall n={len(data)}",
        )

        color = None
        if colors is not None:
            color = colors[0] if isinstance(colors, (list, tuple)) else colors

        elif cmap is not None:
            cmap_obj = plt.get_cmap(cmap)
            color = cmap_obj(0.0)

        kmf.plot_survival_function(
            ax=ax,
            show_censors=censors,
            ci_show=ci,
            color=color,
        )
        add_survival_markers(kmf, ax=ax, color=color)

        kmfs.append(kmf)
        # Median ausgeben
        med = kmf.median_survival_time_
        print(
            f"[KM Median] Overall: {f'{med:.1f} months' if med != float('inf') else 'Not reached (inf)'}"
        )
        for label, m in [("1-Year OS", 12), ("3-Year OS", 36), ("5-Year OS", 60)]:
            if m <= kmf.timeline.max():
                rate = kmf.survival_function_at_times(m).values[0]
                print(f"  {label}: {rate * 100:.1f}%")
            else:
                print(f"  {label}: Not enough data")
        print("========================================\n")

    # grouped km
    else:
        if cat_names is None:
            raise ValueError("cat_names must be provided when cat_col is used.")

        if len(cat_names[0]) not in [2, 3]:
            raise ValueError("cat_names must contain tuples of length 2 or 3.")

        for idx, group in enumerate(cat_names):
            # categorical grouping
            if len(group) == 2:
                value, label = group

                mask = data[cat_col] == value

            # interval grouping
            else:
                lower, upper, label = group

                mask = (data[cat_col] > lower) & (data[cat_col] <= upper)

            subset = data.loc[mask]

            # skip empty groups
            if len(subset) == 0:
                print(f"Skipping empty group: {label}")
                continue

            kmf = KaplanMeierFitter()

            kmf.fit(
                durations=subset[time_col],
                event_observed=subset[event_col],
                label=f"{label} n={len(subset)} ({len(subset) / total_n:.1%})",
            )

            color = color_list[idx] if color_list is not None else None

            kmf.plot_survival_function(
                ax=ax,
                show_censors=censors,
                ci_show=ci,
                color=color,
            )
            add_survival_markers(kmf, ax=ax, color=color)

            kmfs.append(kmf)

            # Median OS ausgeben
            med = kmf.median_survival_time_
            print(
                f"[KM Median] {label}: {f'{med:.1f} months' if med != float('inf') else 'Not reached (inf)'}"
            )
            for label, m in [("1-Year OS", 12), ("3-Year OS", 36), ("5-Year OS", 60)]:
                if m <= kmf.timeline.max():
                    rate = kmf.survival_function_at_times(m).values[0]
                    print(f"  {label}: {rate * 100:.1f}%")
                else:
                    print(f"  {label}: Not enough data")
            print("========================================\n")

    # ----------------------------------------
    # LOG-RANK TEST
    # ----------------------------------------
    if stat and cat_col is not None:
        results, symbol = km_lifeline_logRank(
            data=data,
            event_col=event_col,
            time_col=time_col,
            cat_col=cat_col,
            cat_names=cat_names,
        )

        # show only if exactly 2 groups
        if show_stat and len(cat_names) == 2:
            ax.text(
                0.95,
                0.85,
                f"Log-rank: {symbol}\np = {results[0]:.4e}",
                transform=ax.transAxes,
                fontsize=9,
                verticalalignment="top",
                horizontalalignment="right",
                bbox=dict(
                    facecolor="white",
                    alpha=0.7,
                    edgecolor="none",
                ),
            )

    ax.set_xlabel(xl)
    ax.set_ylabel(yl)

    if xlim is not None:
        ax.set_xlim(xlim)

    if ylim is not None:
        ax.set_ylim(ylim)
    else:
        ax.set_ylim(0, 1.02)

    ax.grid(
        True,
        which="both",
        linestyle="--",
        linewidth=0.5,
    )

    if title is not None:
        ax.set_title(title)

    if cat_col is not None:
        ax.legend(loc="best")

    # risk table
    if len(kmfs) > 0:
        add_at_risk_counts(*kmfs, ax=ax)

    if show_plot:
        plt.show()

    # return ax


def median_iqr(x):
    x = pd.Series(x).dropna()

    return {
        "n": len(x),
        "median": x.median(),
        "q1": x.quantile(0.25),
        "q3": x.quantile(0.75),
        "iqr": x.quantile(0.75) - x.quantile(0.25),
    }


def plot_survival_cohort(
    summary,
    asserted_year=None,
    title="survival cohort",
    left_steps=[],
    right_steps=[],
    center_steps=[],
    cohort_control=True,
):

    def get(step):
        try:
            return int(summary.loc[step, "count"])
        except KeyError:
            return 0

    COLORS = {
        "left": {"fc": "#FFF0E0", "ec": "#E67E22"},
        "right": {"fc": "#E8F0F8", "ec": "#2E5B8B"},
        "join": {"fc": "#F5F5F5", "ec": "#555555"},
        "center": {"fc": "#F5F5F5", "ec": "#555555"},
        "exclude": {"fc": "#FEF9EC", "ec": "#B8860B"},
        "final": {"fc": "#EAF4EA", "ec": "#2E7D32"},
    }

    def box_color(step):
        s = step.lower()

        if s.startswith("excluded") or "discordant" in s:
            return "exclude"

        if "kaplan" in s or "eligible" in s:
            return "final"

        if "deceased patients (event=1)" in s:
            return "final"

        if "censored patients (event=0)" in s:
            return "final"

        return "center"

    def draw_box(ax, cx, cy, text, style="center", width=0.28, height=0.055):
        c = COLORS[style] if style in COLORS else box_color(text)
        patch = FancyBboxPatch(
            (cx - width / 2, cy - height / 2),
            width,
            height,
            boxstyle="round,pad=0.01",
            fc=c["fc"],
            ec=c["ec"],
            linewidth=1.2,
            transform=ax.transAxes,
            clip_on=False,
        )
        ax.add_patch(patch)
        ax.text(
            cx,
            cy,
            text,
            ha="center",
            va="center",
            fontsize=8.5,
            linespacing=1.4,
            transform=ax.transAxes,
            clip_on=False,
        )

    def arrow(ax, x1, y1, x2, y2, color="#555555"):
        ax.annotate(
            "",
            xy=(x2, y2),
            xytext=(x1, y1),
            xycoords="axes fraction",
            textcoords="axes fraction",
            arrowprops=dict(
                arrowstyle="->, head_width=0.3, head_length=0.3",
                color=color,
                lw=1.2,
            ),
        )

    def elbow_arrow(ax, x1, y1, x2, y2, color="#555555"):
        ax.annotate(
            "",
            xy=(x2, y2),
            xytext=(x1, y1),
            xycoords="axes fraction",
            textcoords="axes fraction",
            arrowprops=dict(
                arrowstyle="->, head_width=0.3, head_length=0.3",
                color=color,
                lw=1.2,
                connectionstyle="angle,angleA=0,angleB=90,rad=3",
            ),
        )

    X_LEFT, X_CENTER, X_RIGHT = 0.15, 0.50, 0.85
    BOX_W_SIDE, BOX_W_CENTER, BOX_H = 0.26, 0.38, 0.055
    Y_TOP, Y_STEP = 0.93, 0.082

    y_left = [Y_TOP - i * Y_STEP for i in range(len(left_steps))]
    y_right = [Y_TOP - i * Y_STEP for i in range(len(right_steps))]
    y_join = y_left[-1]
    y_center = [y_join - (i + 1) * Y_STEP for i in range(len(center_steps) - 1)]
    y_center[-1] = y_center[-2] - Y_STEP  # extra gap before KM cohort

    fig_h = max(10, (Y_TOP - y_center[-1] + 0.12) * 14)
    fig, ax = plt.subplots(figsize=(15, fig_h))
    ax.set_xlim(0, 1)
    ax.axis("off")

    for i, (step, y) in enumerate(zip(left_steps, y_left)):
        draw_box(ax, X_LEFT, y, f"{step}\nN={get(step):,}", style="left", width=BOX_W_SIDE)
        if i < len(left_steps) - 1:
            arrow(
                ax,
                X_LEFT,
                y - BOX_H / 2,
                X_LEFT,
                y_left[i + 1] + BOX_H / 2,
                color=COLORS["left"]["ec"],
            )

    for i, (step, y) in enumerate(zip(right_steps, y_right)):
        draw_box(ax, X_RIGHT, y, f"{step}\nN={get(step):,}", style="right", width=BOX_W_SIDE)
        if i < len(right_steps) - 1:
            arrow(
                ax,
                X_RIGHT,
                y - BOX_H / 2,
                X_RIGHT,
                y_right[i + 1] + BOX_H / 2,
                color=COLORS["right"]["ec"],
            )

    join_step = center_steps[0]
    draw_box(
        ax, X_CENTER, y_join, f"{join_step}\nN={get(join_step):,}", style="join", width=BOX_W_CENTER
    )

    elbow_arrow(
        ax,
        X_LEFT + BOX_W_SIDE / 2,
        y_left[-1],
        X_CENTER - BOX_W_CENTER / 2,
        y_join,
        color=COLORS["left"]["ec"],
    )

    elbow_arrow(
        ax,
        X_RIGHT - BOX_W_SIDE / 2,
        y_right[-1],
        X_CENTER + BOX_W_CENTER / 2,
        y_join,
        color=COLORS["right"]["ec"],
    )

    prev_y = y_join
    for i, (step, y) in enumerate(zip(center_steps[1:], y_center)):
        c = box_color(step)
        draw_box(
            ax,
            X_CENTER,
            y,
            f"{step}\nN={get(step):,}",
            style=c if isinstance(c, str) else "center",
            width=BOX_W_CENTER,
        )
        if not isinstance(c, str):
            pass
        arrow(ax, X_CENTER, prev_y - BOX_H / 2, X_CENTER, y + BOX_H / 2)
        prev_y = y

    y_km = y_center[-1]

    y_event = y_km - Y_STEP * 1.2
    y_cens = y_km - Y_STEP * 1.2

    draw_box(
        ax,
        X_CENTER - 0.22,
        y_event,
        f"Deceased patients (event=1)\nN={get('Deceased patients (event=1)'):,}",
        style="final",
        width=0.32,
    )

    draw_box(
        ax,
        X_CENTER + 0.22,
        y_cens,
        f"Censored patients (event=0)\nN={get('Censored patients (event=0) with follow-up information'):,}",
        style="final",
        width=0.32,
    )

    arrow(
        ax,
        X_CENTER,
        y_km - BOX_H / 2,
        X_CENTER - 0.22,
        y_event + BOX_H / 2,
    )

    arrow(
        ax,
        X_CENTER,
        y_km - BOX_H / 2,
        X_CENTER + 0.22,
        y_cens + BOX_H / 2,
    )

    # second level split
    y_sub = y_event - Y_STEP * 1.1

    if cohort_control:
        # deceased -> cohort/rest
        draw_box(
            ax,
            X_CENTER - 0.32,
            y_sub,
            f"Cohort\nN={get('Deceased patients: cohort'):,}",
            style="final",
            width=0.18,
        )

        draw_box(
            ax,
            X_CENTER - 0.12,
            y_sub,
            f"Control\nN={get('Deceased patients: control'):,}",
            style="final",
            width=0.18,
        )

        arrow(
            ax,
            X_CENTER - 0.22,
            y_event - BOX_H / 2,
            X_CENTER - 0.32,
            y_sub + BOX_H / 2,
        )

        arrow(
            ax,
            X_CENTER - 0.22,
            y_event - BOX_H / 2,
            X_CENTER - 0.12,
            y_sub + BOX_H / 2,
        )

        # censored -> cohort/rest
        draw_box(
            ax,
            X_CENTER + 0.12,
            y_sub,
            f"Cohort\nN={get('Censored patients: cohort'):,}",
            style="final",
            width=0.18,
        )

        draw_box(
            ax,
            X_CENTER + 0.32,
            y_sub,
            f"Control\nN={get('Censored patients: control'):,}",
            style="final",
            width=0.18,
        )

        arrow(
            ax,
            X_CENTER + 0.22,
            y_cens - BOX_H / 2,
            X_CENTER + 0.12,
            y_sub + BOX_H / 2,
        )

        arrow(
            ax,
            X_CENTER + 0.22,
            y_cens - BOX_H / 2,
            X_CENTER + 0.32,
            y_sub + BOX_H / 2,
        )

    lowest_y = min(y_event, y_cens, y_sub)
    ax.set_ylim(lowest_y - 0.15, 1)

    # legend
    ax.set_title(title, fontsize=13, fontweight="bold", pad=12)
    legend_patches = [
        mpatches.Patch(fc=COLORS["left"]["fc"], ec=COLORS["left"]["ec"], label="Tumor records"),
        mpatches.Patch(
            fc=COLORS["right"]["fc"], ec=COLORS["right"]["ec"], label="Vital status records"
        ),
        mpatches.Patch(
            fc=COLORS["exclude"]["fc"], ec=COLORS["exclude"]["ec"], label="Exclusion step"
        ),
        mpatches.Patch(fc=COLORS["final"]["fc"], ec=COLORS["final"]["ec"], label="Final cohort"),
    ]
    ax.legend(
        handles=legend_patches,
        loc="upper center",
        ncol=5,
        fontsize=8,
        framealpha=0.8,
        bbox_to_anchor=(0.5, y_sub - 0.08),
    )

    plt.tight_layout()
    plt.savefig("consort_flow_plot.png", dpi=150, bbox_inches="tight")
    plt.show()
    print("y_km   =", y_km)
    print("y_event=", y_event)
    print("y_sub  =", y_sub)


def latest_per_condition(df, date_col, output_col):
    df = df.dropna(subset=[date_col])

    return df.loc[df.groupby("condition_id_hash")[date_col].idxmax()][
        ["condition_id_hash", date_col]
    ].rename(columns={date_col: output_col})


def load_and_clean_base_data(
    df_obds: pd.DataFrame, asserted_min: int, asserted_max: int = None, target_entities: list = None
):
    """
    Filtert nach Diagnosejahren, entfernt Duplikate und extrahiert den ersten Tumor pro Patient.
    Filtert target_entities falls vorhanden.
    Voraussetzung: Studienspezifische Filter (Alter, ICD10, Spalten) wurden bereits angewendet.
    """
    df_clean = df_obds.copy()

    df_clean = df_clean[df_clean["asserted_year"] >= asserted_min]
    if asserted_max is not None:
        df_clean = df_clean[df_clean["asserted_year"] <= asserted_max]

    df_clean = df_clean.drop_duplicates()

    # Top 20 Entitäten filtern
    if target_entities is not None:
        df_clean = df_clean[df_clean["entity_or_parent"].isin(target_entities)]

    df_first_tumor = (
        df_clean.sort_values("age_at_diagnosis")
        .groupby("patient_resource_id_hash")
        .first()
        .reset_index()
    )

    return df_clean, df_first_tumor


def process_survival_events(df_first_tumor: pd.DataFrame, df_vital: pd.DataFrame):
    """
    Bereinigt den Vitalstatus (nur aktuellster Eintrag, keine negativen Zeiten),
    verknüpft ihn mit der Kohorte und berechnet das Survival-Event und die Zeiten.
    """
    df_v = df_vital.copy().reset_index(drop=True)

    # Nur den aktuellsten Vitalstatus pro Patient (größte Follow-up Zeit)
    idx = (
        df_v.groupby("patient_resource_id_hash")["months_between_asserted_effective_dateTime"]
        .idxmax()
        .dropna()
        .astype(int)
    )
    df_vital_latest = df_v.loc[idx].reset_index(drop=True)

    # Filter auf valide Patienten (aus der Kohorte) & nicht-negative Zeiten
    valid_patients = set(df_first_tumor["patient_resource_id_hash"])
    df_vital_latest = df_vital_latest[
        df_vital_latest["patient_resource_id_hash"].isin(valid_patients)
        & (df_vital_latest["months_between_asserted_effective_dateTime"] >= 0)
    ].copy()

    # Zusammenführen von Tumor-Daten und Vitalstatus
    df_join = df_first_tumor.merge(
        df_vital_latest, on="patient_resource_id_hash", how="left", suffixes=("", "_vital")
    )

    # Event definieren (1 = Tod, 0 = Zensiert, NA = Unbekannt)
    event = pd.Series(pd.NA, index=df_join.index, dtype="boolean")

    # Sicher tot (Event = True)
    event.loc[df_join["is_deceased"].eq(True) | df_join["vitalstatus_code"].eq("T")] = True

    # Sicher lebend / zensiert (Event = False) - nur wo event noch NA ist
    event.loc[
        (
            df_join["vitalstatus_code"].eq("L")
            | (df_join["is_deceased"].eq(False) & df_join["vitalstatus_code"].notna())
        )
        & event.isna()
    ] = False

    df_join["event"] = event

    # Finale Zeit-Spalten für die Survival-Analyse vorbereiten
    df_join["death_time"] = df_join["months_between_asserted_deceased_datetime"].combine_first(
        df_join["months_between_asserted_date_death"]
    )
    df_join["followup_time"] = df_join["months_between_asserted_effective_dateTime"]

    # Wir geben df_vital_latest für den Report zurück, und df_join zum Weiterarbeiten
    return df_vital_latest, df_join


def extend_followup_times(
    df_join: pd.DataFrame, extension_dfs: dict, extension_config: dict, df_tnm_m: pd.DataFrame
):
    """
    Sucht in allen Zusatz-Tabellen nach der neuesten Follow-up-Zeit pro Condition,
    verknüpft diese mit df_join und aktualisiert 'followup_time' und 'event'.
    """
    latest_dfs = {}

    # Neueste Datensätze pro Zusatz-Tabelle berechnen
    for key, cfg in extension_config.items():
        # Wichtig: Bei PCA filterst du Progressions auf C61, das passiert vor dieser Funktion
        latest_dfs[key] = latest_per_condition(
            df=extension_dfs[key],
            date_col=cfg["date_col"],
            output_col=cfg["output_col"],
        )

    # TNM_M gesondert hinzufügen (da es schon im Haupt-Config geladen wurde) - ggf. umstellen iwann
    latest_dfs["tnm_m"] = latest_per_condition(
        df=df_tnm_m,
        date_col="months_between_asserted_m_tnm_date",
        output_col="tnm_m_months",
    )

    # Alle Latest-DFs zusammenführen (Outer Join)
    df_all = reduce(
        lambda left, right: left.merge(right, on="condition_id_hash", how="outer"),
        latest_dfs.values(),
    )

    # Spaltennamen für Follow-ups sammeln
    followup_cols = [cfg["output_col"] for cfg in extension_config.values()] + ["tnm_m_months"]

    # Nur positive Zeiten berücksichtigen und Maximum finden
    df_followup_positive_only = df_all[followup_cols].where(df_all[followup_cols] >= 0)
    df_all["months_followup_extended"] = df_followup_positive_only.max(axis=1)

    # An den Haupt-Datensatz (df_join) mergen
    df_join_extended = df_join.merge(
        df_all[["condition_id_hash", "months_followup_extended"]],
        on="condition_id_hash",
        how="left",
    )

    # Follow-up Zeit und Event updaten
    df_join_extended["followup_time"] = df_join_extended["followup_time"].combine_first(
        df_join_extended["months_followup_extended"]
    )
    # Wenn Event fehlt (NA) aber eine Follow-up Zeit existiert
    # -> Patient lebte noch zu dem Zeitpunkt (Zensiert = 0)
    df_join_extended.loc[
        df_join_extended["event"].isna() & df_join_extended["followup_time"].notna(), "event"
    ] = False

    # Zählen der hinzugefügten Records für den Report
    records_count = sum(df.shape[0] for df in latest_dfs.values())

    return df_join_extended, records_count


def finalize_km_cohort(df_join: pd.DataFrame):
    """
    Berechnet die finale Überlebenszeit, filtert unplausible oder
    diskordante Datensätze und gibt die Kaplan-Meier-Kohorte zurück.
    """
    df = df_join.copy()
    exclusion_report = {}

    # Fehlender Event-Status
    unknown_mask = df["event"].isna()
    exclusion_report["Excluded: unknown event status"] = unknown_mask.sum()
    df = df.loc[~unknown_mask].copy()
    df["event"] = df["event"].astype(int)

    # Finale Überlebenszeit berechnen
    df["survival_time_months"] = np.where(df["event"] == 1, df["death_time"], df["followup_time"])

    # Fehlende Überlebenszeit
    missing_mask = df["survival_time_months"].isna()
    exclusion_report["Excluded: survival time missing"] = missing_mask.sum()
    df = df.loc[~missing_mask].copy()

    # Implausible Zeiten (> 100 Jahre = 1200 Monate)
    outlier_mask = df["survival_time_months"] > 1200
    exclusion_report["Excluded: survival time >100 years"] = outlier_mask.sum()
    df = df.loc[~outlier_mask].copy()

    # Negative Überlebenszeiten (Diagnose nach Tod / fehlerhafte Daten)
    negative_mask = df["survival_time_months"] < 0
    exclusion_report["Excluded: negative survival time"] = negative_mask.sum()
    df = df.loc[~negative_mask].copy()

    # Diskordante Fälle (oBDS sagt tot, Vitalstatus sagt lebend)
    discordant_mask = (
        (df["is_deceased"].eq(True))
        & (df["vitalstatus_code"] == "L")
        & (df["death_time"] < df["followup_time"])
    )
    excl_key = (
        "Excluded: discordant mortality records (vital_status code = L after is_deceased=True)"
    )
    exclusion_report[excl_key] = discordant_mask.sum()

    # Finaler Datensatz
    df_km = df.loc[~discordant_mask].copy()
    exclusion_report["Kaplan-Meier eligible cohort"] = df_km.shape[0]

    return df_km, exclusion_report
