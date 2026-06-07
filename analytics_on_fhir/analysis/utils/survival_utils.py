# provided by Johannes Meier, UKR
# extended by Jasmin Ziegler

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
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
    ax=None,
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

        kmfs.append(kmf)

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

            kmfs.append(kmf)

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

    return ax


def plot_survival_cohort_pca(
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
            f"Rest\nN={get('Deceased patients: control'):,}",
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
            f"Rest\nN={get('Censored patients: control'):,}",
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
    plt.savefig(f"consort_flow_{asserted_year or 'plot'}.png", dpi=150, bbox_inches="tight")
    plt.show()
    print("y_km   =", y_km)
    print("y_event=", y_event)
    print("y_sub  =", y_sub)
