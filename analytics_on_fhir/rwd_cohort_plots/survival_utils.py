# provided by Johannes Meier, UKR

import matplotlib.pyplot as plt
import pandas as pd
from lifelines import KaplanMeierFitter
from lifelines.plotting import add_at_risk_counts
from lifelines.statistics import logrank_test


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
                f"'{cat_names[i][cat_no]}' vs. '{cat_names[j][cat_no]}' (Log-rank): p-value = {r.p_value:.4f} - {text}"
            )
            results.append(r.p_value)

    # print(results)
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
    cmap=None,
    colors=None,
    ylim=None,
    ax=None,
):
    """
    Perform Kaplan-Meier survival analysis and plot survival curves.

    Parameters
    ----------
    data : pd.DataFrame
        DataFrame containing survival data.
    event_col : str
        Column containing event indicator (1=event, 0=censored).
    time_col : str
        Column containing survival/follow-up time.
    cat_col : str, optional
        Column containing grouping/category variable.
    cat_names : list of tuples, optional
        Either:
            [(value, label), ...]
        or
            [(lower, upper, label), ...]
    stat : bool
        Perform pairwise log-rank tests.
    show_stat : bool
        Show p-value in plot (only for exactly 2 groups).
    """

    if ax is None:
        fig, ax = plt.subplots(figsize=fig_size)

    kmfs = []

    # ----------------------------------------
    # COLORS
    # ----------------------------------------
    color_list = None

    if cat_names is not None:
        n = len(cat_names)

        if colors is not None:
            if isinstance(colors, (list, tuple)):
                color_list = list(colors)
            else:
                color_list = [colors] * n

        elif cmap is not None:
            cmap_obj = plt.get_cmap(cmap)
            color_list = [cmap_obj(i / max(n - 1, 1)) for i in range(n)]

    # ----------------------------------------
    # NO GROUPS -> SINGLE KM
    # ----------------------------------------
    if cat_col is None:
        kmf = KaplanMeierFitter()

        kmf.fit(
            durations=data[time_col],
            event_observed=data[event_col],
            label=f"Overall n={len(data)}",
        )

        color = None
        if colors is not None:
            if isinstance(colors, (list, tuple)):
                color = colors[0]
            else:
                color = colors

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

    # ----------------------------------------
    # GROUPED KM
    # ----------------------------------------
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
                label=f"{label} n={len(subset)}",
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

    # ----------------------------------------
    # AXIS SETTINGS
    # ----------------------------------------
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

    # ----------------------------------------
    # RISK TABLE
    # ----------------------------------------
    if len(kmfs) > 0:
        add_at_risk_counts(*kmfs, ax=ax)

    return ax
