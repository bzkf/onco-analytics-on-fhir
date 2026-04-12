import os
from hashlib import sha256
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from loguru import logger
from settings import settings

HERE = Path(os.path.abspath(os.path.dirname(__file__)))

patients_with_diagnoses = pd.read_csv(HERE / "results" / "aml" / "aml_all_patients.csv")

diagnosis_unique = patients_with_diagnoses.drop_duplicates(
    subset="condition_patient_reference",
    keep="first",  # or "last" if you prefer
)

df = pd.read_csv(
    HERE / "results" / "aml" / "aml_matched_zenzy_labs.csv",
    sep=",",
    dtype={"lab_quantity_value": "float"},
    parse_dates=["lab_dateTime"],
)

df.nunique()

df = df.merge(
    diagnosis_unique,
    left_on="observation_patient_reference",
    right_on="condition_patient_reference",
    how="left",
)

# lab_patients = df["observation_patient_reference"].unique()
# patients_with_diagnoses_with_lab = patients_with_diagnoses[
#     patients_with_diagnoses["condition_patient_reference"].isin(lab_patients)
# ].copy()

zenzy_df = pd.read_csv(settings.aml.csv_input_file, sep=";")
zenzy_df["Applikationszeitpunkt"] = pd.to_datetime(
    zenzy_df["Datum"] + " " + zenzy_df["Zeit"], format="%d.%m.%Y %H:%M", errors="raise"
)

zenzy_df["Applikationszeitpunkt"] = zenzy_df["Applikationszeitpunkt"].apply(
    lambda x: x + pd.Timedelta(hours=1) if x.day == 31 and x.month == 3 and x.hour == 2 else x
)

zenzy_df["Applikationszeitpunkt"] = zenzy_df["Applikationszeitpunkt"].dt.tz_localize(
    "Europe/Berlin", ambiguous="NaT"
)

zenzy_df["patient_mrn"] = zenzy_df["KIS-Patienten-ID"]
zenzy_df["label"] = (
    "Wirkstoff: "
    + zenzy_df["Wirkstoff"].astype(str)
    + " ("
    + "Dosis: "
    + zenzy_df["Dosis"].astype(str)
    + ") "
    + "Protokoll: "
    + zenzy_df["Therapieprotokoll"].astype(str)
    + " "
    + "Applikationsart: "
    + zenzy_df["Applikationsart"].astype(str)
    + " "
    + "Retoure?: "
    + zenzy_df["Retoure"].astype(str)
)

unique_medications = zenzy_df["Wirkstoff"].unique()

# Automatically generate a color palette with as many colors as there are unique medications
color_palette = sns.color_palette("Set1", len(unique_medications))

# Map each medication ('Wirkstoff') to a color from the color palette
color_map = {med: color_palette[i] for i, med in enumerate(unique_medications)}

ecog_statuses = pd.read_csv(
    HERE / "results" / "aml" / "df_obds_ecog_statuses.csv",
    sep=";",
)
ecog_statuses["effective_dateTime"] = pd.to_datetime(
    ecog_statuses["effective_dateTime"], format="mixed"
)

tumor_progression = pd.read_csv(
    HERE / "results" / "aml" / "df_obds_progressions.csv",
    sep=";",
)
tumor_progression["effective_dateTime"] = pd.to_datetime(
    tumor_progression["effective_dateTime"], format="mixed"
)

sns.set_theme(style="whitegrid", rc={"figure.autolayout": True})


plots_folder = HERE / "results" / "aml" / "plots"
os.makedirs(plots_folder, exist_ok=True)


def plot_patient(df_patient, zenzy_patient, patient_mrn, patient_ecog, patient_tumor_progression):
    # Find the first Zenzy treatment date for this patient
    first_zenzy_date = zenzy_patient["Applikationszeitpunkt"].min()
    last_zenzy_date = zenzy_patient["Applikationszeitpunkt"].max()

    # Define the time window (± 60 days around the first Zenzy treatment)
    start_window = first_zenzy_date - pd.Timedelta(days=60)
    end_window = last_zenzy_date + pd.Timedelta(days=60)

    # Filter lab values to include only those within the ± 60 days window
    df_patient_filtered = df_patient[
        (df_patient["lab_dateTime"] >= start_window) & (df_patient["lab_dateTime"] <= end_window)
    ]

    # Get the list of LOINC codes (and keep the order of appearance)
    loinc_codes = df_patient_filtered["loinc_code"].unique()

    n = len(loinc_codes)
    fig, axes = plt.subplots(
        n,
        1,
        sharex=True,
        figsize=(12, 3 * n),
        squeeze=False,
    )
    # Flatten for easier iteration
    axes = axes.ravel()

    # Loop over each LOINC and plot its values
    for ax, loinc in zip(axes, loinc_codes, strict=True):
        sub = df_patient_filtered[df_patient_filtered["loinc_code"] == loinc].copy()
        sub = sub.sort_values("lab_dateTime")

        sns.lineplot(
            data=sub,
            x="lab_dateTime",
            y="lab_quantity_value",
            marker="o",
            ax=ax,
            legend=False,
        )

        loinc_name = sub["loinc_display"].iloc[0]
        ax.set_title(f"{loinc_name} ({loinc})", fontsize=12, loc="left")
        unit = sub["lab_quantity_unit"].dropna().unique()
        unit_txt = unit[0] if len(unit) else ""
        ax.set_ylabel(unit_txt, fontsize=10)

        if zenzy_patient is not None and not zenzy_patient.empty:
            for _, row in zenzy_patient.iterrows():
                ts = row["Applikationszeitpunkt"]
                medication = row["Wirkstoff"]
                color = color_map.get(medication, "gray")
                is_cancelled = row["Retoure"] == "Wahr"

                if is_cancelled:
                    color = "red"

                # Plot vertical line with the same color as the annotation box
                ax.axvline(
                    ts,
                    color=color,  # Use the color that corresponds to the medication
                    linestyle="--",
                    linewidth=1,
                    alpha=0.7,
                )

        # Tidy up the grid
        ax.grid(True, which="both", ls=":", linewidth=0.5)

    # X‑axis label (only on the bottom subplot)
    axes[-1].set_xlabel("Time", fontsize=12)

    for _, row in zenzy_patient.iterrows():
        ts = row["Applikationszeitpunkt"]
        label = row["label"]
        medication = row["Wirkstoff"]
        color = color_map.get(medication, "gray")
        is_cancelled = row["Retoure"] == "Wahr"

        if is_cancelled:
            color = "red"

        axes[0].annotate(
            label,
            xy=(ts, axes[0].get_ylim()[1]),  # anchor at top
            xytext=(0, 15),  # offset upward
            textcoords="offset points",
            rotation=90,
            fontsize=8,
            ha="center",
            va="bottom",
            bbox=dict(
                boxstyle="round,pad=0.3",
                fc=color,
                ec=color,
                alpha=0.8,
            ),
            arrowprops=dict(
                arrowstyle="-",
                color=color,
                lw=0.8,
            ),
        )

    if patient_ecog is not None and not patient_ecog.empty:
        for _, ecog_row in patient_ecog.iterrows():
            ecog_ts = ecog_row[
                "effective_dateTime"
            ]  # Make sure this column exists in the ECOG DataFrame
            ecog_score = ecog_row[
                "ecog_performance_status"
            ]  # Adjust according to your ECOG column name
            ecog_label = f"ECOG: {ecog_score}"

            # Annotate ECOG score
            axes[0].annotate(
                ecog_label,
                xy=(ecog_ts, axes[0].get_ylim()[1]),  # anchor at top
                xytext=(0, 75),  # offset upward (above tumor progression, see below)
                textcoords="offset points",
                rotation=90,
                fontsize=8,
                ha="center",
                va="bottom",
                bbox=dict(
                    boxstyle="round,pad=0.3",
                    fc="lightblue",  # Color of the ECOG box
                    ec="blue",  # Border color for ECOG
                    alpha=0.8,
                ),
                arrowprops=dict(
                    arrowstyle="-",
                    color="blue",  # Color of the arrow
                    lw=0.8,
                ),
            )

    if patient_tumor_progression is not None and not patient_tumor_progression.empty:
        for _, progression_row in patient_tumor_progression.iterrows():
            progression_ts = progression_row[
                "effective_dateTime"
            ]  # Make sure this column exists in the ECOG DataFrame
            progression = progression_row[
                "overall_assessment_of_tumor_regression"
            ]  # Adjust according to your ECOG column name
            progress_label = f"Progress: {progression}"

            # Annotate Progression
            axes[0].annotate(
                progress_label,
                xy=(
                    progression_ts,
                    axes[0].get_ylim()[1],
                ),
                xytext=(0, 25),  # offset upward
                textcoords="offset points",
                rotation=90,
                fontsize=8,
                ha="center",
                va="bottom",
                bbox=dict(
                    boxstyle="round,pad=0.3",
                    fc="lightblue",
                    ec="blue",
                    alpha=0.8,
                ),
                arrowprops=dict(
                    arrowstyle="-",
                    color="blue",
                    lw=0.8,
                ),
            )

    patient_id = sha256(patient_mrn.encode("utf-8")).hexdigest()

    # Overall figure title
    fig.suptitle(f"Laboratory results for {patient_id}", fontsize=14, y=1.02)

    # Rotate dates for readability
    plt.setp(axes[-1].get_xticklabels(), rotation=30, ha="right")

    plt.tight_layout()
    # plt.show()

    filename = f"patient_{patient_id}.pdf"
    out_path = plots_folder / filename
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)  # free memory
    logger.info("Saved {out_path}", out_path=out_path)


first_10_ids = df["patient_mrn"].unique()[:10]

for pid, grp in df.groupby("patient_mrn"):
    if pid in first_10_ids:
        zenzy_patient = zenzy_df[zenzy_df["patient_mrn"] == pid]
        ecog_patient = ecog_statuses[ecog_statuses["patient_mrn"] == pid]
        progress_patient = tumor_progression[tumor_progression["patient_mrn"] == pid]
        plot_patient(grp, zenzy_patient, pid, ecog_patient, progress_patient)
