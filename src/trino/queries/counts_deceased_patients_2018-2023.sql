WITH onkostar_data AS (
  SELECT
    o1.deathyear AS "Year of Death",
    COUNT(*) AS "ONKOSTAR Death Count (1)"
  FROM
    (
      SELECT
        DISTINCT lme.lkr_meldung,
        regexp_extract(
          lme.xml_daten,
          '<Patienten_Stammdaten Patient_ID="(.*?)"',
          1
        ) AS pid,
        lme.versionsnummer,
        CAST(
          substr(
            regexp_extract(
              lme.xml_daten,
              '<Sterbedatum>(.*?)</Sterbedatum>',
              1
            ),
            7,
            4
          ) AS integer
        ) AS deathyear
      FROM
        dwh.DWH_ROUTINE.STG_ONKOSTAR_LKR_MELDUNG_EXPORT lme
      WHERE
        lme.xml_daten LIKE '%ICD_Version%'
        AND CAST(
          regexp_extract(
            lme.xml_daten,
            '<Sterbedatum>([0-9]{2})\.([0-9]{2})\.([0-9]{4})</Sterbedatum>',
            3
          ) AS integer
        ) BETWEEN 2018
        AND 2023
        AND lme.typ <> -1
        AND lme.versionsnummer IS NOT NULL
        AND lme.xml_daten NOT LIKE '%<Menge_Tumorkonferenz%'
        AND regexp_like(
          regexp_extract(xml_daten, '<Sterbedatum>(.*?)</Sterbedatum>', 1),
          '(([0-2]\d)|(3[01]))\.((0\d)|(1[0-2]))\.(18|19|20)\d\d'
        ) = true
        AND (
          regexp_extract(xml_daten, '<Meldeanlass>(.*?)</Meldeanlass>', 1) = 'tod'
        )
    ) o1
    LEFT OUTER JOIN (
      SELECT
        DISTINCT regexp_extract(
          lme.xml_daten,
          '<Patienten_Stammdaten Patient_ID="(.*?)"',
          1
        ) AS pid,
        MAX(lme.versionsnummer) AS max_version
      FROM
        dwh.DWH_ROUTINE.STG_ONKOSTAR_LKR_MELDUNG_EXPORT lme
      WHERE
        lme.xml_daten LIKE '%ICD_Version%'
        AND CAST(
          regexp_extract(
            lme.xml_daten,
            '<Sterbedatum>([0-9]{2})\.([0-9]{2})\.([0]{4})</Sterbedatum>',
            3
          ) AS integer
        ) BETWEEN 2018
        AND 2023
        AND regexp_like(
          regexp_extract(xml_daten, '<Sterbedatum>(.*?)</Sterbedatum>', 1),
          '(([0-2]\d)|(3[01]))\.((0\d)|(1[0-2]))\.(18|19|20)\d\d'
        ) = true
        AND lme.typ <> -1
        AND lme.versionsnummer IS NOT NULL
        AND lme.xml_daten NOT LIKE '%<Menge_Tumorkonferenz%'
        AND (
          regexp_extract(xml_daten, '<Meldeanlass>(.*?)</Meldeanlass>', 1) = 'tod'
        )
      GROUP BY
        regexp_extract(
          lme.xml_daten,
          '<Patienten_Stammdaten Patient_ID="(.*?)"',
          1
        )
    ) o2 ON o1.pid = o2.pid
    AND o1.versionsnummer < o2.max_version
  WHERE
    o1.deathyear BETWEEN 2018
    AND 2023
    AND o2.pid IS NULL
  GROUP BY
    o1.deathyear
),
fhir_data AS (
  SELECT
    CAST(YEAR(DATE(deceasedDateTime)) AS INTEGER) AS "Year of Death",
    COUNT(DISTINCT id) AS "FHIR Death Count (2)"
  FROM
    fhir.obds_qs.Patient
  WHERE
    deceasedDateTime IS NOT NULL
  GROUP BY
    YEAR(DATE(deceasedDateTime))
  HAVING
    YEAR(DATE(deceasedDateTime)) BETWEEN 2018
    AND 2023
),
csv_data AS (
  SELECT
    (
      CASE
        WHEN (
          deceased_datetime IS NOT NULL
          AND deceased_datetime <> ''
        ) THEN substr(deceased_datetime, 1, 4)
        ELSE ''
      END
    ) AS "Year of Death",
    COUNT(DISTINCT pat_id) AS "CSV Death Count (3)"
  FROM
    storage.csv."s3a://bzkf-obds-dq/df_patid.csv"
  WHERE
    deceased_datetime IS NOT NULL
    AND deceased_datetime <> ''
    AND CAST(substr(deceased_datetime, 1, 4) AS INTEGER) BETWEEN 2018
    AND 2023
  GROUP BY
    (
      CASE
        WHEN (
          deceased_datetime IS NOT NULL
          AND deceased_datetime <> ''
        ) THEN substr(deceased_datetime, 1, 4)
        ELSE ''
      END
    )
  ORDER BY
    (
      CASE
        WHEN (
          deceased_datetime IS NOT NULL
          AND deceased_datetime <> ''
        ) THEN substr(deceased_datetime, 1, 4)
        ELSE ''
      END
    )
)
SELECT
  COALESCE(
    o."Year of Death",
    f."Year of Death",
    CAST(c."Year of Death" AS INTEGER)
  ) AS "Year of Death",
  o."ONKOSTAR Death Count (1)",
  f."FHIR Death Count (2)",
  COALESCE(o."Onkostar Death Count (1)", 0) - COALESCE(f."FHIR Death Count (2)", 0) AS "Absolute Difference (1) - (2)",
  CASE
    WHEN COALESCE(o."Onkostar Death Count (1)", 0) != 0 THEN ROUND(
      ABS(
        COALESCE(o."Onkostar Death Count (1)", 0) - (COALESCE(f."FHIR Death Count (2)", 0))
      ) / CAST(
        COALESCE(o."Onkostar Death Count (1)", 0) AS DOUBLE
      ) * 100,
      2
    )
    ELSE NULL
  END AS "Relative Difference (1) - (2) %",
  c."CSV Death Count (3)",
  ABS(
    COALESCE(f."FHIR Death Count (2)", 0) - COALESCE(c."CSV Death Count (3)", 0)
  ) AS "Absolute Difference (2) - (3)"
FROM
  onkostar_data o FULL
  OUTER JOIN fhir_data f ON o."Year of Death" = f."Year of Death" FULL
  OUTER JOIN csv_data c ON COALESCE(
    CAST(o."Year of Death" AS INTEGER),
    CAST(f."Year of Death" AS INTEGER)
  ) = CAST(c."Year of Death" AS INTEGER)
ORDER BY
  "Year of Death";
