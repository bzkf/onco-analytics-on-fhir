WITH fhir_data AS (
    SELECT
        year AS "Year of Diagnosis",
        sum(count) AS "FHIR Diagnosis Count (2)"
    FROM
        (
            SELECT
                count(*) AS count,
                year(from_iso8601_timestamp(onsetDateTime)) AS year
            FROM
                fhir.obds_qs.Condition c
                LEFT JOIN UNNEST(c.code.coding) AS code_coding ON TRUE
            WHERE
                code_coding.system = 'http://fhir.de/CodeSystem/bfarm/icd-10-gm'
            GROUP BY
                code_coding.code,
                year(from_iso8601_timestamp(onsetDateTime)),
                code_coding.display
        ) AS virtual_table
    GROUP BY
        year
    HAVING
        year BETWEEN 2018
        AND 2023
),
onkostar_data AS (
    SELECT
        o1.diagnosejahr AS "Year of Diagnosis",
        COUNT(*) AS "Onkostar Diagnosis Count (1)"
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
                sha256(
                    CAST(
                        CONCAT(
                            'https://fhir.diz.uk-erlangen.de/identifiers/onkostar-xml-condition-id|',
                            regexp_extract(
                                lme.xml_daten,
                                '<Patienten_Stammdaten Patient_ID="(.*?)"',
                                1
                            ),
                            'condition',
                            regexp_extract(
                                lme.xml_daten,
                                '<Tumorzuordnung Tumor_ID="(.*?)"',
                                1
                            )
                        ) AS VARBINARY
                    )
                ) AS cond_id,
                CAST(
                    regexp_extract(
                        lme.xml_daten,
                        '<Diagnosedatum>([0-9]{2})\.([0-9]{2})\.([0-9]{4})</Diagnosedatum>',
                        3
                    ) AS integer
                ) AS diagnosejahr
            FROM
                dwh.DWH_ROUTINE.STG_ONKOSTAR_LKR_MELDUNG_EXPORT lme
            WHERE
                lme.xml_daten LIKE '%ICD_Version%'
                AND CAST(
                    regexp_extract(
                        lme.xml_daten,
                        '<Diagnosedatum>([0-9]{2})\.([0-9]{2})\.([0-9]{4})</Diagnosedatum>',
                        3
                    ) AS integer
                ) BETWEEN 2018
                AND 2023
                AND lme.typ <> -1
                AND (
                    regexp_extract(
                        lme.xml_daten,
                        '<Meldeanlass>(.*?)</Meldeanlass>',
                        1
                    ) = 'diagnose'
                )
                AND lme.versionsnummer IS NOT NULL
                AND lme.xml_daten NOT LIKE '%<Menge_Tumorkonferenz%'
                AND regexp_extract(
                    lme.xml_daten,
                    '<Patienten_Stammdaten Patient_ID="(.*?)"',
                    1
                ) NOT LIKE 'g%'
        ) o1
        LEFT OUTER JOIN (
            SELECT
                DISTINCT sha256(
                    CAST(
                        CONCAT(
                            'https://fhir.diz.uk-erlangen.de/identifiers/onkostar-xml-condition-id|',
                            regexp_extract(
                                lme.xml_daten,
                                '<Patienten_Stammdaten Patient_ID="(.*?)"',
                                1
                            ),
                            'condition',
                            regexp_extract(lme.xml_daten, ' Tumor_ID="(.*?)"', 1)
                        ) AS VARBINARY
                    )
                ) AS cond_id,
                MAX(lme.versionsnummer) AS max_version
            FROM
                dwh.DWH_ROUTINE.STG_ONKOSTAR_LKR_MELDUNG_EXPORT lme
            WHERE
                lme.xml_daten LIKE '%ICD_Version%'
                AND CAST(
                    regexp_extract(
                        lme.xml_daten,
                        '<Diagnosedatum>([0-9]{2})\.([0-9]{2})\.([0-9]{4})</Diagnosedatum>',
                        3
                    ) AS integer
                ) BETWEEN 2018
                AND 2023
                AND lme.typ <> -1
                AND (
                    regexp_extract(
                        lme.xml_daten,
                        '<Meldeanlass>(.*?)</Meldeanlass>',
                        1
                    ) = 'diagnose'
                )
                AND lme.versionsnummer IS NOT NULL
                AND lme.xml_daten NOT LIKE '%<Menge_Tumorkonferenz%'
                AND regexp_extract(
                    lme.xml_daten,
                    '<Patienten_Stammdaten Patient_ID="(.*?)"',
                    1
                ) NOT LIKE 'g%'
            GROUP BY
                sha256(
                    CAST(
                        CONCAT(
                            'https://fhir.diz.uk-erlangen.de/identifiers/onkostar-xml-condition-id|',
                            regexp_extract(
                                lme.xml_daten,
                                '<Patienten_Stammdaten Patient_ID="(.*?)"',
                                1
                            ),
                            'condition',
                            regexp_extract(lme.xml_daten, ' Tumor_ID="(.*?)"', 1)
                        ) AS VARBINARY
                    )
                )
        ) o2 ON o1.cond_id = o2.cond_id
        AND o1.versionsnummer < o2.max_version
    WHERE
        o1.diagnosejahr BETWEEN 2018
        AND 2023
        AND o2.cond_id IS NULL
    GROUP BY
        o1.diagnosejahr
),
csv_data AS (
    SELECT
        CAST(date_diagnosis_year AS INTEGER) AS "Year of Diagnosis",
        COUNT(*) AS "CSV Diagnosis Count (3)"
    FROM
        storage.csv."s3a://bzkf-obds-dq/df.csv"
    WHERE
        CAST(date_diagnosis_year AS INTEGER) BETWEEN 2018
        AND 2023
    GROUP BY
        date_diagnosis_year
)
SELECT
    COALESCE(
        onkostar_data."Year of Diagnosis",
        fhir_data."Year of Diagnosis",
        csv_data."Year of Diagnosis"
    ) AS "Year of Diagnosis",
    COALESCE(onkostar_data."Onkostar Diagnosis Count (1)", 0) AS "Onkostar Diagnosis Count (1)",
    COALESCE(fhir_data."FHIR Diagnosis Count (2)", 0) AS "FHIR Diagnosis Count (2)",
    COALESCE(onkostar_data."Onkostar Diagnosis Count (1)", 0) - COALESCE(fhir_data."FHIR Diagnosis Count (2)", 0) AS "Absolute Difference (1) - (2)",
    CASE
        WHEN COALESCE(onkostar_data."Onkostar Diagnosis Count (1)", 0) != 0 THEN ROUND(
            ABS(
                COALESCE(onkostar_data."Onkostar Diagnosis Count (1)", 0) - (
                    COALESCE(fhir_data."FHIR Diagnosis Count (2)", 0)
                )
            ) / CAST(
                COALESCE(onkostar_data."Onkostar Diagnosis Count (1)", 0) AS DOUBLE
            ) * 100,
            2
        )
        ELSE NULL
    END AS "Relative Difference (1) - (2) %",
    COALESCE(csv_data."CSV Diagnosis Count (3)", 0) AS "CSV Diagnosis Count (3)",
    ABS(
        COALESCE(fhir_data."FHIR Diagnosis Count (2)", 0) - COALESCE(csv_data."CSV Diagnosis Count (3)", 0)
    ) AS "Absolute Difference (2) - (3)"
FROM
    fhir_data FULL
    OUTER JOIN onkostar_data ON fhir_data."Year of Diagnosis" = onkostar_data."Year of Diagnosis" FULL
    OUTER JOIN csv_data ON COALESCE(
        fhir_data."Year of Diagnosis",
        onkostar_data."Year of Diagnosis"
    ) = csv_data."Year of Diagnosis"
ORDER BY
    "Year of Diagnosis";
