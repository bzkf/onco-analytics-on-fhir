import json
import os
import time
import regex
import xml.etree.ElementTree as ET
from io import BytesIO

from confluent_kafka import Producer
from pydantic import BaseSettings


class Settings(BaseSettings):
    input_folder: str = "./input-obds-reports"
    output_folder: str = "./output-obds-reports"
    output_folder_xml: str = "./output-obds-reports/output-xmls"
    save_as_files_enabled: bool = True
    kafka_enabled: bool = False
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_output_topic: str = "obds.einzelmeldungen"


def conditional_folder_create(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)


def save_xml_files(meldung_root, patient_id, meldung_id):
    settings = Settings()
    ET.register_namespace("", "http://www.gekid.de/namespace")
    tree = ET.ElementTree(meldung_root)
    ET.indent(tree, "  ")
    # ET.ElementTree(meldung_root)

    conditional_folder_create(settings.output_folder_xml)
    tree.write(
        f"{settings.output_folder_xml}/patient_{patient_id}"
        f"_meldung_{meldung_id}.xml",
        encoding="UTF-8",
        xml_declaration=True,
    )


def kafka_delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()}@{msg.partition()}")


def remove_leading_zeros(patient_id: str) -> str:
    if os.environ.get('REMOVE_LEADING_PATIENTID_ZEROS') == 'true':
        return regex.sub(r'^0+', '', patient_id)
    else:
        return patient_id


def decompose_sammelmeldung(root: ET.Element, filename: str):
    results = []
    settings = Settings()
    kafka_producer: Producer = None
    if settings.kafka_enabled:
        kafka_producer = Producer(
            {"bootstrap.servers": settings.kafka_bootstrap_servers}
        )
    # Get all "Patient" elements, save the absender to be appended to each new file
    patients = root.findall(".//{http://www.gekid.de/namespace}Patient")
    absender = root.find("./{http://www.gekid.de/namespace}Absender")

    if absender is None:
        print(f"Absender is not defined in the input file {filename}. Stopping.")
        return []

    # Loop through each patient
    for patient in [p for p in patients if p is not None]:
        if (
            patient_stammdaten := patient.find(
                ".//{http://www.gekid.de/namespace}Patienten_Stammdaten"
            )
        ) is None:
            print(f"Patienten_Stammdaten is unset for {filename}. Skipping.")
            continue

        patient_id = patient_stammdaten.get("Patient_ID")
        if patient_id is None:
            print(f"Patient_ID is unset for {filename}. Skipping.")
            continue

        # remove all Menge_Meldung
        menge_meldung = patient.find('./{http://www.gekid.de/namespace}Menge_Meldung')
        if menge_meldung is not None:
            patient.remove(menge_meldung)

            for meldung in menge_meldung:
                # build Menge_Meldung group
                meldung_id = meldung.get("Meldung_ID")
                menge_meldung_group = ET.Element("Menge_Meldung")
                menge_meldung_group.append(meldung)
                # build parent tag Patient, append two children on the same level:
                # append child Patient_Stammdaten first,
                # append child Menge_Meldung_Group second
                element_patient = ET.Element("Patient")

                # Fix leading IDs in
                if patient_id is not None:
                    patient_id = remove_leading_zeros(patient_id)
                    patient_stammdaten.set("Patient_ID", patient_id)

                element_patient.append(patient_stammdaten)
                element_patient.append(menge_meldung_group)

                # build grandparent tag Menge_Patient and add Patient + children
                menge_patient = ET.Element("Menge_Patient")
                menge_patient.append(element_patient)
                # build root element
                meldung_root = ET.Element(root.tag, root.attrib)
                # append absender
                meldung_root.append(absender)
                # append menge_patient built above
                meldung_root.append(menge_patient)

                ET.register_namespace("", "http://www.gekid.de/namespace")
                f = BytesIO()
                tree = ET.ElementTree(meldung_root)
                ET.indent(tree, "  ")
                tree.write(
                    f, encoding="utf-8", xml_declaration=True
                )

                xml_str = f.getvalue().decode()
                # prepare json files for kafka bridge
                result_data = {
                    "payload": {
                        "LKR_MELDUNG": meldung_id,
                        "XML_DATEN": xml_str,
                        "VERSIONSNUMMER": 1,
                        "REFERENZ_NUMMER": patient_id,
                    }
                }

                results.append(result_data)

                if settings.save_as_files_enabled:
                    with open(
                        f"{settings.output_folder}/"
                        + f"patient_{patient_id}_meldung_"
                        + f"{meldung_id}.json",
                        "w",
                        encoding="utf-8",
                    ) as output_file:
                        json.dump(result_data, output_file, indent=4)

                save_xml_files(
                        meldung_root,
                        patient_id,
                        meldung_id,
                    )

                if kafka_producer is not None:
                    kafka_producer.poll(0)

                    # Asynchronously produce a message. The delivery report callback
                    # will be triggered from the call to poll() above, or flush() below
                    # when the message has been successfully delivered or failed
                    # permanently.
                    kafka_producer.produce(
                        settings.kafka_output_topic,
                        json.dumps(result_data),
                        callback=kafka_delivery_report,
                        key=f"{patient_id}-{meldung_id}",
                    )

    if kafka_producer is not None:
        kafka_producer.flush()

    return results


def decompose_folder(input_folder: str):
    settings = Settings()

    if settings.save_as_files_enabled:
        conditional_folder_create(settings.output_folder_xml)
    for xmlfile in os.listdir(settings.input_folder):
        if not xmlfile.endswith(".xml"):
            continue
        filename = os.path.join(input_folder, xmlfile)
        tree = ET.parse(filename)
        root = tree.getroot()

        decompose_sammelmeldung(root, filename)


def main():
    start = time.monotonic()
    settings = Settings()

    decompose_folder(settings.input_folder)

    end = time.monotonic()
    print(f"time elapsed: {end - start}s")


if __name__ == "__main__":
    main()
