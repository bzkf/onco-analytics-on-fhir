import json
import os
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass

from confluent_kafka import Producer
from pydantic import BaseSettings


class Settings(BaseSettings):
    input_folder: str = "./input-adt-reports"
    output_folder: str = "./output-files"
    output_folder_xml: str = "./output-files/output-xmls"
    save_as_files_enabled: bool = True
    kafka_enabled: bool = False
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_output_topic: str = "adt.einzelmeldungen"


@dataclass
class Einzelmeldung:
    xml: ET.Element
    patient_id: str
    meldung_id: str

    def __repr__(self) -> str:
        ET.register_namespace("", "http://www.gekid.de/namespace")
        dict_repr = {
            "xml": ET.tostring(self.xml, encoding="unicode"),
            "patient_id": self.patient_id,
            "meldung_id": self.meldung_id,
        }
        return json.dumps(dict_repr)


def conditional_folder_create(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)


def save_xml_files(meldung_root, patient_id, meldung_id):
    settings = Settings()
    ET.register_namespace("", "http://www.gekid.de/namespace")
    ET.ElementTree(meldung_root).write(
        f"{settings.output_folder_xml}/patient_{patient_id}_meldung_{meldung_id}.xml",
        encoding="UTF-8",
        xml_declaration=True,
    )


def decompose_sammelmeldung(root: ET.Element, filename: str) -> list[Einzelmeldung]:
    # Get all "Patient" elements, save the absender to be appended to each new file
    patients = root.findall(".//{http://www.gekid.de/namespace}Patient")
    absender = root.find("./{http://www.gekid.de/namespace}Absender")

    if absender is None:
        print(f"Absender is not defined in the input file {filename}. Stopping.")
        return []

    result: list[Einzelmeldung] = []

    # Loop through each patient
    for patient in [p for p in patients if p is not None]:
        # Get the patient ID - might remove from filename later
        # and only keep the meldung_id
        if (
            patient_id_element := patient.find(
                ".//{http://www.gekid.de/namespace}Patienten_Stammdaten"
            )
        ) is None:
            print(f"Patienten_Stammdaten is unset for {filename}. Skipping.")
            continue

        patient_id = patient_id_element.get("Patient_ID")
        if patient_id is None:
            print(f"Patient_ID is unset for {filename}. Skipping.")
            continue

        # Get all "Meldung" elements for this patient
        meldungen = patient.findall(".//{http://www.gekid.de/namespace}Meldung")

        # Loop through each meldung for this patient
        for meldung in meldungen:
            meldung_id = meldung.get("Meldung_ID")
            if meldung_id is None:
                print(f"Meldung_ID is unset for {filename}. Skipping.")
                continue

            # NEW IDEA - copy root, LOOP THROUGH EXISTING MELDUNGEN, BUT ONLY KEEP
            # THE ONE WITH meldung_id and remove the rest
            meldung_root = ET.Element(root.tag, root.attrib)
            meldung_root.append(absender)

            # reintroduce parent tag "Menge_Patient" that gets lost in the looping
            menge_patient = ET.Element("Menge_Patient")
            menge_patient.append(patient)
            meldung_root.append(menge_patient)

            menge_meldung_element = meldung_root.find(
                ".//{http://www.gekid.de/namespace}Menge_Meldung"
            )

            if menge_meldung_element is None:
                print(f"Menge_Meldung element not found in {filename}")
                continue

            menge_meldung_element.append(meldung)

            relevant_meldung = None
            # loop through the newly build meldung_root and remove all meldungen
            # with meldung_id not matching to currrent one and remove duplicates
            for einzelmeldung in meldung_root.findall(
                ".//{http://www.gekid.de/namespace}Meldung"
            ):
                if einzelmeldung.attrib["Meldung_ID"] == meldung_id:
                    if relevant_meldung is None:
                        relevant_meldung = einzelmeldung
                    else:
                        menge_meldung_element.remove(einzelmeldung)
                else:
                    menge_meldung_element.remove(einzelmeldung)

            result.append(Einzelmeldung(meldung_root, patient_id, meldung_id))

    return result


def kafka_delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def decompose_folder(input_folder: str):
    settings = Settings()

    kafka_producer: Producer = None
    if settings.kafka_enabled:
        kafka_producer = Producer(
            {"bootstrap.servers": settings.kafka_bootstrap_servers}
        )

    for xmlfile in os.listdir(input_folder):
        if not xmlfile.endswith(".xml"):
            continue
        filename = os.path.join(input_folder, xmlfile)
        tree = ET.parse(filename)
        root = tree.getroot()

        for einzelmeldung in decompose_sammelmeldung(root, filename):
            # saves json files for kafka bridge input in this schema
            xml_str = ET.tostring(root, encoding="unicode")

            # prepare json files for kafka bridge
            result_data = {
                "LKR_MELDUNG": einzelmeldung.meldung_id,
                "XML_DATEN": xml_str,
                "VERSIONSNUMMER": 1,
                "REFERENZ_NUMMER": einzelmeldung.patient_id,
            }

            if settings.save_as_files_enabled:
                with open(
                    f"{settings.output_folder}/"
                    + f"patient_{einzelmeldung.patient_id}_meldung_"
                    + f"{einzelmeldung.meldung_id}.json",
                    "w",
                    encoding="utf-8",
                ) as f:
                    json.dump(result_data, f, indent=4)

                save_xml_files(
                    einzelmeldung.xml,
                    einzelmeldung.patient_id,
                    einzelmeldung.meldung_id,
                )

            if kafka_producer is not None:
                kafka_producer.poll(0)

                # Asynchronously produce a message. The delivery report callback will
                # be triggered from the call to poll() above, or flush() below, when the
                # message has been successfully delivered or failed permanently.
                kafka_producer.produce(
                    settings.kafka_output_topic,
                    json.dumps(result_data),
                    callback=kafka_delivery_report,
                    key=f"{einzelmeldung.patient_id}-{einzelmeldung.meldung_id}",
                )

    if kafka_producer is not None:
        kafka_producer.flush()


def main():
    start = time.monotonic()
    settings = Settings()
    # do all stuff here
    conditional_folder_create(settings.output_folder_xml)
    decompose_folder(settings.input_folder)

    end = time.monotonic()
    print(f"time elapsed: {end - start}s")


if __name__ == "__main__":
    main()
