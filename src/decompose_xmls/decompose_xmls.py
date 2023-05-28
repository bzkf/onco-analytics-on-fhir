import json
import os
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass

# TODO Env.vars?
input_folder = "input-adt-reports"
output_folder = "output-files"
output_folder_xml = "output-files/output-xmls"

save_xmls = True  # brauchen wir im Grunde nicht
save_jsons = True


@dataclass
class Einzelmeldung:
    xml: ET.Element
    patient_id: str
    meldung_id: str

    def __repr__(self) -> str:
        ET.register_namespace("", "http://www.gekid.de/namespace")
        repr = {
            "xml": ET.tostring(self.xml, encoding="unicode"),
            "patient_id": self.patient_id,
            "meldung_id": self.meldung_id,
        }
        return json.dumps(repr)


def conditional_folder_create(folder_name):
    if os.path.exists(folder_name):
        pass
    else:
        os.makedirs(folder_name)

    """ ist eine Variante besser als die andere?
    try:
        os.mkdir(output_folder_xml)
    except OSError as error:
        print(error) """


def save_xml_files(meldung_root, patient_id, meldung_id):
    ET.register_namespace("", "http://www.gekid.de/namespace")
    ET.ElementTree(meldung_root).write(
        f"{output_folder_xml}/patient_{patient_id}_meldung_{meldung_id}.xml",
        encoding="UTF-8",
        xml_declaration=True,
    )


def save_json_files(meldung_root, patient_id, meldung_id):
    # saves json files for kafka bridge input in this schema
    xml_str = ET.tostring(meldung_root, encoding="unicode")

    # prepare json files for kafka bridge
    result_data = {}
    result_data["LKR_MELDUNG"] = meldung_id
    result_data["XML_DATEN"] = xml_str
    result_data["VERSIONSNUMMER"] = 1
    result_data["REFERENZ_NUMMER"] = patient_id

    with open(
        f"{output_folder}/patient_{patient_id}_meldung_{meldung_id}.json",
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(result_data, f, indent=4)


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


def decompose_folder(input_folder: str):
    for xmlfile in os.listdir(input_folder):
        if not xmlfile.endswith(".xml"):
            continue
        filename = os.path.join(input_folder, xmlfile)
        tree = ET.parse(filename)
        root = tree.getroot()

        for einzelmeldung in decompose_sammelmeldung(root, filename):
            # TODO das ist nicht so ideal, aber sonst muss ich in der main auch
            # nochmal loopen - Verbesserungsvorschlag? oder Loop nur in main?
            if save_xmls:
                save_xml_files(
                    einzelmeldung.xml,
                    einzelmeldung.patient_id,
                    einzelmeldung.meldung_id,
                )

            if save_jsons:
                save_json_files(
                    einzelmeldung.xml,
                    einzelmeldung.patient_id,
                    einzelmeldung.meldung_id,
                )


def main():
    start = time.time()

    # do all stuff here
    conditional_folder_create(output_folder_xml)
    decompose_folder(input_folder)

    end = time.time()
    print("time elapsed:", end - start, "s")


if __name__ == "__main__":
    main()
