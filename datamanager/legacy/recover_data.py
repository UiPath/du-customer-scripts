#!/usr/bin/env python

"""
This script is used to recover data from a DataManager Legacy instance.
Most normalization and processing steps are skipped so this data is not to be used in ML pipelines, but to be
imported in a clean DataManager (legacy or not) instance and exported from there.

Usage::
    python recover_data.py
        - will recover data from the Docker container, has to run in the same container
    python recover_data.py --dataset-dir C:\\path\\to\\data\\folder\\default
        - will recover the data from the specified folder

The dataset-dir has to be the folder where schema.json and the input/ and output/ folders are located
"""
import argparse
import json
import os
import re
from pathlib import Path
from zipfile import ZipFile


def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument("--dataset-dir", help="usually /app/data/default in container", default="/app/data/default")
    args = parser.parse_args()
    try:
        export(Path(args.dataset_dir))
    except Exception as e:
        print(f"Export failed. Reason: \n {e}")


def export(dataset_path: Path):
    root_dir = Path("/app") if str(dataset_path) == "/app/data/default" else dataset_path
    export_tmp_file = root_dir.joinpath("backup.tmp")
    export_zip_file = root_dir.joinpath("backup.zip")

    print(f"Export - started...")

    fnames = list(dataset_path.joinpath("input").glob("*.jpg")) + list(dataset_path.joinpath("input").glob("*.png"))
    documents = []
    split_list = ["files\tsubset"]
    print(f"Export - processing documents")
    for ofile in fnames:
        filename = ofile.name
        doc = load_document(dataset_path, str(filename))
        doc["fname"] = str(filename)
        documents.append(doc)

    schema_data = create_schema_data(dataset_path)

    # Removed the deduplication from here as this script serves the purpose of getting all your data
    # It is assumed that the resulted export will be imported in another DM instance and deduplication will be performed
    # when exporting from there

    # -- export images and latest
    print(f"Export - archiving documents")
    with ZipFile(export_tmp_file, "w") as export_zip:
        for doc in documents:
            export_zip.write(str(dataset_path.joinpath("input", doc["fname"])),
                             os.path.join("images", doc["fname"]))
            # -- update split_list
            doc["subset"] = doc["subset"] if doc["subset"] not in ["", "none", None, "None"] else "TRAIN"
            split_list.append(doc["fname"] + "\t" + doc["subset"])
            if "language" in doc and type(doc["language"]) != str:
                doc["language"] = ""
            # -- save
            json_data = json.dumps(doc, indent=3)
            export_zip.writestr(os.path.join("latest", doc["fname"] + ".json"), json_data)

    print("Export - archiving schema")
    with ZipFile(export_tmp_file, "a") as export_zip:
        schema_data_json = json.dumps(schema_data, indent=3)
        export_zip.writestr("schema.json", schema_data_json)

    print("Export - archiving split.csv")
    with ZipFile(export_tmp_file, "a") as export_zip:
        export_zip.writestr("split.csv", "\n".join(split_list))
    os.rename(export_tmp_file, export_zip_file)
    print(f"Exported file is available at: {export_zip_file}")


def load_document(dspath: Path, fname: str):
    fname_key = os.path.split(fname)[1]
    fname_key = re.sub(r"\.(box|raw)\.json$", "", fname_key)

    fjson_output_path = dspath.joinpath(f"output/{fname_key}.json")
    fjson_input_path = dspath.joinpath(f"input/{fname_key}.box.json")

    item = {}

    if os.path.isfile(fjson_output_path):
        with open(fjson_output_path, "r") as f:
            item = json.load(f)

    elif os.path.isfile(fjson_input_path):
        # new line_seg has different format
        def adapt_to_old(page):
            words = []
            for nrl, line in enumerate(page):
                for w in line["words"]:
                    w["line"] = nrl
                    words.append(w)
            return words

        # read .box.json file
        with open(fjson_input_path, "r") as f:
            try:
                data = json.load(f)
                if data:
                    # collect some attributes stored in the first dict
                    if "angle" in data[0]:
                        item["angle"] = data[0]["angle"]
                        del data[0]["angle"]
                    if "ocr_language" in data[0]:
                        item["ocr_language"] = data[0]["ocr_language"]
                        del data[0]["ocr_language"]
                    if "batch_name" in data[0]:
                        item["batch"] = data[0]["batch_name"]
                        del data[0]["batch_name"]
                    if "text" in data[0]:
                        # adapt a different input format
                        data = adapt_to_old(data)
                item["words"] = data
            except Exception as e:
                print(fjson_input_path, e)

    item["fname"] = os.path.basename(fname)
    if "subset" in item:
        item["subset"] = item["subset"] if item["subset"] not in ["", "none", None, "None"] else "TRAIN"
    return item


def create_schema_data(dspath: Path) -> dict:
    with open(dspath.joinpath("schema.json")) as f:
        schema_data = json.load(f)
    # remove hidden fields
    schema_data["extraction"] = [x for x in schema_data["extraction"] if not x.get("hidden")]

    for item in schema_data["extraction"]:
        if "color" in item:
            del item["color"]
        if "hotkey" in item:
            del item["hotkey"]
        if "section" in item and item["section"] != "items":
            del item["section"]

    return schema_data


if __name__ == '__main__':
    main()
