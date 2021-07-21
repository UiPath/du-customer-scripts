import csv
import os
import re
import tempfile
import zipfile
from io import TextIOWrapper
from pathlib import Path
from typing import List, Optional

import click
import ujson

SPLIT_FILE_NAME = "split.csv"


@click.command()
@click.option(
    "--file", help="Name of the (cloud DM) archive that needs to be processed."
)
def process(file: Optional[str]):
    """A script to process exported archives from cloud DM in order to make the archive compatible with the standalone version of DM.
    Run the script without any parameters in order to process all (cloud DM) archives in the same folder as the script.
    Run the script with the --file <path_to_specific_archive_to_process> option in order to process one specific archive."""
    if file:
        if Path(file).exists():
            zip_file_paths = [file]
        else:
            raise ValueError(
                """No archives were found for processing!
            When using the --file flag to run this script you need to specify the path of an archive for processing."""
            )
    else:
        script_folder_path = Path(__file__).parent
        zip_file_paths = list(map(lambda p: str(p), script_folder_path.rglob("*.zip")))
        if not zip_file_paths:
            raise ValueError(
                """No archives were found for processing!
            Add one or more archives in the same folder as the script and run the script without any flags!"""
            )

    print("The following archives will be processed:")
    for zip_file_path in zip_file_paths:
        print(f"\t{Path(zip_file_path).name}")

    for zip_file_path in zip_file_paths:
        _process_single_archive(zip_file_path)

    print("Done.")


def _process_single_archive(zip_file_path: str):
    with tempfile.TemporaryDirectory() as temp_zip_contents:
        print(
            f"The temporary folder for used for unzipping {zip_file_path} is {temp_zip_contents}"
        )
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(temp_zip_contents)

            all_file_paths = list(
                map(lambda p: str(p), Path(temp_zip_contents).rglob("*"))
            )

            split_file_path = _get_split_file_path(all_file_paths)

            subset_by_document = _read_document_subset_split(split_file_path)

            metadata_file_paths = _get_metadata_file_paths(all_file_paths)
            for metadata_file_path in metadata_file_paths:
                _process_metadata_json_file(metadata_file_path, subset_by_document)

            processed_zip_file_path = zip_file_path.rstrip(".zip") + "_with_subset.zip"
            _create_archive(processed_zip_file_path, temp_zip_contents)

            print(f"Finished processing archive at {zip_file_path}")


def _get_split_file_path(all_file_paths: List[str]):
    split_file_paths = [
        path for path in all_file_paths if path.endswith(SPLIT_FILE_NAME)
    ]
    if not split_file_paths:
        raise ValueError(
            """Missing split.csv file!
            Cannot determine the split and thus cannot continue!"""
        )

    split_file_path = split_file_paths[0]
    print(f"Using the split.csv file found at {split_file_path}")

    return split_file_path


def _read_document_subset_split(split_file_path: str) -> dict:
    subset_by_document = {}
    with open(split_file_path, "rb") as f:
        reader = csv.reader(TextIOWrapper(f, "utf-8"), delimiter="\t")
        next(reader)
        for row in reader:
            subset_by_document[row[0]] = row[1]
    return subset_by_document


def _get_metadata_file_paths(all_file_paths: List[str]):
    if os.name == "nt":
        print("You are running this script on Windows.")
        pattern = r"\\?latest\\[^/]+\.json$"
    else:
        print("You are running this script on Linux.")
        pattern = r"/?latest/[^/]+\.json$"

    metadata_file_paths = [path for path in all_file_paths if re.search(pattern, path)]
    print(f"Found {len(metadata_file_paths)} metadata json files to process")
    return metadata_file_paths


def _process_metadata_json_file(metadata_file_path: str, subset_by_document: dict):
    with open(metadata_file_path, "rb") as f:
        metadata = ujson.loads(f.read())
        if "vs_labelled" in metadata:
            del metadata["vs_labelled"]
        document_name = Path(metadata_file_path).name.rstrip(".json")
        subset = subset_by_document[document_name]
        metadata["subset"] = subset
        with open(metadata_file_path, "w") as g:
            g.write(ujson.dumps(metadata, indent=3))


def _create_archive(processed_zip_file_path: str, folder_to_pack: str):
    print(
        f"Recreating the archive at {processed_zip_file_path} from temporary folder {folder_to_pack}"
    )
    with zipfile.ZipFile(processed_zip_file_path, "w") as zip_handle:
        for root, _, files in os.walk(folder_to_pack):
            for file in files:
                zip_handle.write(
                    os.path.join(root, file),
                    os.path.relpath(os.path.join(root, file), folder_to_pack),
                )


if __name__ == "__main__":
    process()
