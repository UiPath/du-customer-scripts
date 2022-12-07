#!/usr/bin/env python
"""
This script is used to split zip files (DM exports) that are above 1GB in size or over 1500 files.

Usage:
    split_zip.py --path C:\\path\\to\\import\\archive.zip

The result of running the script is multiple zip files that are all below 1GB and have less than 1500 files.
"""
import csv
import os
import re
import tempfile
import threading
import zipfile
from dataclasses import dataclass
from io import TextIOWrapper
from pathlib import Path
import tkinter as tk
from tkinter import messagebox
import sys

SIZE_LIMIT_IN_BYTES = 1000000000
PAGE_LIMIT = 1500


@dataclass
class Configs:
    schema_path: Path = Path()
    split_path: Path = Path()
    start_size: int = 0  # size of schema and split file
    zip_name: str = ""


class Window:
    ZIP_SIZE_OPTIONS = [
        "MB",
        "GB"
    ]
    ZIP_SIZE_DICT = {
        "MB": 2**20,
        "GB": 2**30
    }

    def __init__(self):
        self.is_split_in_progress = False

        self.gui = tk.Tk(className="Split ZIP")
        self.gui.geometry("200x200")
        self.gui.maxsize(width=500,height=500)
        self.gui.minsize(width=250, height=250)

        self.gui.protocol("WM_DELETE_WINDOW", self.on_close_gui)

        self.path_label = tk.Label(self.gui, text = "ZIP Path:")
        self.path_label.place(rely=0.1)
        
        self.path_field = tk.Entry(self.gui, bd=3)
        self.path_field.place(x=55, rely=0.1, relwidth=0.7)

        self.zip_max_size_label = tk.Label(self.gui, text="ZIP Size:")
        self.zip_max_size_label.place(rely=0.2)

        self.zip_max_size_field = tk.Entry(self.gui, bd=3)
        self.zip_max_size_field.place(x=55, rely=0.2, relwidth=0.3)

        self.selected_zip_size = tk.StringVar()
        self.selected_zip_size.set(Window.ZIP_SIZE_OPTIONS[0])

        self.size_options = tk.OptionMenu(self.gui, self.selected_zip_size, *Window.ZIP_SIZE_OPTIONS)
        self.size_options.place(rely=0.19, x=140, height=25)

        self.file_limit_label = tk.Label(self.gui, text="File limit:")
        self.file_limit_label.place(rely=0.3)

        self.file_limit_field = tk.Entry(self.gui, bd=3)
        self.file_limit_field.place(x=55, rely=0.3, relwidth=0.3)

        self.button = tk.Button(self.gui, command=self.perform_split, text="Split zip")
        self.button.place(rely=0.5, x=150)

        self.split_zip_started_label = tk.Label(self.gui, text="Split zip started", fg="green")

        self.current_zip_no_label = tk.Label(self.gui, fg="green")

        self.error_label = tk.Label(self.gui, text="")
        self.error_label.place(rely=0.7, x=10)

    def perform_split(self):
        self.is_split_in_progress = True

        self.current_zip_no_label.config(text="")
        self.split_zip_started_label.place(rely=0.8, x=10)
        self.current_zip_no_label.place(rely=0.9, x=10)

        self.button["state"] = "disabled"
        self.error_label.place_forget()
        split_zip_thread = threading.Thread(target=process_zip, args = [Path(self.path_field.get())], daemon=True)
        split_zip_thread.start()
    
    def update_zip_status(self, message, is_split_finished: bool = False):
        if is_split_finished:
            self.is_split_in_progress = False
            self.split_zip_started_label.place_forget()
            self.button["state"] = "active"
        self.current_zip_no_label.config(text=message, fg="green")

    def handle_exception(self, args):
        self.is_split_in_progress = False
        self.split_zip_started_label.place_forget()
        self.current_zip_no_label.place_forget()
        self.button["state"] = "active"
        if not args.exc_value:
            self.error_label.config(text=f"Split failed.", fg="red")
        else: 
            self.error_label.config(text=f"Split failed.\n Reason: {args.exc_value}", fg="red")
        self.error_label.place(rely=0.65, x=10)
    
    def on_close_gui(self):
        if not self.is_split_in_progress:
            self.gui.destroy()
            return

        if messagebox.askquestion("Quit", "Closing the window will end the zip split. Do you want to continue?"):
            self.gui.destroy()
            sys.exit()


window: Window = None


def main():
    global window
    window = Window()

    threading.excepthook = window.handle_exception

    window.gui.mainloop()


def process_zip(path: Path):
    Configs.zip_name = path.stem
    with tempfile.TemporaryDirectory() as temp_zip_contents:
        with zipfile.ZipFile(path, "r") as zip_handle:
            zip_handle.extractall(temp_zip_contents)

            directory_structure = list(Path(temp_zip_contents).iterdir())
            while "images" not in [str(p.stem) for p in directory_structure]:
                directory_structure = list(directory_structure[0].iterdir())

            top_level_paths = {p.name: p for p in directory_structure}

            Configs.schema_path = top_level_paths["schema.json"]
            Configs.start_size += Configs.schema_path.stat().st_size

            Configs.split_path = top_level_paths["split.csv"]
            Configs.start_size += Configs.split_path.stat().st_size

            split_files(top_level_paths["images"], top_level_paths["latest"], temp_zip_contents)


def split_files(images_path: Path, latest_path: Path, folder_contents_path: str):
    image_names = list()
    current_size = Configs.start_size
    archive_count = 1
    image_paths = {path.name: path for path in list(Path(images_path).iterdir())}
    latest_paths = {path.stem: path for path in list(Path(latest_path).iterdir())}  # keys are document names
    pages_by_documents = get_pages_by_documents(latest_paths, image_paths)

    for image_name in latest_paths:
        size = image_paths[image_name].stat().st_size + latest_paths[image_name].stat().st_size
        size += sum([x.stat().st_size for x in pages_by_documents[image_name]])
        current_size += size
        if current_size < SIZE_LIMIT_IN_BYTES and len(image_names) < PAGE_LIMIT:
            image_names.append(image_name)
        else:
            create_archive(folder_contents_path, image_names, archive_count)
            window.update_zip_status(f"Processed zip number {archive_count}")

            archive_count += 1
            current_size = Configs.start_size + size
            image_names = [image_name]
    create_archive(folder_contents_path, image_names, archive_count)  # for the last zip
    window.update_zip_status(f"Split zip finished with {archive_count} zip files.", True)


def get_pages_by_documents(document_names: dict, image_names: dict):
    pages_by_documents = {}
    for document_name in document_names:
        if document_name.split(".")[-1] in ("pdf", "tiff", "tif"):
            pages_by_documents[document_name] = [
                image_names[x] for x in image_names if re.search(re.escape(document_name) + "_\d+.jpg", x)
            ]  # the paths of the pages
        else:
            pages_by_documents[document_name] = []
    return pages_by_documents


def create_archive(folder_contents_path: str, image_names: list, suffix: int):
    with zipfile.ZipFile(
        Configs.zip_name + "_" + str(suffix) + ".zip", "w", compression=zipfile.ZIP_DEFLATED
    ) as zip_handle:
        for root, _, files in os.walk(folder_contents_path):
            for file in files:
                for image_name in image_names:
                    # both the image and the related metadata file will be picked up for writing due to this check
                    if image_name in file:
                        zip_handle.write(
                            Path(root).joinpath(file),
                            Path(root).joinpath(file).relative_to(folder_contents_path),
                        )
        zip_handle.write(
            Path(root).joinpath(Configs.schema_path),
            Path(root).joinpath(Configs.schema_path).relative_to(folder_contents_path),
        )
        processed_split_name = process_split_file_one_archive(image_names, suffix)
        zip_handle.write(
            Path(root).joinpath(processed_split_name),
            Path(root).joinpath(Configs.split_path).relative_to(folder_contents_path),
        )


def process_split_file_one_archive(images: list, suffix: int):
    lines = list()

    with open(Configs.split_path, "rb") as f:
        reader = csv.reader(TextIOWrapper(f, "utf-8"), delimiter="\t")
        lines.append(next(reader))
        for row in reader:
            if row[0] in images:
                lines.append(row)

    split_file_name = Configs.split_path.stem + "_" + str(suffix)
    temp_split_path = change_file_name(Configs.split_path, split_file_name)
    with open(temp_split_path, "w", encoding="utf-8", newline="") as write_file_handle:
        writer = csv.writer(write_file_handle, delimiter="\t")
        writer.writerows(lines)
    return temp_split_path


def change_file_name(path: Path, file_name: str):
    return f"{path.parent}/{file_name}.{path.suffix}"


if __name__ == "__main__":
    main()
