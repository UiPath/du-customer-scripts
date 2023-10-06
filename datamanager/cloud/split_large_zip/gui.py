import sys
import threading

from typing import Callable
from pathlib import Path

import tkinter as tk
from tkinter import messagebox

from utils import ZipUtils


DEFAULT_ZIP_SIZE_IN_KB = 100


class Window:
    zip_size_options: list = [
        "KB",
        "MB",
        "GB"
    ]
    size_in_bytes_map: dict = {
        "KB": 2**10,
        "MB": 2**20,
        "GB": 2**30
    }

    def __init__(self, zip_process_func: Callable):
        self.zip_process_func = zip_process_func

        self.is_split_in_progress = False
        self.is_split_finished = False

        self.gui = tk.Tk(className="Split ZIP")
        self.gui.geometry("640x360")
        self.gui.minsize(width=300, height=300)

        self.gui.protocol("WM_DELETE_WINDOW", self.on_close_gui)

        self.path_label = tk.Label(self.gui, text="ZIP Path:")
        self.path_label.place(rely=0.1)
        
        self.path_field = tk.Entry(self.gui, bd=3)
        self.path_field.place(x=55, rely=0.1, relwidth=0.7)

        self.zip_max_size_label = tk.Label(self.gui, text="ZIP Size:")
        self.zip_max_size_label.place(rely=0.2)

        self.zip_max_size_field = tk.Entry(self.gui, bd=3)
        self.zip_max_size_field.insert(0, DEFAULT_ZIP_SIZE_IN_KB)
        self.zip_max_size_field.place(x=55, rely=0.2)

        self.selected_zip_size = tk.StringVar()
        self.selected_zip_size.set(Window.zip_size_options[0])

        self.size_options = tk.OptionMenu(self.gui, self.selected_zip_size, *Window.zip_size_options)
        self.size_options.place(rely=0.195, x=190, height=25)

        self.page_limit_label = tk.Label(self.gui, text="File limit:")
        self.page_limit_label.place(rely=0.3)

        self.page_limit_field = tk.Entry(self.gui, bd=3)
        self.page_limit_field.place(x=55, rely=0.3)
        self.page_limit_field.insert(0, DEFAULT_ZIP_SIZE_IN_KB)
        
        self.button = tk.Button(self.gui, command=self.perform_split, text="Split zip")
        self.button.place(rely=0.45, x=5)

        self.split_zip_started_label = tk.Label(self.gui, text="Split zip started", fg="green")

        self.current_zip_no_label = tk.Label(self.gui, fg="green")

        self.error_label = tk.Label(self.gui, text="")
        self.error_label.place(rely=0.7, x=10)

    def perform_split(self):
        if not self.is_input_valid():
            return

        ZipUtils.size_limit_in_bytes = int(self.zip_max_size_field.get()) * Window.size_in_bytes_map[self.selected_zip_size.get()]
        ZipUtils.page_limit = int(self.page_limit_field.get())

        self.is_split_in_progress = True

        self.current_zip_no_label.config(text="")
        self.split_zip_started_label.place(rely=0.8, x=10)
        self.current_zip_no_label.place(rely=0.9, x=10)

        self.button["state"] = "disabled"
        self.error_label.place_forget()
        split_zip_thread = threading.Thread(target=self.zip_process_func, args=[Path(self.path_field.get())], daemon=True)
        split_zip_thread.start()
    
    def update_zip_status(self, message):
        if self.is_split_finished:
            self.is_split_in_progress = False
            self.split_zip_started_label.place_forget()
            self.button["state"] = "active"
            self.is_split_finished = False
        self.current_zip_no_label.config(text=message, fg="green")

    def handle_exception(self, args):
        self.is_split_in_progress = False
        self.split_zip_started_label.place_forget()
        self.current_zip_no_label.place_forget()
        self.button["state"] = "active"
        if not args.exc_value:
            self.error_label.config(text="Split failed.", fg="red")
        else: 
            self.error_label.config(text=f"Split failed.\n Reason: {args.exc_value}", fg="red")
        self.error_label.place(rely=0.65, x=0)

    def is_input_valid(self) -> bool:
        zip_max_size = self.zip_max_size_field.get()
        page_limit = self.page_limit_field.get()

        if not self.path_field.get():
            messagebox.showerror("Error", "Zip path must not be empty")
            return False

        if not zip_max_size.isdigit() or int(zip_max_size) <= 0:
            messagebox.showerror("Error", "Zip size must be a positive integer")
            return False

        if not page_limit.isdigit() or int(page_limit) <= 0:
            messagebox.showerror("Error", "File limit must be a positive integer")
            return False

        return True

    def on_close_gui(self):
        if not self.is_split_in_progress:
            self.gui.destroy()
            return

        if messagebox.askyesno(title="", message="Closing the window will end the zip split. Do you want to continue?", icon=messagebox.WARNING):
            self.gui.destroy()
            sys.exit()