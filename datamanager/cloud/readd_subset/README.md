## What is this?
This is a script to process exported archives from cloud DM in order to make the archive compatible with the standalone version of DM.

Run the script without any parameters in order to process all cloud DM archives in the same folder as the script.

Run the script with the `--file <path_to_specific_archive_to_process>` option in order to process one specific archive.

## Prerequisites
Remember to run `py -m pip install -r requirements.txt` on Windows or `python -m pip install -r requirements.txt` on Linux before running the script the first time in order to install all the required libraries.

## Debugging
The script can be debugged in Visual Studio code by opening the **folder** (containing the script, .vscode subfolder, and the requirements.txt file) and running the existing _Debug_ _script_ configuration (adding breakpoints as necessary). The _Debug_ _script_ can be altered to add arguments to the script for debugging the script when running it for a specific archive.