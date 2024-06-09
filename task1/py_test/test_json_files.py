import os
import json
import pytest

folder_path = '../jsons/jsons_file'


def test_json_files_exist():
    """Check that there is at least one JSON file in the directory."""
    # List all files in the directory
    files = os.listdir(folder_path)
    # Keep only files that end with .json
    json_files = [file for file in files if file.endswith('.json')]
    # Assert that there are JSON files present
    assert len(json_files) > 0, "No JSON files found in the directory."


def test_json_files_not_empty():
    """Ensure that JSON files are not empty."""
    files = os.listdir(folder_path)
    json_files = [file for file in files if file.endswith('.json')]
    for json_file in json_files:
        # Construct the full file path
        file_path = os.path.join(folder_path, json_file)
        # Open and load the JSON file
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            # Assert that the file contains data
            assert data, f"File {json_file} is empty."
