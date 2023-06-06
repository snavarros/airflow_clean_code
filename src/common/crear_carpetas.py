import os


def create_data_folders(file_path) -> None:
    os.makedirs(name=f"{file_path}", exist_ok=True)
