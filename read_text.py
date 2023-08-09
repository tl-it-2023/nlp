from pypdf import PdfReader
from zipfile import ZipFile
from os import path, remove

from config import RESUME_STORAGE_DIR


def unzip(zipfile_path: str, files_directory):
    """Разархивирование всех файлов в папку"""
    with ZipFile(zipfile_path, "r") as myzip:
        myzip.extractall(path=files_directory)


def pdf_to_text(file_path: str) -> str:
    """Получение текста с pdf"""
    try:
        reader = PdfReader(file_path)
        data = ' '.join([page.extract_text() for page in reader.pages])
        data = ' '.join(data.replace('\n', ' ').split()).replace(' ,', ',').strip()
    except Exception as _ex:
        data = ''
        print(_ex)
    # finally:
    #     remove(file_path)

    return data


def txt_to_text(file_path: str) -> str:
    """Получение текста с txt"""
    try:
        with open(file_path, encoding='UTF-8') as file:
            data = file.read().replace('\n', ' ').replace(' ,', ',').strip()
    except Exception as _ex:
        data = ''
        print(_ex)
    # finally:
    #     remove(file_path)

    return data


# def docx_to_text(file_path: str) -> str:
#     """Получение текста с docx"""
#     data = docx2txt.process(file_path)
#     return data.replace('\n', '.').replace(' ,', ',').strip()


def read_files(filename: str) -> str:
    """Генератор получения текста из файлов"""
    extension = RESUME_STORAGE_DIR[RESUME_STORAGE_DIR.rfind('.') + 1:]
    if extension == 'zip':
        unzip(files_directory=RESUME_STORAGE_DIR, zipfile_path=path.join(RESUME_STORAGE_DIR, filename))

    extension = filename[filename.rfind('.') + 1:]
    match extension:
        case 'pdf':
            text = pdf_to_text(path.join(RESUME_STORAGE_DIR, filename))
        case 'txt':
            text = txt_to_text(path.join(RESUME_STORAGE_DIR, filename))
        # case 'docx':
        #     text = docx_to_text(path.join(resume_storage_dir, file_name))
        case _:
            text = ''

    return text

    # for file_name in listdir(files_directory_to):
    #     extension = file_name[file_name.rfind('.') + 1:]
    #     match extension:
    #         case 'pdf':
    #             text = pdf_to_text(path.join(files_directory_to, file_name), filename)
    #         case 'txt':
    #             text = txt_to_text(path.join(files_directory_to, file_name))
    #         # case 'docx':
    #         #     text = docx_to_text(path.join(files_directory_to, file_name))
    #         case _:
    #             text = ''
    #     yield text
