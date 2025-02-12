import re

from unidecode import unidecode


def clean_name_string(value):
    value = value.strip()
    value = unidecode(value)
    value = re.sub(r'[^a-zA-Z]', '', value)
    value = value.upper()
    return value