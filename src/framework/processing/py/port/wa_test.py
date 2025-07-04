"""
WhatsApp module

This module contains functions to handle WhatsApp chatlog exports (*.txt files)
"""
from pathlib import Path
from typing import Any
import logging
import zipfile
import re

import pandas as pd

import port.whatstk as whatstk
from port.whatstk import WhatsAppChat




import port.unzipddp as unzipddp
import port.helpers as helpers
from port.validate import (
    DDPCategory,
    StatusCode,
    ValidateInput,
    Language,
    DDPFiletype,
)




logger = logging.getLogger(__name__)

def validate(zfile: Path) -> ValidateInput:
    """
    Validates the input of a WhatsApp zipfile
    """
    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix == ".txt" and "WhatsApp Chat" in p.name:
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validation.infer_ddp_category(paths)
        if validation.ddp_category.id is None:
            validation.set_status_code(1)
        else:
            validation.set_status_code(0)

    except zipfile.BadZipFile:
        validation.set_status_code(2)

    return validation


def chatlog_to_df(whatsapp_zip: str, chat_filename: str = None) -> pd.DataFrame:
    """
    Extracts WhatsApp chatlog from zip and parses it into a DataFrame.
    If chat_filename is None, tries to find the first .txt file with 'WhatsApp Chat' in the name.
    """
    # Find the chatlog file
    print("looking for the chatlog file in zip")
    if chat_filename is None:
        with zipfile.ZipFile(whatsapp_zip, "r") as zf:
            for f in zf.namelist():
                print("filename: " + f)
                if f.endswith(".txt") and ("WhatsApp Chat" in f or "WhatsApp-Chat" in f):
                    chat_filename = f
                    break
    if chat_filename is None:
        logger.error("No WhatsApp chatlog found in zip")
        return pd.DataFrame()

    b = unzipddp.extract_file_from_zip(whatsapp_zip, chat_filename)
    chat_text = b.read().decode("utf-8")
    print("chat_text: " + chat_text[:100])  # Print first 100 characters for debugging
    # Parse the chatlog text into a DataFrame
    messages = whatstk.whatsapp.parser._df_from_str(chat_text, auto_header = False, 
                                                    hformat = "%d.%m.%y, %H:%M - %name:")
    print("Extracted the messages from the chatlog: " + str(len(messages)) + " messages found.")
    # messages: list of dicts with keys: date, time, sender, message
    df = pd.DataFrame(messages)
    return df


from whatstk import df_from_whatsapp
from whatstk.data import whatsapp_urls
#df = df_from_whatsapp(filepath="/mnt/c/Users/marti/Meine Ablage/Dokumente/Arbeit/KoWi/DDP/WhatsApp-Chat mit Tanja S. Strukelj  Smål Jež ❤️.txt")
#print(df.head(5))

chatlog = chatlog_to_df(whatsapp_zip="/mnt/c/Users/marti/Meine Ablage/Dokumente/Arbeit/KoWi/DDP/Tanja")
print(chatlog.head(5))

"""
``hformat`` is required.
        hformat (str, optional): :ref:`Format of the header <The header format>`, e.g.
                                    ``'[%y-%m-%d %H:%M:%S] - %name:'``. Use following keywords:

                                    - ``'%y'``: for year (``'%Y'`` is equivalent).
                                    - ``'%m'``: for month.
                                    - ``'%d'``: for day.
                                    - ``'%H'``: for 24h-hour.
                                    - ``'%I'``: for 12h-hour.
                                    - ``'%M'``: for minutes.
                                    - ``'%S'``: for seconds.
                                    - ``'%P'``: for "PM"/"AM" or "p.m."/"a.m." characters.
                                    - ``'%name'``: for the username.

                                    Example 1: For the header '12/08/2016, 16:20 - username:' we have the
                                    ``'hformat='%d/%m/%y, %H:%M - %name:'``.

                                    Example 2: For the header '2016-08-12, 4:20 PM - username:' we have
                                    ``hformat='%y-%m-%d, %I:%M %P - %name:'``.


                                    german formatting:
                                    31.10.21, 22:36 - Martin Fischer: <Medien ausgeschlossen>
                            31.10.21, 22:36 - Martin Fischer: <Medien ausgeschlossen>
                            31.10.21, 22:36 - Martin Fischer: <Medien ausgeschlossen>
                            31.10.21, 22:36 - Martin Fischer: <Medien ausgeschlossen>
                            31.10.21, 22:36 - Martin Fischer: <Medien ausgeschlossen>
                            31.10.21, 22:36 - Martin Fischer: <Medien ausgeschlossen>

                            hformat = "%d.%m.%y, %H:%M - %name:"
"""