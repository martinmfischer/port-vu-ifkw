"""
WhatsApp module

This module contains functions to handle WhatsApp chatlog exports (*.txt files)
"""
from pathlib import Path
from typing import Any
import logging
import zipfile
import re
import tempfile

import pandas as pd


import os


import port.whatstk as whatstk
from port.whatstk import WhatsAppChat
from port.whatstk import df_from_whatsapp


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

DDP_CATEGORIES = [
    DDPCategory(
        id="whatsapp_txt",
        ddp_filetype=DDPFiletype.TXT,
        language=Language.EN,
        known_files=[
            # English WhatsApp exports
            "WhatsApp Chat with",
            "WhatsApp Chat.txt",
            "WhatsApp-Chat with",
            "WhatsApp-Chat.txt",
            # German WhatsApp exports
            "WhatsApp Chat mit",
            "WhatsApp-Chat mit",
            "WhatsApp Chat.txt",
            "WhatsApp-Chat.txt",
        ],
    )
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid WhatsApp Export", message=""),
    StatusCode(id=1, description="Not a valid WhatsApp Export", message=""),
    StatusCode(id=2, description="Bad zipfile", message=""),
]

def is_known_file(filename: str) -> bool:
    """
    Checks if the filename matches any known WhatsApp export patterns.
    """
    normalized_filename = filename.lower().strip()
    for known_file in DDP_CATEGORIES[0].known_files:
        logger.debug(f"Comparing filename {filename} to known file: {known_file}")
        if known_file.lower().strip() in normalized_filename:
            logger.debug("Found known WhatsApp file: %s", filename)
            return True
    return False

def validate(zfile: Path) -> ValidateInput:
    """
    Validates the input of a WhatsApp zipfile
    """
    logger.debug("Starting validation for zipfile: %s", zfile)
    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)
    found = False

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            logger.debug("Opened zipfile: %s", zfile)
            for f in zf.namelist():
                logger.debug("Inspecting file in zip: %s", f)
                p = Path(f)
                
                if p.suffix == ".txt" and is_known_file(f):
                    logger.debug("Found candidate WhatsApp txt file: %s", p.name)
                    paths.append(p.name)
                    # Check content of the txt file using whatstk
                    with zf.open(f) as chat_file:
                        try:
                            logger.debug("Attempting to parse file with whatstk: %s", p.name)
                            
                            with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmpfile:
                                tmpfile.write(chat_file.read())
                                tmpfile.flush()
                                df = df_from_whatsapp(
                                    tmpfile.name,
                                    auto_header=False,
                                    hformat="%d.%m.%y, %H:%M - %name:"
                                )
                            logger.debug("Parsing result DataFrame shape: %s", df.shape)
                            if not df.empty:
                                logger.debug("Valid WhatsApp chatlog found: %s", p.name)
                                validation.set_status_code(0)
                                found = True
                                break
                            else:
                                logger.debug("Parsed DataFrame is empty for file: %s", p.name)
                        except Exception as e:
                            logger.debug("whatstk failed to parse file %s: %s", p.name, e)
            if not found:
                logger.debug("No valid WhatsApp chatlog found in zipfile: %s", zfile)
                validation.set_status_code(1)

    except zipfile.BadZipFile as e:
        logger.debug("BadZipFile exception for file %s: %s", zfile, e)
        validation.set_status_code(2)
    except Exception as e:
        logger.debug("Unexpected exception during validation: %s", e)

    logger.debug("Validation result for %s: status_code=%s", zfile, validation.status_code)
    return validation


def chatlog_to_df(whatsapp_zip: str, chat_filename: str = None) -> pd.DataFrame:
    """
    Extracts WhatsApp chatlog from zip and parses it into a DataFrame.
    If chat_filename is None, tries to find the first .txt file with 'WhatsApp Chat' in the name.
    """
    # Find the chatlog file
    chat_filename = None
    out = pd.DataFrame()

    with zipfile.ZipFile(whatsapp_zip, "r") as zf:
        for f in zf.namelist():
            if f.endswith(".txt") and is_known_file(f):
                chat_filename = f
                break



        if chat_filename is None:
            logger.error("No WhatsApp chatlog found in zip")
            return pd.DataFrame()


        with zf.open(chat_filename) as chat_file:
            try:
            # Write the chat_file to a temporary file because whatstk expects a file path
                with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmpfile:
                    tmpfile.write(chat_file.read())
                    tmpfile.flush()
                    out = df_from_whatsapp(
                    tmpfile.name,
                    auto_header=False,
                    hformat="%d.%m.%y, %H:%M - %name:"
                    )
                logger.debug("Parsed DataFrame: %s", out.head())
            except Exception as e:
                logger.debug("whatstk failed to parse file %s: %s", chat_filename, e)
    return out
