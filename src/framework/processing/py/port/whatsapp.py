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
            # WhatsApp exports are usually named like this
            "WhatsApp Chat with",
            "WhatsApp Chat.txt",
        ],
    )
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid WhatsApp Export", message=""),
    StatusCode(id=1, description="Not a valid WhatsApp Export", message=""),
    StatusCode(id=2, description="Bad zipfile", message=""),
]


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
    if chat_filename is None:
        with zipfile.ZipFile(whatsapp_zip, "r") as zf:
            for f in zf.namelist():
                if f.endswith(".txt") and "WhatsApp Chat" in f:
                    chat_filename = f
                    break
    if chat_filename is None:
        logger.error("No WhatsApp chatlog found in zip")
        return pd.DataFrame()

    b = unzipddp.extract_file_from_zip(whatsapp_zip, chat_filename)
    chat_text = b.decode("utf-8")

    # Use a helper to parse the chatlog (see script.py)
    messages = helpers.whatsapp_parse_chatlog(chat_text)

    # messages: list of dicts with keys: date, time, sender, message
    df = pd.DataFrame(messages)
    return df


# The following functions are not directly relevant for WhatsApp, but kept for compatibility.
# They can be adapted or left as stubs.

def group_interactions_to_df(*args, **kwargs) -> pd.DataFrame:
    logger.info("Not applicable for WhatsApp exports.")
    return pd.DataFrame()

def comments_to_df(*args, **kwargs) -> pd.DataFrame:
    logger.info("Not applicable for WhatsApp exports.")
    return pd.DataFrame()

def likes_and_reactions_to_df(*args, **kwargs) -> pd.DataFrame:
    logger.info("Not applicable for WhatsApp exports.")
    return pd.DataFrame()

def your_badges_to_df(*args, **kwargs) -> pd.DataFrame:
    logger.info("Not applicable for WhatsApp exports.")
    return pd.DataFrame()

def find_items(d: dict[Any, Any], key_to_match: str) -> str:
    # Not needed for WhatsApp, but kept for compatibility
    return ""

def your_posts_to_df(*args, **kwargs) -> pd.DataFrame:
    logger.info("Not applicable for WhatsApp exports.")
    return pd.DataFrame()

def your_posts_check_ins_photos_and_videos_1_to_df(*args, **kwargs) -> pd.DataFrame:
    logger.info("Not applicable for WhatsApp exports.")
    return pd.DataFrame()

def your_search_history_to_df(*args, **kwargs) -> pd.DataFrame:
    logger.info("Not applicable for WhatsApp exports.")
    return pd.DataFrame()

def recently_viewed_to_df(*args, **kwargs) -> pd.DataFrame:
    logger.info("Not applicable for WhatsApp exports.")
    return pd.DataFrame()

def recently_visited_to_df(*args, **kwargs) -> pd.DataFrame:
    logger.info("Not applicable for WhatsApp exports.")
    return pd.DataFrame()

def feed_to_df(*args, **kwargs) -> pd.DataFrame:
    logger.info("Not applicable for WhatsApp exports.")
    return pd.DataFrame()

def controls_to_df(*args, **kwargs) -> pd.DataFrame:
    logger.info("Not applicable for WhatsApp exports.")
    return pd.DataFrame()

def group_posts_and_comments_to_df(*args, **kwargs) -> pd.DataFrame:
    logger.info("Not applicable for WhatsApp exports.")
    return pd.DataFrame()
