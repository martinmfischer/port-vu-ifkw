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
from urllib.parse import urlparse

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
url_pattern = r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.-]+\.[a-z]{2,})(?:[^\s()<>]+|\([^\s()<>]+\))*)'


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
            # iOS exports
            "_chat.txt",
            "_chat",
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
    
    # Supported formats for different WhatsApp export versions
    formats_to_try = [
        "[%d.%m.%y, %H:%M:%S] %name:",  # New format with brackets and seconds
        "%d.%m.%y, %H:%M - %name:",      # Old format without seconds
    ]

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            logger.debug("Opened zipfile: %s", zfile)
            for f in zf.namelist():
                logger.debug("Inspecting file in zip: %s", f)
                p = Path(f)
                
                if is_known_file(f):
                    logger.debug("Found candidate WhatsApp txt file: %s", p.name)
                    paths.append(p.name)
                    # Check content of the txt file using whatstk
                    with zf.open(f) as chat_file:
                        # Read file content once
                        file_content = chat_file.read()
                        
                        # Try each format
                        for hformat in formats_to_try:
                            try:
                                logger.debug("Attempting to parse file with format: %s", hformat)
                                
                                with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmpfile:
                                    tmpfile.write(file_content)
                                    tmpfile.flush()
                                    df = df_from_whatsapp(
                                        tmpfile.name,
                                        auto_header=False,
                                        hformat=hformat
                                    )
                                logger.debug("Parsing result DataFrame shape: %s", df.shape)
                                if not df.empty:
                                    logger.debug("Valid WhatsApp chatlog found: %s with format: %s", p.name, hformat)
                                    validation.set_status_code(0)
                                    found = True
                                    break
                                else:
                                    logger.debug("Parsed DataFrame is empty for file: %s with format: %s", p.name, hformat)
                            except Exception as e:
                                logger.debug("Format %s failed to parse file %s: %s", hformat, p.name, e)
                        
                        if found:
                            break
            
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

def anonymize_chatlog(df: pd.DataFrame) -> pd.DataFrame:
    """
    Anonymizes the chatlog DataFrame by replacing usernames with user_1, user_2, etc.,
    and removing these names from the message body as well.
    """
    if df.empty or 'username' not in df.columns or 'message' not in df.columns:
        return df

    # Map unique usernames to user_1, user_2, ...
    unique_users = {name: f"user_{i+1}" for i, name in enumerate(df['username'].unique())}
    df['username'] = df['username'].map(unique_users)

    
    # Replace usernames in message body with their anonymized versions
    def replace_names_in_message(msg):
        for orig, anon in unique_users.items():
            # Use word boundaries to avoid partial replacements
            msg = re.sub(rf'\b{re.escape(str(orig))}\b', anon, msg)
        return msg

    df['message'] = df['message'].astype(str).apply(replace_names_in_message)
    
    return df



def extract_links(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts and normalizes links from the 'message' column of the DataFrame.
    Strips 'www.' from domains and handles broken/IPv6 URLs gracefully.
    """
    if df.empty or 'message' not in df.columns:
        return pd.DataFrame(columns=["link", "domain"])

    url_pattern = r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.-]+\.[a-z]{2,})(?:[^\s()<>]+|\([^\s()<>]+\))*)'

    mask = df['message'].str.contains(url_pattern, na=False)
    link_rows = df[mask]

    results = []
    for idx in link_rows.index:
        message = df.loc[idx, 'message']
        found_links = re.findall(url_pattern, message)
        date = df.loc[idx, 'date'] if 'date' in df.columns else None

        for link in found_links:
            # Füge http:// hinzu, wenn kein Protokoll vorhanden
            if not link.startswith(('http://', 'https://')):
                link = 'http://' + link

            try:
                parsed = urlparse(link)
                domain = parsed.netloc.lower()
                if domain.startswith("www."):
                    domain = domain[4:]
                results.append({
                    "date": date,
                    "link": link,
                    "domain": domain
                })
            except ValueError as e:
                logger.warning("Invalid URL skipped: %s (%s)", link, e)
                continue

    return pd.DataFrame(results)

def extract_links_with_context(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or 'message' not in df.columns:
        return pd.DataFrame(columns=["link", "context", "message"])

    url_pattern = r'(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.-]+\.[a-z]{2,})(?:[^\s()<>]+|\([^\s()<>]+\))*)'

    # Zeilen mit Links finden
    mask = df['message'].str.contains(url_pattern, na=False)
    link_rows = df[mask]

    results = []
    for idx in link_rows.index:
        message = df.loc[idx, 'message']
        found_links = re.findall(url_pattern, message)

        # Kontext definieren (±5 Nachrichten)
        start = max(0, idx - 5)
        end = min(len(df), idx + 6)  # +6, weil exklusiv

        context_messages = df.iloc[start:end]['message'].tolist()



        for link in found_links:
            results.append({
                "link": link,
                "context": context_messages,
                "message": message
            })

    return pd.DataFrame(results)

def clean_chatlog(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the chatlog DataFrame by removing rows where all values are NaN.
    This is useful to ensure that the DataFrame does not contain empty rows.
    """
    if df.empty:
        return df
    # Drop rows where all values are NaN
    #logger.debug("Cleaning chatlog DataFrame, initial shape: %s", df.shape)
    df = df.dropna(how='all')
    df = df[df["message"].notna() & (df["message"].str.strip() != "<Medien ausgeschlossen>")]    
    #logger.debug("Shape after removing '<Medien ausgeschlossen>': %s", df.shape)

    return df


def chatlog_to_df(whatsapp_zip: str, chat_filename: str = None) -> pd.DataFrame:
    """
    Extracts WhatsApp chatlog from zip and parses it into a DataFrame.
    If chat_filename is None, tries to find the first .txt file with 'WhatsApp Chat' in the name.
    """
    # Find the chatlog file
    chat_filename = None
    out = pd.DataFrame()
    
    # Supported formats for different WhatsApp export versions
    formats_to_try = [
        "[%d.%m.%y, %H:%M:%S] %name:",  # New format with brackets and seconds
        "%d.%m.%y, %H:%M - %name:",      # Old format without seconds
    ]

    with zipfile.ZipFile(whatsapp_zip, "r") as zf:
        for f in zf.namelist():
            if f.endswith(".txt") and is_known_file(f):
                chat_filename = f
                break

        if chat_filename is None:
            logger.error("No WhatsApp chatlog found in zip")
            return pd.DataFrame()

        with zf.open(chat_filename) as chat_file:
            file_content = chat_file.read()
            
            # Try each format
            for hformat in formats_to_try:
                try:
                    logger.debug("Attempting to parse with format: %s", hformat)
                    
                    # Write the chat_file to a temporary file because whatstk expects a file path
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as tmpfile:
                        tmpfile.write(file_content)
                        tmpfile.flush()
                        out = df_from_whatsapp(
                            tmpfile.name,
                            auto_header=False,
                            hformat=hformat
                        )
                    
                    if out is not None and not out.empty:
                        logger.debug("Successfully parsed with format: %s", hformat)
                        logger.debug("Parsed DataFrame: %s", out.head())
                        break
                    else:
                        logger.debug("DataFrame empty for format: %s", hformat)
                except Exception as e:
                    logger.debug("Format %s failed: %s", hformat, e)
            
            if out is None or out.empty:
                logger.debug("Failed to parse with any format")
                out = pd.DataFrame()
        
        out = clean_chatlog(out)
        out = anonymize_chatlog(out)
        #out = filter_to_links(out)
        out = out.reset_index(drop=True)
        logger.debug("After cleaning, returning the following dataframe chatlog: %s", out.head())
    return out

