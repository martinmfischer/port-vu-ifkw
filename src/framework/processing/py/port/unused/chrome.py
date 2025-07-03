"""
DDP extract Chrome
"""
from pathlib import Path
import logging
import zipfile

import pandas as pd
from lxml import etree

import port.unzipddp as unzipddp
import port.helpers as helpers

from port.validate import (
    DDPCategory,
    Language,
    DDPFiletype,
    ValidateInput,
    StatusCode,
)

logger = logging.getLogger(__name__)


DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "Autofill.json",
            "Bookmarks.html",
            "BrowserHistory.json",
            "Device Information.json",
            "Dictionary.csv",
            "Extensions.json",
            "Omnibox.json",
            "OS Settings.json",
            "ReadingList.html",
            "SearchEngines.json",
            "SyncSettings.json",
        ],
    ),
    DDPCategory(
        id="json_nl",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.NL,
        known_files=[
            "Adressen en meer.json",
            "Bookmarks.html",
            "Geschiedenis.json",
            "Leeslijst.html",
            "Woordenboek.csv",
            "Apparaatgegevens.json",
            "Extensies.json",
            "Instellingen.json",
            "OS-instellingen.json",
        ],
    ),
]



STATUS_CODES = [
    StatusCode(id=0, description="Valid zip", message=""),
    StatusCode(id=1, description="Not a valid DDP", message=""),
    StatusCode(id=2, description="Bad zipfile", message=""),
]


def validate(zfile: Path) -> ValidateInput:
    """
    Validates the input of an Youtube zipfile

    This function sets a validation object generated with ValidateInput
    This validation object can be read later on to infer possible problems with the zipfile
    I dont like this design myself, but I also havent found any alternatives that are better
    """

    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".json", ".csv", ".html"):
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


# Extract BrowserHistory
def browser_history_to_df(chrome_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(chrome_zip, "Geschiedenis.json")
    d = unzipddp.read_json_from_bytes(b)

    if not d:
        b = unzipddp.extract_file_from_zip(chrome_zip, "BrowserHistory.json")
        d = unzipddp.read_json_from_bytes(b)

    if not d:
        b = unzipddp.extract_file_from_zip(chrome_zip, "History.json")
        d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["Browser History"]
        for item in items:
            datapoints.append((
                item.get("title", None),
                item.get("url", None),
                item.get("page_transition", None),
                helpers.epoch_to_iso(item.get("time_usec", None) / 1000000)
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Url", "Transition", "Date"])
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


# Extract Bookmarks
def bookmarks_to_df(chrome_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(chrome_zip, "Bookmarks.html")
    out = pd.DataFrame()
    datapoints = []

    try:
        tree = etree.HTML(b.read())
        r = tree.xpath(f"//a")

        for e in r:
            datapoints.append((
                e.text,
                f"{e.get('href')}"
            ))

        out = pd.DataFrame(datapoints, columns=["Bookmark", "Url"])
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


# Extract Omnibox
def omnibox_to_df(chrome_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(chrome_zip, "Omnibox.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["Typed Url"]
        for item in items:
            datapoints.append((
                item.get("title", None),
                len(item.get("visits", [])),
                item.get("url", None),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Number of visits", "Url"])
        out = out.sort_values(by="Number of visits", ascending=False).reset_index()
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


