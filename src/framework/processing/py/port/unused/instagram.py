"""
DDP Instagram module

This module contains functions to handle *.jons files contained within an instagram ddp
"""

from pathlib import Path
from typing import Any
from itertools import product
import math
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


logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "secret_conversations.json",
            "personal_information.json",
            "account_privacy_changes.json",
            "account_based_in.json",
            "recently_deleted_content.json",
            "liked_posts.json",
            "stories.json",
            "profile_photos.json",
            "followers.json",
            "signup_information.json",
            "comments_allowed_from.json",
            "login_activity.json",
            "your_topics.json",
            "camera_information.json",
            "recent_follow_requests.json",
            "devices.json",
            "professional_information.json",
            "follow_requests_you've_received.json",
            "eligibility.json",
            "pending_follow_requests.json",
            "videos_watched.json",
            "ads_interests.json",
            "account_searches.json",
            "following.json",
            "posts_viewed.json",
            "recently_unfollowed_accounts.json",
            "post_comments.json",
            "account_information.json",
            "accounts_you're_not_interested_in.json",
            "use_cross-app_messaging.json",
            "profile_changes.json",
            "reels.json",
        ],
    )
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid DDP", message="Valid DDP"),
    StatusCode(id=1, description="Not a valid DDP", message="Not a valid DDP"),
    StatusCode(id=2, description="Bad zipfile", message="Bad zip"),
]


def validate(zfile: Path) -> ValidateInput:
    """
    Validates the input of an Instagram zipfile

    This function should set and return a validation object
    """

    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(zfile, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".html", ".json"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validation.infer_ddp_category(paths)
        if validation.ddp_category.id is not None:
            validation.set_status_code(0)
        else:
            validation.set_status_code(1)

    except zipfile.BadZipFile:
        validation.set_status_code(2)

    return validation


def accounts_not_interested_in_to_df(instagram_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(instagram_zip, "accounts_you're_not_interested_in.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["impressions_history_recs_hidden_authors"]
        for item in items:
            data = item.get("string_map_data", {})
            account_name = data.get("Username", {}).get("value", None),
            if "Time" in data:
                timestamp = data.get("Time", {}).get("timestamp", "")
            else:
                timestamp = data.get("Tijd", {}).get("timestamp", "")

            datapoints.append((
                account_name,
                helpers.epoch_to_iso(timestamp)
            ))
        out = pd.DataFrame(datapoints, columns=["Account name", "Date"])
        out = out.sort_values(by="Date", key=helpers.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def ads_viewed_to_df(instagram_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(instagram_zip, "ads_viewed.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["impressions_history_ads_seen"]
        for item in items:
            data = item.get("string_map_data", {})
            account_name = data.get("Author", {}).get("value", None)
            if "Time" in data:
                timestamp = data.get("Time", {}).get("timestamp", "")
            else:
                timestamp = data.get("Tijd", {}).get("timestamp", "")

            datapoints.append((
                account_name,
                helpers.epoch_to_iso(timestamp)
            ))
        out = pd.DataFrame(datapoints, columns=["Author of ad", "Date"])
        out = out.sort_values(by="Date", key=helpers.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def posts_viewed_to_df(instagram_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(instagram_zip, "posts_viewed.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["impressions_history_posts_seen"]
        for item in items:
            data = item.get("string_map_data", {})
            account_name = data.get("Author", {}).get("value", None)
            if "Time" in data:
                timestamp = data.get("Time", {}).get("timestamp", "")
            else:
                timestamp = data.get("Tijd", {}).get("timestamp", "")

            datapoints.append((
                account_name,
                helpers.epoch_to_iso(timestamp)
            ))
        out = pd.DataFrame(datapoints, columns=["Author", "Date"])
        out = out.sort_values(by="Date", key=helpers.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def posts_not_interested_in_to_df(instagram_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(instagram_zip, "posts_you're_not_interested_in.json")
    data = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = data["impressions_history_posts_not_interested"]
        for item in items:
            d = helpers.dict_denester(item.get("string_list_data"))
            datapoints.append((
                helpers.fix_latin1_string(helpers.find_items(d, "value")),
                helpers.find_items(d, "href"),
                helpers.epoch_to_iso(helpers.find_items(d, "timestamp"))
            ))
        out = pd.DataFrame(datapoints, columns=["Post", "Link", "Date"])
        out = out.sort_values(by="Date", key=helpers.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def videos_watched_to_df(instagram_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(instagram_zip, "videos_watched.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["impressions_history_videos_watched"]
        for item in items:
            data = item.get("string_map_data", {})
            account_name = data.get("Author", {}).get("value", None)
            if "Time" in data:
                timestamp = data.get("Time", {}).get("timestamp", "")
            else:
                timestamp = data.get("Tijd", {}).get("timestamp", "")

            datapoints.append((
                account_name,
                helpers.epoch_to_iso(timestamp)
            ))
        out = pd.DataFrame(datapoints, columns=["Author", "Date"])
        out = out.sort_values(by="Date", key=helpers.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def post_comments_to_df(instagram_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(instagram_zip, "post_comments.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["comments_media_comments"]
        for item in items:
            data = item.get("string_map_data", {})
            media_owner = data.get("Media Owner", {}).get("value", "")
            comment = data.get("Comment", {}).get("value", "")
            if "Time" in data:
                timestamp = data.get("Time", {}).get("timestamp", "")
            else:
                timestamp = data.get("Tijd", {}).get("timestamp", "")

            datapoints.append((
                media_owner,
                helpers.fix_latin1_string(comment),
                helpers.epoch_to_iso(timestamp)
            ))
        out = pd.DataFrame(datapoints, columns=["Media Owner", "Comment", "Date"])
        out = out.sort_values(by="Date", key=helpers.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def following_to_df(instagram_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(instagram_zip, "following.json")
    data = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = data["relationships_following"]
        for item in items:
            d = helpers.dict_denester(item)
            datapoints.append((
                helpers.fix_latin1_string(helpers.find_items(d, "value")),
                helpers.find_items(d, "href"),
                helpers.epoch_to_iso(helpers.find_items(d, "timestamp"))
            ))
        out = pd.DataFrame(datapoints, columns=["Account", "Link", "Date"])
        out = out.sort_values(by="Date", key=helpers.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def liked_comments_to_df(instagram_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(instagram_zip, "liked_comments.json")
    data = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = data["likes_comment_likes"]
        for item in items:
            d = helpers.dict_denester(item)
            datapoints.append((
                helpers.fix_latin1_string(helpers.find_items(d, "value")),
                helpers.find_items(d, "href"),
                helpers.epoch_to_iso(helpers.find_items(d, "timestamp"))
            ))
        out = pd.DataFrame(datapoints, columns=["Value", "Link", "Date"])
        out = out.sort_values(by="Date", key=helpers.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def liked_posts_to_df(instagram_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(instagram_zip, "liked_posts.json")
    data = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = data["likes_media_likes"]
        for item in items:
            d = helpers.dict_denester(item)
            datapoints.append((
                helpers.fix_latin1_string(helpers.find_items(d, "value")),
                helpers.find_items(d, "href"),
                helpers.epoch_to_iso(helpers.find_items(d, "timestamp"))
            ))
        out = pd.DataFrame(datapoints, columns=["Value", "Link", "Date"])
        out = out.sort_values(by="Date", key=helpers.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


