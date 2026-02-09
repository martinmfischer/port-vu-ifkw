"""
DDP facebook module

This module contains functions to handle *.jons files contained within a facebook ddp
"""
from pathlib import Path
from typing import Any
import math
import logging
import zipfile
import re
import io
import pandas as pd
import datetime
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
        id="json_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "events_interactions.json",
            "group_interactions.json",
            "people_and_friends.json",
            "advertisers_using_your_activity_or_information.json",
            "advertisers_you've_interacted_with.json",
            "apps_and_websites.json",
            "your_off-facebook_activity.json",
            "comments.json",
            "posts_and_comments.json",
            "event_invitations.json",
            "your_event_responses.json",
            "accounts_center.json",
            "marketplace_notifications.json",
            "payment_history.json",
            "controls.json",
            "reduce.json",
            "friend_requests_received.json",
            "friend_requests_sent.json",
            "friends.json",
            "rejected_friend_requests.json",
            "removed_friends.json",
            "who_you_follow.json",
            "your_comments_in_groups.json",
            "your_group_membership_activity.json",
            "your_posts_in_groups.json",
            "primary_location.json",
            "primary_public_location.json",
            "timezone.json",
            "notifications.json",
            "pokes.json",
            "ads_interests.json",
            "friend_peer_group.json",
            "pages_and_profiles_you_follow.json",
            "pages_and_profiles_you've_recommended.json",
            "pages_and_profiles_you've_unfollowed.json",
            "pages_you've_liked.json",
            "polls_you_voted_on.json",
            "your_uncategorized_photos.json",
            "your_videos.json",
            "language_and_locale.json",
            "live_video_subscriptions.json",
            "profile_information.json",
            "profile_update_history.json",
            "your_local_lists.json",
            "your_saved_items.json",
            "your_search_history.json",
            "account_activity.json",
            "authorized_logins.json",
            "browser_cookies.json",
            "email_address_verifications.json",
            "ip_address_activity.json",
            "login_protection_data.json",
            "logins_and_logouts.json",
            "mobile_devices.json",
            "record_details.json",
            "where_you're_logged_in.json",
            "your_facebook_activity_history.json",
            "archived_stories.json",
            "location.json",
            "recently_viewed.json",
            "recently_visited.json",
            "your_topics.json",
        ],
    )
]

DDP_TO_RETAIN = [
            "events_interactions.json",
            "group_interactions.json",
            "comments.json",
            "posts_and_comments.json",
            "who_you_follow.json",
            "your_comments_in_groups.json",
            "your_group_membership_activity.json",
            "your_posts_in_groups.json",
            "pages_and_profiles_you_follow.json",
            "pages_and_profiles_you've_recommended.json",
            "pages_and_profiles_you've_unfollowed.json",
            "pages_you've_liked.json",
            "language_and_locale.json",
            "profile_information.json",
]


STATUS_CODES = [
    StatusCode(id=0, description="Valid DDP", message=""),
    StatusCode(id=1, description="Not a valid DDP", message=""),
    StatusCode(id=2, description="Bad zipfile", message=""),
]

def return_items_based_on_key_pattern(d: dict[Any, Any], keys_to_match: list[str]) -> list[Any]:
    """
    Returns a list of items from a dictionary that match any of the keys in keys_to_match.
    d is a denested dict
    keys_to_match is a list of strings to match against the keys in d
    If no keys match, an empty list is returned.
    :param d: Dictionary to search in
    :param keys_to_match: List of keys to match against the keys in d
    :return: List of items that match any of the keys in keys_to_match

    """
    #key_list = []
    #out_list = []

   # for key in d.keys():
    #    for key_to_match in keys_to_match:
     #       if key_to_match in key:
      #          key_list.append(key)

    #for fitting_key in key_list:
    #    out_list.append(d.get(fitting_key))        
    #return out_list
    return [v for k, v in d.items() if any(match in k for match in keys_to_match)]

def return_files_based_on_filenames(zip_path: str, filenames: list[str] | str) -> list[io.BytesIO]:
    """
    Returns a list of files from a zipfile that match any of the filenames in filenames.
    :param zipfile: Path to the zipfile
    :param filenames: List of filenames to match against the files in the zipfile
    :return: List of files that match any of the filenames in filenames
    """
    matching_files = []

    if isinstance(filenames, str):
        filenames = [filenames]

    for filename in filenames:
        try:
            content = unzipddp.extract_file_from_zip(zip_path, filename)

            # Prüfe, ob das zurückgegebene BytesIO leer ist
            content.seek(0, io.SEEK_END)
            size = content.tell()
            content.seek(0)  # Reset auf Anfang

            if size == 0:
                logger.warning("return_files_based_on_filenames: File found but empty (probably missing in zip): %s", filename)
                continue  # Datei ignorieren, da leer / vermutlich nicht gefunden

            matching_files.append(content)
            logger.debug("return_files_based_on_filenames: Found and extracted file in zip: %s", filename)

        except Exception as e:
            logger.error("return_files_based_on_filenames: Unexpected error while extracting %s: %s", filename, e, exc_info=True)

    return matching_files

def validate(zfile: Path) -> ValidateInput:
    """
    Validates the input of an Facebook zipfile
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
        if validation.ddp_category is None or validation.ddp_category.id is None:
            validation.set_status_code(1)
        else:
            validation.set_status_code(0)

    except zipfile.BadZipFile:
        validation.set_status_code(2)

    return validation


def likes_to_df(facebook_zip: str) -> pd.DataFrame:
    """
    Extracts likes from a facebook zip file and returns them as a pandas DataFrame
    """

    b = return_files_based_on_filenames(facebook_zip, "likes.json")[0]
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        
        items = return_items_based_on_key_pattern(d, ["likes"])
        #items = d["likes"]
        for item in items:
            datapoints.append((
                unzipddp.fix_mojibake(item.get("title", "")),
                item["data"][0].get("like", {}).get("like", ""),
                helpers.epoch_to_iso(item.get("timestamp", {}))
            ))
        out = pd.DataFrame(datapoints, columns=["Action", "Like", "Date"])

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out





def follows_to_df(facebook_zip: str) -> pd.DataFrame:  
    """
    Extracts follows from a facebook zip file and returns them as a pandas DataFrame
    """
    logger.debug("Extracting follows from facebook zip!" )
    filenames = [
        "who_you've_followed.json",
        "who_you_follow.json"
    ]

    rows = []
    found_files = return_files_based_on_filenames(facebook_zip, filenames)
    if len(found_files) == 0:
        logger.warning("No follows found in facebook zip file!")
        return pd.DataFrame()
    
    for found in found_files:
        try:
            d = unzipddp.read_json_from_bytes(found) ### THIS DOES NOT WORK RIGHT NOW, THROWS ERROR FAILING TO DECODE JSON
        except Exception as e:
            logger.error("Follows_to_df: Failed to decode JSON from file: %s, error: %s", found, e)
            continue
    
        logger.debug("!! Extracting followed persons from file: %s", found)
        #logger.debug("Following keys: %s", following_keys)

        
        items = return_items_based_on_key_pattern(d, ["follow", "follows"])
        logger.debug("Items found: %s", items)
        if len(items) == 0:
            logger.warning("No follows found in file: %s", found)
            continue
        if len(items) == 1 and isinstance(items[0], list):
            # Likely doubled list, denesting
            items = items[0]

        for item in items:
            logger.debug("Item type is %s", type(item))
            logger.debug("Item content: %s", item)
            # defensive extraction
            name = unzipddp.fix_mojibake(item.get("name", None))
            title = item.get("title", None)
            timestamp = helpers.epoch_to_iso(item.get("timestamp", None))

            rows.append({
                "name": name,
                "title": title,
                "timestamp": timestamp
            })
    
    out = pd.DataFrame(rows)
    logger.debug("Length of extracted follows: %s", len(out))
    return out

def followed_pages_to_df(facebook_zip: str) -> pd.DataFrame:
    """
    Extracts followed pages from a facebook zip file and returns them as a pandas DataFrame

    DDP structure often looks like this:

      "pages_followed_v2": [
    {
      "timestamp": 1363970068,
      "data": [
        {
          "name": "Pendulum"
        }
      ],
      "title": "Pendulum"
    },

    """

    #logger.debug("Extracting followed pages from facebook zip!" )

    #logger.debug("Extracting follows from facebook zip!" )
    filenames = [
        "pages_and_profiles_you_follow.json"
    ]
    found_files = return_files_based_on_filenames(facebook_zip, filenames)
    rows = []
    logger.debug("Found number of files: %s", len(found_files))
    for found in found_files:
        d = unzipddp.read_json_from_bytes(found)


        following_keys = d.keys()
        logger.debug("Extracting followed persons from file: %s", found)
        logger.debug("Following keys: %s", following_keys)

        items = []
        #logger.debug("We got the following items after denesting the dict in the json: %s", items)
        for key in following_keys:
            # Make sure we are not gettin something completely random
            if not "follow" in key:
                logger.warning("Key %s does not contain 'follow', consider adjusting/skipping", key)
            items.append(d.get(key, []))


        #logger.debug("Length of items: %s", len(items))


        for list_of_dicts in items:
            for dictionary in list_of_dicts:

            
            #if not isinstance(dictionary, dict):
            #    logger.warning("dictionary is not a dict: %s", dictionary)
            #    if (isinstance(dictionary, list) and len(dictionary) == 1):
            #        dictionary = dictionary[0]
            #    else:
            #        logger.warning("Item is not a list with one element, skipping: %s", dictionary)
            #        continue
            

                #logger.debug("Item type is %s", type(dictionary))
                #logger.debug("Item content: %s", dictionary)
                name = unzipddp.fix_mojibake(dictionary["data"][0]["name"])
                title = dictionary.get("title", None)
                timestamp = helpers.epoch_to_iso(dictionary.get("timestamp", None))

                rows.append({
                    "name": name,
                    "title": title,
                    "timestamp": timestamp
                })

    out = pd.DataFrame(rows)

    return out


def comments_to_df(facebook_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(facebook_zip, "comments.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []
    
    try:
        #There are different versions, each with a different suffix like _v2, but all starting with comments. Adjusting for that

        dict_list = return_items_based_on_key_pattern(d, ["comments"])

        if len(dict_list) == 0:
            logger.warning("No comments found in file: %s", facebook_zip)
            return pd.DataFrame()
        
        if len(dict_list) == 1 and isinstance(dict_list, list):
            #Likely doubled list, denesting
            dict_list = dict_list[0]
        #logger.debug("dict_list has length %s and looks like that: %s", len(dict_list), dict_list)

        
        for d in dict_list:
            #logger.debug("d has length %s and looks like that: %s", len(d), d)
            
            title = unzipddp.fix_mojibake(d.get("title", ""))
            timestamp = helpers.epoch_to_iso(d.get("timestamp", None))

            # Robust aus dem inneren 'comment' ziehen
            comment_text = ""
            data = d.get("data", [])
            if data and isinstance(data, list):
                first_data = data[0]
                if isinstance(first_data, dict):
                    comment_obj = first_data.get("comment", {})
                    comment_text = unzipddp.fix_mojibake(comment_obj.get("comment", ""))

            datapoints.append((title, comment_text, timestamp))
        out = pd.DataFrame(datapoints, columns=["Action", "Comment", "Date"])

    except Exception as e:
        logger.error("Exception caught in comments_to_df: %s", e)

    return out



def likes_and_reactions_to_df(facebook_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(facebook_zip, "likes_and_reactions_1.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        for item in d:
            datapoints.append((
                item.get("title", ""),
                item["data"][0].get("reaction", {}).get("reaction", ""),
                helpers.epoch_to_iso(item.get("timestamp", {}))
            ))
        out = pd.DataFrame(datapoints, columns=["Action", "Reaction", "Date"])

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def find_items(d: dict[Any, Any],  key_to_match: str) -> str:
    """
    d is a denested dict
    match all keys in d that contain key_to_match

    return the value beloning to that key that are the least nested
    In case of no match return empty string

    example:
    key_to_match = asd

    asd-asd-asd-asd-asd-asd: 1
    asd-asd: 2
    qwe: 3

    returns 2

    This function is needed because your_posts_1.json contains a wide variety of nestedness per post
    """
    out = ""
    pattern = r"{}".format(f"^.*{key_to_match}.*$")
    depth = math.inf

    try:
        for k, v in d.items():
            if re.match(pattern, k):
                depth_current_match = k.count("-")
                if depth_current_match < depth:
                    depth = depth_current_match
                    out = str(v)
    except Exception as e:
        logger.error("bork bork: %s", e)

    return out
            


def recently_viewed_to_df(facebook_zip: str) -> pd.DataFrame:
    b = unzipddp.extract_file_from_zip(facebook_zip, "recently_viewed.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["recently_viewed"]
        for item in items:

            if "entries" in item:
                for entry in item["entries"]:
                    datapoints.append((
                        item.get("name", ""),
                        entry.get("data", {}).get("name", ""),
                        entry.get("data", {}).get("uri", ""),
                        helpers.epoch_to_iso(entry.get("timestamp"))
                    ))

            # The nesting goes deeper
            if "children" in item:
                for child in item["children"]:
                    for entry in child["entries"]:
                        datapoints.append((
                            child.get("name", ""),
                            entry.get("data", {}).get("name", ""),
                            entry.get("data", {}).get("uri", ""),
                            helpers.epoch_to_iso(entry.get("timestamp"))
                        ))

        out = pd.DataFrame(datapoints, columns=["Watched", "Name", "Link", "Date"])
        out = out.sort_values(by="Date", key=helpers.sort_isotimestamp_empty_timestamp_last)

    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out



def recently_visited_to_df(facebook_zip: str) -> pd.DataFrame:
    b = unzipddp.extract_file_from_zip(facebook_zip, "recently_visited.json")
    d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        items = d["visited_things_v2"]
        for item in items:
            if "entries" in item:
                for entry in item["entries"]:
                    datapoints.append((
                        item.get("name", ""),
                        entry.get("data", {}).get("name", ""),
                        entry.get("data", {}).get("uri", ""),
                        helpers.epoch_to_iso(entry.get("timestamp"))
                    ))
        out = pd.DataFrame(datapoints, columns=["Watched", "Name", "Link", "Date"])
        out = out.sort_values(by="Date", key=helpers.sort_isotimestamp_empty_timestamp_last)
        
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


def group_posts_and_comments_to_df(facebook_zip: str) -> pd.DataFrame:

    b = unzipddp.extract_file_from_zip(facebook_zip, "group_posts_and_comments.json")
    d = unzipddp.read_json_from_bytes(b)

    if not d:
        b = unzipddp.extract_file_from_zip(facebook_zip, "your_posts_in_groups.json")
        d = unzipddp.read_json_from_bytes(b)

    out = pd.DataFrame()
    datapoints = []

    try:
        l = d["group_posts_v2"]
        for item in l:
            denested_dict = helpers.dict_denester(item)

            datapoints.append((
                find_items(denested_dict, "title"),
                find_items(denested_dict, "post"),
                find_items(denested_dict, "comment"), # There are no comments in my test data, this is a guess!!
                helpers.epoch_to_iso(find_items(denested_dict, "timestamp")),
                find_items(denested_dict, "url"),
            ))

        out = pd.DataFrame(datapoints, columns=["Title", "Post", "Comment", "Date", "Url"])
    except Exception as e:
        logger.error("Exception caught: %s", e)

    return out


