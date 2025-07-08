import logging
import json
import io
from typing import Optional, Literal


import pandas as pd

import port.api.props as props
import port.helpers as helpers
import port.validate as validate
import port.facebook as facebook
import port.whatsapp as whatsapp

import port.whatstk


from port.api.commands import (CommandSystemDonate, CommandUIRender, CommandSystemExit)

LOG_STREAM = io.StringIO()

logging.basicConfig(
    #stream=LOG_STREAM,
    level=logging.DEBUG,
    format="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("script")


def process(session_id):
    LOGGER.info("Starting the donation flow")
    yield donate_logs(f"{session_id}-tracking")

    platforms = [ 
        
        ("Facebook", extract_facebook, facebook.validate),
        ("Top WhatsApp-Chat 1", extract_whatsapp, whatsapp.validate), 
        ("Top WhatsApp-Chat 2", extract_whatsapp, whatsapp.validate), 
        ("Top WhatsApp-Chat 3", extract_whatsapp, whatsapp.validate), 
    ]

    LOGGER.info("Platforms to process: %s", platforms)

    # progress in %
    subflows = len(platforms)
    steps = 2
    step_percentage = (100 / subflows) / steps
    progress = 0

    # For each platform
    # 1. Prompt file extraction loop
    # 2. In case of succes render data on screen
    for platform in platforms:
        platform_name, extraction_fun, validation_fun = platform

        table_list = None
        progress += step_percentage

        # Prompt file extraction loop
        while True:
            LOGGER.info("Prompt for file for %s", platform_name)
            yield donate_logs(f"{session_id}-tracking")

            # Render the propmt file page
            # Accept zip, plain text, json, and files without extension (empty string for extension)
            promptFile = None
            if "WhatsApp" in platform_name:
                promptFile = prompt_file("", platform_name)
            else:
                promptFile = prompt_file("application/zip, text/plain, application/json", platform_name)
            
            file_result = yield render_donation_page(platform_name, promptFile, progress)

            if file_result.__type__ == "PayloadString":
                validation = validation_fun(file_result.value)

                # DDP is recognized: Status code zero
                if validation.status_code.id == 0: 
                    LOGGER.info("Payload for %s", platform_name)
                    yield donate_logs(f"{session_id}-tracking")

                    table_list = extraction_fun(file_result.value, validation)
                    break

                # DDP is not recognized: Different status code
                if validation.status_code.id != 0: 
                    LOGGER.info("Not a valid %s zip; No payload; prompt retry_confirmation", platform_name)
                    LOGGER.info("Status code: %s", validation.status_code.id)
                    LOGGER.info("Status code: %s", validation)
                    yield donate_logs(f"{session_id}-tracking")
                    retry_result = yield render_donation_page(platform_name, retry_confirmation(platform_name), progress)

                    if retry_result.__type__ == "PayloadTrue":
                        continue
                    else:
                        LOGGER.info("Skipped during retry %s", platform_name)
                        yield donate_logs(f"{session_id}-tracking")
                        break
            else:
                LOGGER.info("Skipped %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")
                break

        progress += step_percentage

        # Render data on screen
        if table_list is not None:
            LOGGER.info("Prompt consent; %s", platform_name)
            LOGGER.debug("Table list: %s", table_list)
            yield donate_logs(f"{session_id}-tracking")

            # Check if extract something got extracted
            if len(table_list) == 0:
                table_list.append(create_empty_table(platform_name))

            prompt = assemble_tables_into_form(table_list)
            consent_result = yield render_donation_page(platform_name, prompt, progress)

            if consent_result.__type__ == "PayloadJSON":
                LOGGER.info("Data donated; %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")
                yield donate(platform_name, consent_result.value)
            else:
                LOGGER.info("Skipped ater reviewing consent: %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")

    yield exit(0, "Success")
    yield render_end_page()



##################################################################

def assemble_tables_into_form(table_list: list[props.PropsUIPromptConsentFormTable]) -> props.PropsUIPromptConsentForm:
    """
    Assembles all donated data in consent form to be displayed
    """
    return props.PropsUIPromptConsentForm(table_list, [])


def donate_logs(key):
    log_string = LOG_STREAM.getvalue()  # read the log stream
    if log_string:
        log_data = log_string.split("\n")
    else:
        log_data = ["no logs"]

    return donate(key, json.dumps(log_data))


def create_empty_table(platform_name: str) -> props.PropsUIPromptConsentFormTable:
    """
    Show something in case no data was extracted
    """
    title = props.Translatable({
       "en": "Er ging niks mis, maar we konden niks vinden",
       "nl": "Er ging niks mis, maar we konden niks vinden",
        "de": "Es ist nichts schiefgegangen, aber wir konnten nichts finden"
    })
    df = pd.DataFrame(["No data found"], columns=["No data found"])
    table = props.PropsUIPromptConsentFormTable(f"{platform_name}_no_data_found", title, df)
    return table


##################################################################
# Visualization helpers
def create_chart(type: Literal["bar", "line", "area"], 
                 nl_title: str, en_title: str, de_title: str,
                 x: str, y: Optional[str] = None, 
                 x_label: Optional[str] = None, y_label: Optional[str] = None,
                 date_format: Optional[str] = None, aggregate: str = "count", addZeroes: bool = True):
    if y is None:
        y = x
        if aggregate != "count": 
            raise ValueError("If y is None, aggregate must be count if y is not specified")
        
    return props.PropsUIChartVisualization(
        title = props.Translatable({"en": en_title, "nl": nl_title, "de": de_title}),
        type = type,
        group = props.PropsUIChartGroup(column= x, label= x_label, dateFormat= date_format),
        values = [props.PropsUIChartValue(column= y, label= y_label, aggregate= aggregate, addZeroes= addZeroes)]       
    )

def create_wordcloud(nl_title: str, en_title: str, de_title: str, column: str, 
                     tokenize: bool = False, 
                     value_column: Optional[str] = None, 
                     extract: Optional[Literal["url_domain"]] = None):
    return props.PropsUITextVisualization(title = props.Translatable({"en": en_title, "nl": nl_title, "de": en_title}),
                                          type='wordcloud',
                                          text_column=column,
                                          value_column=value_column,
                                          tokenize=tokenize,
                                          extract=extract)


##################################################################
# Extraction functions

def extract_facebook(facebook_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    df = facebook.group_interactions_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook group interactions", "nl": "Facebook group interactions", "de": "Facebook Gruppeninteraktionen"})
        vis = [create_wordcloud("Groepen met meeste interacties", "Groups with most interactions", "Gruppen mit den meisten Interaktionen", "Group name", value_column="Times Interacted")]
        table =  props.PropsUIPromptConsentFormTable("facebook_group_interactions", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = facebook.comments_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook comments", "nl": "Facebook comments", "de": "Facebook Kommentare"})
        vis = [create_wordcloud("Meest voorkomende woorden in comments", "Most common words in comments", "Häufigste Wörter in Kommentaren", "Comment", tokenize=True)]
        table =  props.PropsUIPromptConsentFormTable("facebook_comments", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = facebook.likes_and_reactions_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook likes and reactions", "nl": "Facebook likes and reactions", "de": "Facebook Likes und Reaktionen"})
        vis = [create_chart('bar', "Meest gebruikte reacties", "Most used reactions", "Häufigsten Reaktionen", "Reaction")]
        table =  props.PropsUIPromptConsentFormTable("facebook_likes_and_reactions", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = facebook.your_badges_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook your badges", "nl": "Facebook your badges", "de": "Facebook Ihre Abzeichen"})
        table =  props.PropsUIPromptConsentFormTable("facebook_your_badges", table_title, df) 
        tables_to_render.append(table)

    df = facebook.your_posts_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook your posts", "nl": "Facebook your posts" , "de": "Facebook Ihre Beiträge"})
        table =  props.PropsUIPromptConsentFormTable("facebook_your_posts", table_title, df) 
        tables_to_render.append(table)

    df = facebook.your_search_history_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook your searh history", "nl": "Facebook your search history", "de": "Facebook Ihre Suchhistorie"})
        vis = [create_wordcloud("Meest gebruikte zoektermen", "Most used search terms", "Suchanfragen", "Search Term", tokenize=True)]
        table =  props.PropsUIPromptConsentFormTable("facebook_your_search_history", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    df = facebook.recently_viewed_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook recently viewed", "nl": "Facebook recently viewed", "de": "Facebook Kürzlich Angesehenes"})
        table =  props.PropsUIPromptConsentFormTable("facebook_recently_viewed", table_title, df) 
        tables_to_render.append(table)

    df = facebook.recently_visited_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook recently visited", "nl": "Facebook recently visited", "de": "Facebook Kürzlich Besuchte"})
        table =  props.PropsUIPromptConsentFormTable("facebook_recently_visited", table_title, df) 
        tables_to_render.append(table)

    df = facebook.feed_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook feed", "nl": "Facebook feed", "de": "Facebook Feed"})
        table =  props.PropsUIPromptConsentFormTable("facebook_feed", table_title, df) 
        tables_to_render.append(table)

    df = facebook.controls_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook controls", "nl": "Facebook controls", "de": "Facebook Einstellungen"})
        table =  props.PropsUIPromptConsentFormTable("facebook_controls", table_title, df) 
        tables_to_render.append(table)

    df = facebook.group_posts_and_comments_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook group posts and comments", "nl": "Facebook group posts and comments", "de": "Facebook Gruppenbeiträge und Kommentare"})
        table =  props.PropsUIPromptConsentFormTable("facebook_group_posts_and_comments", table_title, df) 
        tables_to_render.append(table)
        
    df = facebook.your_posts_check_ins_photos_and_videos_1_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook your posts check ins photos and videos", "nl": "Facebook group posts and comments", "de": "Facebook Ihre Beiträge, Check-Ins, Fotos und Videos"})
        table =  props.PropsUIPromptConsentFormTable("facebook_your_posts_check_ins_photos_and_videos", table_title, df) 
        tables_to_render.append(table)

    return tables_to_render


def extract_whatsapp(whatsapp_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    import port.whatsapp as whatsapp

    tables_to_render = []

    # Extract chat messages
    df = whatsapp.chatlog_to_df(whatsapp_zip)
    LOGGER.debug("Extracted WhatsApp chat messages DataFrame shape in script.py: %s", df.shape)
    LOGGER.debug("head of extracted messages in script.py: %s", df.head())
    # Convert all cells to strings:
    df = df.astype(str)
    if not df.empty:
        table_title = props.Translatable({"en": "WhatsApp chat messages", "nl": "WhatsApp chatberichten", "de": "WhatsApp Chatnachrichten"})
        vis = [
            create_wordcloud(
                "Meest voorkomende woorden in chats",
                "Most common words in chats",
                "Häufigste Wörter in Chats",
                "message",
                tokenize=True
            ),
            create_chart(
                "bar",
                "Aantal berichten per contact",
                "Number of messages per contact",
                "Anzahl der Nachrichten pro Kontakt",
                "username"
            )
        ]
        table = props.PropsUIPromptConsentFormTable("whatsapp_chats", table_title, df, visualizations=vis)
        tables_to_render.append(table)

    return tables_to_render


##########################################
# Functions provided by Eyra did not change

def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)


def render_donation_page(platform, body, progress):
    header = props.PropsUIHeader(props.Translatable({"en": platform, "nl": platform, "de": platform}))

    footer = props.PropsUIFooter(progress)
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)


def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": f"Unfortunately, we could not process your {platform} file. If you are sure that you selected the correct file, press Continue. To select a different file, press Try again.",
            "nl": f"Helaas, kunnen we uw {platform} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen.",
            "de": f"Leider konnten wir Ihre {platform}-Datei nicht verarbeiten. Wenn Sie sicher sind, dass Sie die richtige Datei ausgewählt haben, klicken Sie auf Weiter. Um eine andere Datei auszuwählen, klicken Sie auf Erneut versuchen."
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw", "de": "Erneut versuchen"})
    cancel = props.Translatable({"en": "Continue", "nl": "Verder", "de": "Weiter"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def prompt_file(extensions, platform):
    description = props.Translatable(
        {
            "en": f"Please follow the download instructions and choose the file that you stored on your device. Click “Skip” at the right bottom, if you do not have a file from {platform}.",
            "nl": f"Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat. Als u geen {platform} bestand heeft klik dan op “Overslaan” rechts onder.",
            "de": f"Befolgen Sie bitte die Download-Anweisungen und wählen Sie die Datei aus, die Sie auf Ihrem Gerät gespeichert haben. Klicken Sie unten rechts auf „Überspringen“, wenn Sie keine Datei von {platform} haben."
        }
    )
    return props.PropsUIPromptFileInput(description, extensions)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)

def exit(code, info):
    return CommandSystemExit(code, info)
