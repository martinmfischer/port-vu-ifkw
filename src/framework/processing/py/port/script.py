import logging
import json
import io
import os
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

    # Get release platform from session ID (passed from frontend)
    # Format: <timestamp>-<platform>
    # Options: 'facebook', 'whatsapp', 'whatsapp:1', 'whatsapp:2', 'whatsapp:3', 'all'
    session_parts = session_id.split('-')
    release_platform = session_parts[-1].lower() if len(session_parts) > 1 else os.getenv("RELEASE_PLATFORM", "all").lower()
    
    all_platforms = [ 
        ("Facebook", extract_facebook, facebook.validate),
        ("Top WhatsApp-Chat 1", extract_whatsapp, whatsapp.validate), 
        ("Top WhatsApp-Chat 2", extract_whatsapp, whatsapp.validate), 
        ("Top WhatsApp-Chat 3", extract_whatsapp, whatsapp.validate), 
    ]
    
    # Filter platforms based on RELEASE_PLATFORM environment variable
    if release_platform == "facebook":
        platforms = [p for p in all_platforms if "Facebook" in p[0]]
    elif release_platform == "whatsapp":
        platforms = [p for p in all_platforms if "WhatsApp" in p[0]]
    elif release_platform.startswith("whatsapp:"):
        # Extract specific WhatsApp chat number (e.g., 'whatsapp:1' -> chat 1)
        try:
            chat_num = int(release_platform.split(":")[1])
            platforms = [p for p in all_platforms if f"WhatsApp-Chat {chat_num}" in p[0]]
        except (ValueError, IndexError):
            LOGGER.warning("Invalid RELEASE_PLATFORM format '%s', using all platforms", release_platform)
            platforms = all_platforms
    else:  # 'all' or default
        platforms = all_platforms

    LOGGER.info("Release platform: %s", release_platform)
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
            #LOGGER.info("Prompt consent; %s", platform_name)
            #LOGGER.debug("Table list: %s", table_list)
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

    df = facebook.follows_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook follows", "nl": "Facebook volgt", "de": "Gefolgte Personen auf Facebook"})
        vis = [create_wordcloud("Meest voorkomende woorden in follöws", "Most common words in followed accounts", "Häufigste Wörter in gefolgten Personen-Accounts", "name", tokenize=True)]
        table =  props.PropsUIPromptConsentFormTable("facebook_follows", table_title, df, visualizations=vis) 
        tables_to_render.append(table)
    else:
        LOGGER.warning("No followed persons found in Facebook data, df empty")

    df = facebook.followed_pages_to_df(facebook_zip)
    if not df.empty:
        table_title = props.Translatable({"en": "Facebook followed pages", "nl": "Facebook gevolgde pagina's", "de": "Gefolgte Seiten auf Facebook"})
        vis = [create_wordcloud("Meest voorkomende woorden in follöws", "Most common words in followed accounts", "Häufigste Wörter in gefolgten Accounts", "name", tokenize=True)]
        table =  props.PropsUIPromptConsentFormTable("facebook_followed_pages", table_title, df, visualizations=vis) 
        tables_to_render.append(table)

    return tables_to_render


def extract_whatsapp(whatsapp_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    import port.whatsapp as whatsapp

    tables_to_render = []

    # Extract chat messages
    df = whatsapp.chatlog_to_df(whatsapp_zip)
    #LOGGER.debug("Extracted WhatsApp chat messages DataFrame shape in script.py: %s", df.shape)
    #LOGGER.debug("head of extracted messages in script.py: %s", df.head())
    # Convert all cells to strings:
    df = df.astype(str)
    if not df.empty:
        """
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
        """
        tbl = whatsapp.extract_links(df)
        if not tbl.empty:
            table_title = props.Translatable({"en": "WhatsApp links", "nl": "WhatsApp links", "de": "WhatsApp Links"})




            vis = [
                create_wordcloud(
                    "Meest voorkomende woorden in links",
                    "Most common words in links",
                    "Häufigste Wörter in Links",
                    "link",
                    tokenize=True,
                    extract="url_domain"
                )
            ]
            table = props.PropsUIPromptConsentFormTable("whatsapp_links_with_context", table_title, tbl, visualizations=vis)


            tables_to_render.append(table)


            
            ### tbl contains the columns link and domain; create new dataframe named domain_tbl that contains the count for each domain in descending order, then only select the top 10 domains
            domain_tbl = tbl['domain'].value_counts().reset_index()
            domain_tbl.columns = ['domain', 'count']
            domain_tbl = domain_tbl.sort_values(by='count', ascending=False)
            table_title = props.Translatable({"en": "Top Domains", "nl": "Top Domains", "de": "Top Domains"})
            vis = []
            #LOGGER.debug("Domain table: %s", domain_tbl)
            table = props.PropsUIPromptConsentFormTable("whatsapp_domains", table_title, domain_tbl, visualizations=vis)
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
            "en": f"Please follow the download instructions and choose the file that you stored on your device.",
            "nl": f"Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat. Als u geen {platform} bestand heeft klik dan op “Overslaan” rechts onder.",
            "de": f"Befolgen Sie bitte die Download-Anweisungen und wählen Sie die Datei aus, die Sie auf Ihrem Gerät gespeichert haben."
        }
    )
    return props.PropsUIPromptFileInput(description, extensions)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)

def exit(code, info):
    return CommandSystemExit(code, info)
