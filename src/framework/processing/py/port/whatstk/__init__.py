"""Python wrapper and analysis tools for WhatsApp chats.

This library provides a powerful wrapper for multiple Languages and OS. In addition, analytics tools are provided.

"""


from .whatsapp.objects import WhatsAppChat
from .graph import FigureBuilder
from .whatsapp.parser import df_from_txt_whatsapp, df_from_whatsapp


name = "whatstk"

__version__ = "0.7.1"

__all__ = [
    "WhatsAppChat",
    "df_from_txt_whatsapp",
    "df_from_whatsapp",
    "FigureBuilder",
]
