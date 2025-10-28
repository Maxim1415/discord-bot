import pandas as pd
from unidecode import unidecode
from database import load_players

def fmt(value):
    try:
        value = int(value)
        return f"{value:,}".replace(",", " ")
    except (ValueError, TypeError):
        return value

async def nickname_autocomplete(ctx):
    current_value: str = ctx.focused.value or ""
    guild_id = ctx.interaction.guild_id
    pl, _ = load_players(guild_id)

    values_to_recommend = [
        name for name in pl if unidecode(current_value).lower() in unidecode(name).lower()
    ]
    await ctx.respond(values_to_recommend[:25])

async def category_autocomplete(ctx):
    current_value: str = ctx.focused.value or ""
    categories = ["Merits", "Units Killed", "Units Dead"]
    category_to_recommend = [cat for cat in categories if current_value.lower() in cat.lower()]
    await ctx.respond(category_to_recommend)