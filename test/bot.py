import hikari
import lightbulb
import pandas as pd
import os
import asyncio
import time
from config import *
from unidecode import unidecode
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text
from utils import fmt, nickname_autocomplete, category_autocomplete
from database import clean_dataframe, save_table, save_file_to_db, load_players, load_previous_kvk, engine
from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def home():
    return "Bot is alive"

def run():
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    Thread(target=run).start()


bot = hikari.GatewayBot(DISCORD_TOKEN)
client = lightbulb.client_from_app(bot)
bot.subscribe(hikari.StartingEvent, client.start)



# CHANNEL_ID = 1410340027279478904

# @bot.listen(hikari.StartedEvent)
# async def on_started(event: hikari.StartedEvent) -> None:
#     instructions = (
#         "📌 User manual:\n"
#         "• `/stats` — player stats\n"
#         "• Example - /stats nickname PlayerNickname or id PlayerID\n"
#         "• `/rating` — players rating\n"
#         "• Example - /rating Merits / Units Killed / Units Dead"
#     )

#     await bot.rest.create_message(CHANNEL_ID, instructions)

@client.register()
class add_new_file(
    lightbulb.SlashCommand,
    name="add_new_file",
    description="Add your csv or exccel file",
    default_member_permissions=hikari.Permissions.ADMINISTRATOR
):

    file = lightbulb.attachment("file", "Upload CSV or Excel file")
    @lightbulb.invoke
    async def invoke(self, ctx: lightbulb.Context) -> None:
        guild_id = ctx.guild_id
        guild_file: hikari.Attachment = self.file
        if not guild_file.filename.endswith((".csv", ".xlsx", ".xls")):
            await ctx.respond("Need csv or excel file!", flags=hikari.MessageFlag.EPHEMERAL)
            return

        temp_path = f"tmp/{guild_file.filename}"
        os.makedirs("tmp", exist_ok=True)

        await guild_file.save(temp_path, force=True)
        if guild_file.filename.endswith(".csv"):
            df = pd.read_csv(temp_path, sep=";", encoding="utf-8")
        else:  # xlsx
            df = pd.read_excel(temp_path, engine="openpyxl")

        message = await ctx.respond("⏳ Processing...", flags=hikari.MessageFlag.EPHEMERAL)
        save_table(df, guild_id, True)
        await ctx.edit_response(message, "✅ Table replaced and old table archived successfully!")
        os.remove(temp_path)
        
@client.register()
class change_new_file(
    lightbulb.SlashCommand,
    name="change_new_file",
    description="Add your csv or excel file",
    default_member_permissions=hikari.Permissions.ADMINISTRATOR
):
    
    file = lightbulb.attachment("file", "Upload CSV or Excel file")
    @lightbulb.invoke
    async def invoke(self, ctx: lightbulb.Context) -> None:
        guild_id = ctx.guild_id
        guild_file: hikari.Attachment = self.file
        if not guild_file.filename.endswith((".csv", ".xlsx", ".xls")):
            await ctx.respond("Need csv or excel file!", flags=hikari.MessageFlag.EPHEMERAL)
            return
        temp_path = f"tmp/{guild_file.filename}"
        os.makedirs("tmp", exist_ok=True)
        await guild_file.save(temp_path, force=True)

        if guild_file.filename.endswith(".csv"):
            df = pd.read_csv(temp_path, sep=";", encoding="utf-8")
        else:  # xlsx
            df = pd.read_excel(temp_path, engine="openpyxl")

        message = await ctx.respond("⏳ Processing...", flags=hikari.MessageFlag.EPHEMERAL)
        save_table(df, guild_id, False)
        await ctx.edit_response(message, "✅ Table replaced successfully!")
        os.remove(temp_path)
        
@client.register()
class add_report(
    lightbulb.SlashCommand,
    name="add_report",
    description="Add your csv or excel file",
    default_member_permissions=hikari.Permissions.ADMINISTRATOR
):
    
    file = lightbulb.attachment("file", "Upload CSV or Excel file")
    @lightbulb.invoke
    async def invoke(self, ctx: lightbulb.Context) -> None:
        guild_id = ctx.guild_id
        guild_file: hikari.Attachment = self.file
        if not guild_file.filename.endswith((".csv", ".xlsx", ".xls")):
            await ctx.respond("Need csv or excel file!", flags=hikari.MessageFlag.EPHEMERAL)
            return
        
        temp_path = f"tmp/{guild_file.filename}"
        os.makedirs("tmp", exist_ok=True)
        await guild_file.save(temp_path, force=True)
        if guild_file.filename.endswith(".csv"):
            df = pd.read_csv(temp_path, sep=";", encoding="utf-8")
        else:  # xlsx
            df = pd.read_excel(temp_path, engine="openpyxl")
        message = await ctx.respond("⏳ Processing...", flags=hikari.MessageFlag.EPHEMERAL)
        success = save_file_to_db(df, guild_id)
        if success:
            await ctx.edit_response(message, "✅ File saved!")
        else:
            await ctx.edit_response(message, "❌ You have already added 3 reports!")
        os.remove(temp_path)



@client.register()
class stats(
    lightbulb.SlashCommand,
    name="stats",
    description="show player stats",
):

    name = lightbulb.string("nickname", "Player nickname", autocomplete=nickname_autocomplete, default="")

    player_id = lightbulb.integer("id", "Player ID", default="")

    @lightbulb.invoke
    async def invoke(self, ctx: lightbulb.Context) -> None:
        _, df = load_players(ctx.guild_id)

        nickname = self.name
        player_id = self.player_id

        player = None

        if nickname:
            player = df.loc[df["name"].str.lower() == nickname.lower()]
        elif player_id:
            player = df.loc[df["lord_id"] == player_id]

        if player is None or player.empty:
            await ctx.respond("Player doesn't exist", flags=hikari.MessageFlag.EPHEMERAL)
        else:
            row = player.iloc[0]
            reports = sorted(
                {col.split(".")[1] for col in row.index if col.startswith("units_killed.")},
                key=int
            )
            msg = (
                f"📊Stats for **{row['name']}**\n"
                f"Faction - **{row['faction']}**\n\n"
            )

            stages = ["Start of season", "Mid season", "End of season"]
            for i, r in enumerate(reports, start=1):                
                title = stages[i-1] if i <= len(stages) else f"Report {r}"
                msg += f"🔹**{title}:**\n"
                msg += f"⚡Power: {fmt(row.get(f'power.{r}', 0))}\n"
                msg += f"🏅Merits: {fmt(row.get(f'merits.{r}', 0))}\n"
                msg += f"⚔️Units killed: {fmt(row.get(f'units_killed.{r}', 0))}\n"
                msg += f"💀Units dead: {fmt(row.get(f'units_dead.{r}', 0))}\n\n"

            # підсумки, якщо є хоча б 2 звіти
            if len(reports) >= 2:
                start = reports[0]
                end = reports[-1]
                killed_start = int(row.get(f'units_killed.{start}', 0) or 0)
                killed_end = int(row.get(f'units_killed.{end}', 0) or 0)
                dead_start = int(row.get(f'units_dead.{start}', 0) or 0)
                dead_end = int(row.get(f'units_dead.{end}', 0) or 0)

                msg += f"📈**Season result:**\n"
                msg += f"⚔️Season units killed: {fmt(killed_end - killed_start)}\n"
                for tier in range(1, 6):
                    start_val = int(row.get(f"t{tier}_kill_count.{start}", 0) or 0)
                    end_val = int(row.get(f"t{tier}_kill_count.{end}", 0) or 0)
                    diff = end_val - start_val
                    msg += f"T{tier} units killed: {fmt(diff)}\n"
                msg += f"💀Season units dead: {fmt(dead_end - dead_start)}\n"
                msg += f"🏅Season merits: {fmt(row.get(f'merits.{end}', 0))}\n"
            
            message = await ctx.respond(msg, flags=hikari.MessageFlag.EPHEMERAL)
            
@client.register()
class compare_kvk(
    lightbulb.SlashCommand,
    name="compare_kvk",
    description="Compare current and previous kvk"
):

    @lightbulb.invoke
    async def invoke(self, ctx: lightbulb.Context) -> None:
        _, df = load_players(ctx.guild_id)
        old_df = load_previous_kvk(ctx.guild_id)
        if old_df is None:
            await ctx.respond("⚠️ Previous kvk doesn't exist!", flags=hikari.MessageFlag.EPHEMERAL)
            return
        reports_new = sorted(
            {col.split(".")[1] for col in df.columns if col.startswith("power.")},
            key=int
        )
        reports_old = sorted(
            {col.split(".")[1] for col in old_df.columns if col.startswith("power.")},
            key=int
        )
        # for i, r in enumerate(reports_new, start=1):
        if len(reports_new) >= 2:
            start_new = reports_new[0]
            end_new = reports_new[-1]
            df_filtered = df[df[f"power.{end_new}"] >=15000000].copy()
            df_totals_power = df_filtered[f"power.{end_new}"].sum()
            df_totals_merits = df_filtered[f"merits.{end_new}"].sum()
            df_totals_units_killed = df_filtered[f"units_killed.{end_new}"].sum() - df_filtered[f"units_killed.{start_new}"].sum()
            df_totals_units_dead = df_filtered[f"units_dead.{end_new}"].sum() - df_filtered[f"units_dead.{start_new}"].sum()
        else:
            df_filtered = df[df[f"power.1"] >=15000000].copy()
            df_totals_power = df_filtered[f"power.1"].sum()
            df_totals_merits = df_filtered[f"merits.1"].sum()
            df_totals_units_killed = df_filtered[f"units_killed.1"].sum()
            df_totals_units_dead = df_filtered[f"units_dead.1"].sum()

        if len(reports_old) >= 2:
            start_old = reports_old[0]
            end_old = reports_old[-1]
            old_df_filtered = old_df[old_df[f"power.{end_old}"] >=15000000].copy()
            old_df_totals_power = old_df_filtered[f"power.{end_old}"].sum()
            old_df_totals_merits = old_df_filtered[f"merits.{end_old}"].sum()
            old_df_totals_units_killed = old_df_filtered[f"units_killed.{end_old}"].sum() - old_df_filtered[f"units_killed.{start_old}"].sum()
            old_df_totals_units_dead = old_df_filtered[f"units_dead.{end_old}"].sum() - old_df_filtered[f"units_dead.{start_old}"].sum()
        else:
            old_df_filtered = old_df[old_df[f"power.1"] >=15000000].copy()
            old_df_totals_power = old_df_filtered[f"power.1"].sum()
            old_df_totals_merits = old_df_filtered[f"merits.1"].sum()
            old_df_totals_units_killed = old_df_filtered[f"units_killed.1"].sum()
            old_df_totals_units_dead = old_df_filtered[f"units_dead.1"].sum()

        msg = (
            f"📊 **Compare current and previous kvk results**\n\n"
            "```\n"
            f"🟡 Previous Season        | 🔴 Current Season\n"
            f"⚡ Power: {fmt(int(old_df_totals_power))}        | ⚡ Power: {fmt(int(df_totals_power))}\n"
            f"🏅 Merits: {fmt(int(old_df_totals_merits))}      | 🏅 Merits: {fmt(int(df_totals_merits))}\n"
            f"⚔️ Units killed: {fmt(int(old_df_totals_units_killed))} | ⚔️ Units killed: {fmt(int(df_totals_units_killed))}\n"
            f"💀 Units dead: {fmt(int(old_df_totals_units_dead))}    | 💀 Units dead: {fmt(int(df_totals_units_dead))}\n"
            "```"
        )
        message = await ctx.respond(msg, flags=hikari.MessageFlag.EPHEMERAL)


@client.register()
class rating(
    lightbulb.SlashCommand,
    name="rating",
    description="Show top 20 players by Merits / Units Killed / Units Dead",
):
    category = lightbulb.string(
        "category",
        "Choose what to rank by",
        autocomplete=category_autocomplete
    )

    @lightbulb.invoke
    async def invoke(self, ctx: lightbulb.Context) -> None:
        _, df = load_players(ctx.guild_id)

        keywords = ["Merits", "Units", "Power"]

        category = self.category
        reports = sorted(
            {col.split(".")[1] for col in df.columns if col.startswith("power.")},
            key=int
        )
        first = reports[0]
        last = reports[-1]
        if category == "Merits":
            df_filtered = df[df["new_player"] != "migrant"].copy()
            if df_filtered.empty:
                await ctx.respond("⚠️ Немає даних для Units Killed", flags=hikari.MessageFlag.EPHEMERAL)
                return
            data = df_filtered[["name", f"merits.{last}"]].sort_values(by=[f"merits.{last}"], ascending=False).head(20)
            col = f"merits.{last}"
        elif category == "Units Killed":
            df_filtered = df[df["new_player"] != "migrant"].copy()
            if df_filtered.empty:
                await ctx.respond("⚠️ Немає даних для Units Killed", flags=hikari.MessageFlag.EPHEMERAL)
                return
            if len(reports) == 1:
                data = df_filtered[["name", f"units_killed.{last}"]].sort_values(by=[f"units_killed.{last}"], ascending=False).head(20)
                col = f"units_killed.{last}"
            else:
                df_filtered["Diff"] = df_filtered[f"units_killed.{last}"] - df_filtered[f"units_killed.{first}"]
                data = df_filtered[["name", "Diff"]].sort_values(by="Diff", ascending=False).head(20)
                col = "Diff"
        elif category == "Units Dead":
            df_filtered = df[df["new_player"] != "migrant"].copy()
            if df_filtered.empty:
                await ctx.respond("⚠️ Немає даних для Units Dead", flags=hikari.MessageFlag.EPHEMERAL)
                return
            if len(reports) == 1:
                data = df_filtered[["name", f"units_dead.{last}"]].sort_values(by=[f"units_dead.{last}"], ascending=False).head(20)
                col = f"units_dead.{last}"
            else:
                df_filtered["Diff"] = df_filtered[f"units_dead.{last}"] - df_filtered[f"units_dead.{first}"]
                data = df_filtered[["name", "Diff"]].sort_values(by="Diff", ascending=False).head(20)
                col = "Diff"
        else:
            await ctx.respond("⚠️ Unknown category", flags=hikari.MessageFlag.EPHEMERAL)
            return
        
        lines = []
        for i, row in enumerate(data.itertuples(index=False, name=None), start=1):
            name, value = row
            lines.append(f"{i}. **{name}** — {fmt(value)}")

        if len(reports) == 1:
            msg = f"🏆 **Top 20 by {category}** 🏆\n\n" + "\n".join(lines)
        else:
            msg = f"🏆 **Top 20 by season {category}** 🏆\n\n" + "\n".join(lines)

        await ctx.respond(msg, flags=hikari.MessageFlag.EPHEMERAL)

@client.register()
class remove_player(
    lightbulb.SlashCommand,
    name="remove_player",
    description="Removing player from the list",
    default_member_permissions=hikari.Permissions.ADMINISTRATOR
):
    
    name = lightbulb.string("name", "Player name", autocomplete=nickname_autocomplete, default="")
    player_id = lightbulb.integer("id", "Player ID", default=0)

    @lightbulb.invoke
    async def invoke(self, ctx: lightbulb.Context) -> None:
        _, df = load_players(ctx.guild_id)
        table_name = f"guild_{ctx.guild_id}"
        nickname = self.name
        player_id = self.player_id

        player = None
        with engine.begin() as conn:
            if nickname:
                result = conn.execute(text(f'SELECT * FROM "{table_name}" WHERE "name" = :name'), {"name": nickname}).fetchone()
                if result:
                    player = dict(result._mapping)
                    conn.execute(text(f'DELETE FROM "{table_name}" WHERE "name" = :name'), {"name": nickname})
            elif player_id:
                result = conn.execute(text(f'SELECT * FROM "{table_name}" WHERE "lord_id" = :id'), {"id": player_id}).fetchone()
                if result:
                    player = dict(result._mapping)
                    conn.execute(text(f'DELETE FROM "{table_name}" WHERE "lord_id" = :id'), {"id": player_id})
            else:
                await ctx.respond("Enter name or ID", flags=hikari.MessageFlag.EPHEMERAL)
                return
        if player:
            await ctx.respond(f"Player {player.get('name')} removed", flags=hikari.MessageFlag.EPHEMERAL)
        else:
            await ctx.respond("Player not fount", flags=hikari.MessageFlag.EPHEMERAL)

@client.register()
class merits_rating(
    lightbulb.SlashCommand,
    name="merits_list",
    description="Merits rating"
):
    min_percent = lightbulb.number("min_percent", "min %", default=0.0)
    max_percent = lightbulb.number("max_percent", "max %", default=100.0)

    @lightbulb.invoke
    async def invoke(self, ctx: lightbulb.Context) -> None:
        try:
            _, df = load_players(ctx.guild_id)
        except Exception:
            await ctx.respond("❌ The table doesn't exist!", flags=hikari.MessageFlag.EPHEMERAL)
            return
        table_name = f"guild_{ctx.guild_id}"
        min_percent = self.min_percent
        max_percent = self.max_percent
        reports = sorted(
            {col.split(".")[1] for col in df.columns if col.startswith("power.")},
            key=int
        )
        first = reports[0]
        last = reports[-1]

        # Перевіряємо, що колонка merits_% існує
        if "merits_%" not in df.columns:
            await ctx.respond("❌ Add a report first!", flags=hikari.MessageFlag.EPHEMERAL)
            return

        # Фільтрація по умовах
        filtered = df[
            (df["merits_%"] >= min_percent) &
            (df["merits_%"] <= max_percent) &
            (df[f"power.{last}"] > 15_000_000)
        ].copy()

        if filtered.empty:
            await ctx.respond("ℹ️ There are no eligible players.", flags=hikari.MessageFlag.EPHEMERAL)
            return

        # Сортування
        filtered = filtered.sort_values("merits_%", ascending=False)

        # Формуємо таблицю (Markdown)
        file_path = f"merits_list_{ctx.guild_id}_{int(time.time())}.xlsx"
        filtered.to_excel(
            file_path,
            columns=["name", "merits_%", f"merits.{last}", f"power.{last}"],
            index=False,
            engine="openpyxl"
        )

        await ctx.respond(
            f"📄 Merits list from {min_percent}% to {max_percent}% (Power > 15M):",
            attachment=hikari.File(file_path),
            flags=hikari.MessageFlag.EPHEMERAL
        )
        os.remove(file_path)

keep_alive()
bot.run()