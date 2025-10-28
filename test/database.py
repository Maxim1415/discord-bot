import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect, text
from config import DATABASE_URL

engine = create_engine(DATABASE_URL)
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.str.strip()  # прибрати пробіли з початку та кінця
                  .str.lower()  # усе в нижній регістр
                  .str.replace(" ", "_")  # пробіли → "_"
                  .str.replace(r"\.", "_", regex=True)  # крапки → "_"
    )
    return df
def clean_dataframe(df: pd.DataFrame, int_keywords=None, str_keywords=None) -> pd.DataFrame:
    if int_keywords is None:
        int_keywords = ["power", "merits", "units", "id", "server", "city", "defeats", "victories", "scouted", "gold", "wood", "ore", "mana", "gems", "resources", "helps", "killcount"]
    if str_keywords is None:
        str_keywords = ["Name", "%"]

    for col in df.columns:
        if df[col].dtype == "float64":
            df[col] = df[col].fillna(0).astype("int64")
        if any(keyword.lower() in col.lower() for keyword in int_keywords):
            df[col] = (
                df[col]
                .fillna(0)
                .astype(str)
                .str.replace(r"[^\d]", "", regex=True)
                .replace("", "0")
                .astype("int64")
            )
        elif any(keyword.lower() in col.lower() for keyword in str_keywords) or df[col].dtype == object:
            df[col] = df[col].fillna("").astype(str)
    return df

def mark_new_players(df: pd.DataFrame, old_table: pd.DataFrame | None, current_table: pd.DataFrame | None) -> pd.DataFrame:
    if "new_player" not in df.columns:
        df["new_player"] = None

    old_ids = set(old_table["lord_id"]) if old_table is not None and not old_table.empty else set()
    current_ids = set(current_table["lord_id"]) if current_table is not None and not current_table.empty else set()

    if current_table is None or current_table.empty:
        df["new_player"] = df["lord_id"].apply(
            lambda x: "old" if x in old_ids else "new"
        )
        return df

    current_status_map = dict(
        zip(current_table["lord_id"], current_table["new_player"])    
    )

    def detect_status(lord_id: int) -> str:
        if lord_id in current_status_map:
            return current_status_map[lord_id]
        if lord_id not in old_ids and lord_id not in current_ids:
            return "migrant"
        return "old"

    df["new_player"] = df["lord_id"].apply(detect_status)
    return df

def add_merits_percent(df: pd.DataFrame) -> pd.DataFrame:
    reports = sorted(
        {col.split(".")[1] for col in df.columns if col.startswith("power.")},
        key=int
    )
    print(reports)
    last = reports[-1]
    df["merits_%"] = df.apply(lambda row: round((row[f"merits.{last}"] / row[f"power.{last}"] * 100), 2) if row[f"power.{last}"] > 0 else 0, axis=1)

    return df

def save_table(df: pd.DataFrame, guild_id: int, archive: bool):
    df = normalize_columns(df)
    df = clean_dataframe(df)
    table_name = f"guild_{guild_id}"
    old_table_name = f"guild_{guild_id}_old"

    if df is None or df.empty:
        return False

    # Архівація при потребі (робимо копію ПЕРЕД видаленням)
    if archive:
        try:
            old_df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
            # зберігаємо повну копію в *_old
            old_df.to_sql(old_table_name, engine, if_exists="replace", index=False)
        except Exception:
            old_df = None

        try:
            old_table = pd.read_sql(f"SELECT * FROM {old_table_name}", engine)
        except Exception:
            old_table = None

        df = mark_new_players(df, old_table, old_df)
    # Завжди чистимо основну
    try:
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
    except Exception as e:
        print(f"⚠️ Помилка при очищенні таблиці {table_name}: {e}")

    # Заливаємо нові дані з нуля    
    return save_file_to_db(df, guild_id)
def save_file_to_db(df: pd.DataFrame, guild_id: int):
    df = normalize_columns(df)
    df = clean_dataframe(df)
    table_name = f"guild_{guild_id}"
    old_table_name = f"guild_{guild_id}_old"
    key_cols = ["lord_id", "name", "alliance_id", "alliance_tag", "faction", "division", "new_player"]

    df["lord_id"] = df["lord_id"].fillna(0).astype("int64")
    df = df[df["lord_id"] > 0]
    # Завантажуємо стару таблицю
    try:
        old_df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    except Exception:
        old_df = pd.DataFrame(columns=key_cols)
    if not old_df.empty and "lord_id" in old_df.columns:
        old_df = old_df[old_df["lord_id"].isin(df["lord_id"])].copy()
    # Позначаємо нових гравців

    try:
        old_table = pd.read_sql(f"SELECT * FROM {old_table_name}", engine)
    except Exception:
        old_table = None

    df = mark_new_players(df, old_table, old_df)

    # Підготовка нових колонок з індексами
    new_df = df.copy()
    for col in df.columns:
        if col in key_cols:
            continue
        base_col = col.split(".")[0]
        index = 1
        new_col = f"{base_col}.{index}"
        while new_col in old_df.columns:
            index += 1
            new_col = f"{base_col}.{index}"
        new_df = new_df.rename(columns={col: new_col})
    # Зливаємо таблиці по ключу lord_id, оновлюючи останні дані
    old_map = old_df.set_index("lord_id").to_dict(orient="index")
    for _, row in new_df.iterrows():
        lord_id = row["lord_id"]
        if pd.isna(lord_id):
            continue
        if lord_id in old_map:
            for col in new_df.columns:
                if col != "lord_id":
                    new_val = row[col]
                    if not pd.isna(new_val):
                        old_map[lord_id][col] = new_val
        else:
            # створюємо словник з усіма старими колонками
            row_dict = {col: 0 for col in old_df.columns if col != "lord_id"}
            # заповнюємо значеннями з нового звіту
            for col in new_df.columns:
                if col != "lord_id":
                    row_dict[col] = row[col]
            old_map[lord_id] = row_dict

    merged = pd.DataFrame.from_dict(old_map, orient="index").reset_index()
    merged = merged.rename(columns={"index": "lord_id"})

    # Додаємо відсоток заслуг
    merged = add_merits_percent(merged)

    # Зберігаємо у базу
    merged.to_sql(table_name, engine, if_exists="replace", index=False)

    # Оновлюємо глобальні імена
    update_global_names(guild_id, merged)

    return True

def load_players(guild_id):
    table_name = f"guild_{guild_id}"
    query = f"SELECT * FROM {table_name}"
    try:
        df = pd.read_sql(query, engine)
        players_list = df["name"].to_list()
        return players_list, df
    except Exception:
        return [], pd.DataFrame()
    
def load_previous_kvk(guild_id):
    table_name = f"guild_{guild_id}_old"
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        return None
    
    query = f"SELECT * FROM {table_name}"
    old_df = pd.read_sql(query, engine)
    return old_df

def update_global_names(guild_id, df: pd.DataFrame):
    table_name = f"names_{guild_id}"
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                lord_id BIGINT PRIMARY KEY,
                name TEXT
            )
        """))
    try:
        global_df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
    except Exception:
        global_df = pd.DataFrame(columns=["lord_id", "name"])

    global_map = dict(zip(global_df["lord_id"], global_df["name"]))

    updates = []
    inserts = []

    for _, row in df.iterrows():
        lord_id, name = row["lord_id"], row["name"]
        if lord_id in global_map:
            if global_map[lord_id] != name:
                updates.append({"name": name, "lord_id": lord_id})
        else:
            inserts.append({"lord_id": lord_id, "name": name})

    if updates:
        with engine.begin() as conn:
            conn.execute(
                text(f"UPDATE {table_name} SET name = :name WHERE lord_id = :lord_id"),
                updates
            )

    if inserts:
        with engine.begin() as conn:
            conn.execute(
                text(f"INSERT INTO {table_name} (lord_id, name) VALUES (:lord_id, :name)"
                     f"ON CONFLICT(lord_id) DO NOTHING"),
                inserts    
            )
    sync_names_with_global(guild_id)

def sync_names_with_global(guild_id: int):
    table_name = f"guild_{guild_id}"
    global_name_table = f"names_{guild_id}"

    try:
        df = pd.read_sql(f"SELECT * FROM {table_name}", engine)
        global_df = pd.read_sql(f"SELECT * FROM {global_name_table}", engine)
    except Exception:
        return False

    global_map = dict(zip(global_df["lord_id"], global_df["name"]))

    df["name"] = df["lord_id"].map(global_map).fillna(df["name"])

    df.to_sql(table_name, engine, if_exists="replace", index=False)
    return True