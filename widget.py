# widget.py — Grist Custom Widget
import grist_plugin_api as grist
import pandas as pd
import matplotlib.pyplot as plt
import io, base64, json

# ── Colonnes à conserver (toutes les autres sont supprimées)
COLS_TO_KEEP = [
    "ZoneStatReponse4",
    "Date de Debut/Depart",
    "Destination Depart",
    "Code Pays Depart",
    "Destination Arrivee",
    "Code Pays Arrivee",
    "Activite Type",       # sera renommée en "Mode de déplacement"
    "Prix de Vente",
]

# ── Valeurs autorisées pour "Activite Type"
ACTIVITE_VALIDES = ["RAIL", "AIR"]


def get_dataframe():
    """Récupère les données Grist et les convertit en DataFrame."""
    records = grist.fetch_table("Table1")
    return pd.DataFrame(records)


def keep_columns(df):
    """Supprime toutes les colonnes absentes de COLS_TO_KEEP."""
    existing = [c for c in COLS_TO_KEEP if c in df.columns]
    missing  = [c for c in COLS_TO_KEEP if c not in df.columns]
    if missing:
        print(f"[AVERTISSEMENT] Colonnes absentes de la source : {missing}")
    return df[existing]


def filter_activite_type(df):
    """Garde uniquement les lignes RAIL et AIR, supprime les autres."""
    before = len(df)
    df = df[df["Activite Type"].isin(ACTIVITE_VALIDES)].copy()
    return df, before - len(df)


def remove_cancelling_rows(df, value_col, group_cols):
    """Supprime les paires de lignes qui s'annulent (+X / -X)."""
    df = df.copy()
    df["__key__"] = df[group_cols].astype(str).agg("-".join, axis=1)
    to_drop = []
    for _, grp in df.groupby("__key__"):
        pos = grp[grp[value_col] > 0].copy()
        neg = grp[grp[value_col] < 0].copy()
        for _, p in pos.iterrows():
            match = neg[neg[value_col] == -p[value_col]]
            if not match.empty:
                to_drop += [p.name, match.index[0]]
                neg = neg.drop(match.index[0])
    df = df.drop(index=to_drop).drop(columns=["__key__"])
    return df, len(to_drop)


def finalize_columns(df):
    """Renomme Activite Type et ajoute les 2 colonnes finales."""
    df = df.rename(columns={"Activite Type": "Mode de déplacement"})
    df["Nb de personnes dans la voiture"] = ""
    df["Aller / Retour"] = "Oui"
    return df


def export_csv(df, path="export.csv"):
    """Exporte le DataFrame nettoyé en CSV (encodage Excel-compatible)."""
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def generate_histogram(df, value_col, x_col):
    """Génère un histogramme en colonnes, retourné en base64."""
    fig, ax = plt.subplots(figsize=(10, 5))
    grouped = df.groupby(x_col)[value_col].sum().sort_values(ascending=False)
    ax.bar(grouped.index, grouped.values, color="#185FA5", edgecolor="#0C447C")
    ax.set_xlabel(x_col)
    ax.set_ylabel(value_col)
    ax.set_title(f"Histogramme : {value_col} par {x_col}")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


# ── Point d'entrée appelé par le widget HTML
def on_message(msg):
    action     = msg.get("action")
    value_col  = msg.get("value_col")
    group_cols = msg.get("group_cols", [])
    x_col      = msg.get("x_col", group_cols[0] if group_cols else "Mode de déplacement")

    if action == "clean":
        df                 = get_dataframe()
        df                 = keep_columns(df)
        df, filtered_rows  = filter_activite_type(df)
        df, cancelled_rows = remove_cancelling_rows(df, value_col, group_cols)
        df                 = finalize_columns(df)
        csv_path           = export_csv(df)
        img_b64            = generate_histogram(df, value_col, x_col)

        return {
            "status"         : "ok",
            "filtered_rows"  : filtered_rows,
            "cancelled_rows" : cancelled_rows,
            "csv_path"       : csv_path,
            "chart_b64"      : img_b64,
        }

grist.on_message(on_message)
grist.ready()
