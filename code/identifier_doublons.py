import pandas as pd

# =========================
# Fichiers
# =========================
input_file = "../data/data.xlsx"
output_file = "../data/lignes_ayant_doublons.xlsx"

sheet_name = "Données SCANS et INTERSCANS"

# =========================
# Lecture du fichier Excel
# =========================
df = pd.read_excel(input_file, sheet_name=sheet_name)

# =========================
# Suppression colonnes parasites
# =========================
df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]

# =========================
# Normalisation (évite problèmes datetime/time)
# =========================
df_norm = df.copy()

for col in df_norm.columns:
    df_norm[col] = df_norm[col].apply(
        lambda x: "" if pd.isna(x) else str(x).strip()
    )

# =========================
# Calcul du nombre d'occurrences
# =========================
group_cols = list(df_norm.columns)

group_sizes = (
    df_norm.groupby(group_cols, dropna=False)
    .size()
    .reset_index(name="nb_occurrences")
)

# =========================
# Ajout du nombre d'occurrences à chaque ligne
# =========================
df_with_counts = df_norm.merge(
    group_sizes,
    on=group_cols,
    how="left"
)

# =========================
# Garder seulement les lignes ayant un doublon
# =========================
df_doublons = df_with_counts[df_with_counts["nb_occurrences"] > 1]

# =========================
# Sauvegarde
# =========================
df_doublons.to_excel(output_file, index=False)

# =========================
# Informations
# =========================
print("Analyse terminée")
print("Nombre total de lignes :", len(df))
print("Nombre de lignes ayant au moins un doublon :", len(df_doublons))
print("Fichier créé :", output_file)