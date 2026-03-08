import pandas as pd

# =========================
# Fichiers
# =========================
input_file = "../data/data.xlsx"
output_file = "../data/donnees_fusionnees.xlsx"
output_non_fusion_file = "../data/lignes_non_fusionnees.xlsx"
output_fusion_lignes_file = "../data/lignes_fusionnees_detail.xlsx"

sheet_name = "Données SCANS et INTERSCANS"

# =========================
# Lecture
# =========================
df = pd.read_excel(input_file, sheet_name=sheet_name)

# =========================
# Supprimer les colonnes parasites de type Unnamed
# =========================
df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]

# =========================
# Vérification de la colonne activité
# =========================
if "type_activite" not in df.columns:
    print("Colonnes disponibles :")
    for col in df.columns:
        print(repr(col))
    raise ValueError("La colonne 'type_activite' est introuvable.")

# =========================
# Colonnes de groupement
# =========================
group_cols = [col for col in df.columns if col != "type_activite"]

# =========================
# Normalisation des colonnes de groupement
# =========================
df_group = df.copy()

for col in group_cols:
    df_group[col] = df_group[col].apply(
        lambda x: "" if pd.isna(x) else str(x).strip()
    )

# =========================
# Fonction de fusion des activités
# =========================
def fusion_activites(series):
    valeurs = [
        str(x).strip()
        for x in series
        if pd.notna(x) and str(x).strip() != ""
    ]
    return "_".join(sorted(set(valeurs)))

# =========================
# Taille des groupes
# =========================
group_sizes = (
    df_group.groupby(group_cols, dropna=False, sort=False)
    .size()
    .reset_index(name="nb_lignes_groupe")
)

# =========================
# Fusion
# =========================
df_fusion = (
    df_group.groupby(group_cols, dropna=False, as_index=False, sort=False)
            .agg({"type_activite": fusion_activites})
)

# =========================
# Lignes non fusionnées
# =========================
groupes_non_fusionnes = group_sizes[group_sizes["nb_lignes_groupe"] == 1][group_cols]

df_non_fusion = df_group.merge(
    groupes_non_fusionnes,
    on=group_cols,
    how="inner"
)

# =========================
# Lignes ayant participé à une fusion
# =========================
groupes_fusionnes = group_sizes[group_sizes["nb_lignes_groupe"] > 1][group_cols]

df_lignes_fusionnees = df_group.merge(
    groupes_fusionnes,
    on=group_cols,
    how="inner"
)

# =========================
# Sauvegarde
# =========================
df_fusion.to_excel(output_file, index=False)
df_non_fusion.to_excel(output_non_fusion_file, index=False)
df_lignes_fusionnees.to_excel(output_fusion_lignes_file, index=False)

print("Fusion terminée.")
print("Lignes initiales :", len(df))
print("Lignes après fusion :", len(df_fusion))
print("Lignes non fusionnées :", len(df_non_fusion))
print("Lignes impliquées dans une fusion :", len(df_lignes_fusionnees))

print("Fichier fusion :", output_file)
print("Fichier non fusion :", output_non_fusion_file)
print("Fichier lignes fusionnées :", output_fusion_lignes_file)