import pandas as pd
import random
import string
import os

# ── 1. Chargement du fichier Excel ──────────────────────────────────────────
file_path = r'C:\Users\Lenovo\Downloads\leads_nettoyes_Alex.xlsx'

if not os.path.exists(file_path):
    print(f"Erreur : fichier introuvable → {file_path}")
    exit()

try:
    df = pd.read_excel(file_path, engine='openpyxl')
    print(f"Fichier chargé avec succès. ({len(df)} lignes, {len(df.columns)} colonnes)")
except Exception as e:
    print(f"Erreur lors de la lecture du fichier : {e}")
    exit()

# ── 2. Détection de la colonne email ────────────────────────────────────────
email_candidates = ['email', 'Email', 'EMAIL', 'Mail', 'mail', 'MAIL', 'e-mail', 'E-mail']
target_col = next((col for col in email_candidates if col in df.columns), None)

if target_col is None:
    print(f"Erreur : aucune colonne email trouvée.\nColonnes disponibles : {df.columns.tolist()}")
    exit()

print(f"Colonne email détectée : '{target_col}'")

# ── 3. Fonction d'anonymisation ──────────────────────────────────────────────
def randomize_email(email):
    rand_str = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"test_{rand_str}@example.com"

df[target_col] = df[target_col].apply(randomize_email)
print(f"Anonymisation de '{target_col}' terminée.")

# ── 4. Sauvegarde en CSV ─────────────────────────────────────────────────────
output_path = r'C:\Users\Lenovo\Downloads\donnees_anonymisees.csv'

try:
    df.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
    print(f"Fichier sauvegardé : {output_path}")
except PermissionError:
    print(f"Erreur : impossible d'écrire dans {output_path}. Fermez le fichier s'il est ouvert.")
except Exception as e:
    print(f"Erreur lors de la sauvegarde : {e}")