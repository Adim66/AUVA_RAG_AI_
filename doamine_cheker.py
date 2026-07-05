"""
check_domains.py
----------------
Lance ce script sur ton PC (pas dans le container).
Il teste chaque domaine des emails du CSV et génère :
  - leads_valid.csv   : leads avec domaine résolu
  - leads_invalid.csv : leads avec domaine non résolu

Usage:
    python check_domains.py

Mettre leads_nettoyes_Alex.csv dans le même dossier.
"""

import csv
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed

INPUT_CSV   = r"C:\Users\Lenovo\Downloads\leads_nettoyes_Alex.csv"
VALID_CSV   = r"C:\Users\Lenovo\Downloads\leads_valid.csv"
INVALID_CSV = r"C:\Users\Lenovo\Downloads\leads_invalid.csv"

def check_domain(domain):
    try:
        socket.setdefaulttimeout(5)
        socket.gethostbyname(domain)
        return domain, True
    except:
        return domain, False

# ── Lecture ───────────────────────────────────────────────────
rows = []
with open(INPUT_CSV, encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    for row in reader:
        rows.append(row)

# ── Extraire domaines uniques ─────────────────────────────────
domains = set()
for row in rows:
    email = row.get('email', '').strip()
    if '@' in email:
        domains.add(email.split('@')[1].lower().strip())

print(f"Total leads    : {len(rows)}")
print(f"Domaines uniques : {len(domains)}")
print("Test DNS en cours...\n")

# ── Test DNS en parallèle ─────────────────────────────────────
results = {}
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = {executor.submit(check_domain, d): d for d in domains}
    for future in as_completed(futures):
        domain, ok = future.result()
        results[domain] = ok
        status = "✅" if ok else "❌"
        print(f"  {status} {domain}")

# ── Séparer valid / invalid ───────────────────────────────────
valid_rows   = []
invalid_rows = []

for row in rows:
    email = row.get('email', '').strip()
    if '@' in email:
        domain = email.split('@')[1].lower().strip()
        if results.get(domain, False):
            valid_rows.append(row)
        else:
            invalid_rows.append(row)
    else:
        invalid_rows.append(row)

# ── Export ────────────────────────────────────────────────────
with open(VALID_CSV, 'w', newline='', encoding='utf-8-sig') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(valid_rows)

with open(INVALID_CSV, 'w', newline='', encoding='utf-8-sig') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(invalid_rows)

ok_domains = [d for d, v in results.items() if v]
ko_domains = [d for d, v in results.items() if not v]

print(f"\n{'='*50}")
print(f"✅ Domaines résolus   : {len(ok_domains)}")
print(f"❌ Domaines non résolus : {len(ko_domains)}")
print(f"\n✅ Leads valides  → {VALID_CSV}   ({len(valid_rows)} leads)")
print(f"❌ Leads invalides → {INVALID_CSV} ({len(invalid_rows)} leads)")

print(f"\n--- Domaines résolus ---")
for d in sorted(ok_domains):
    print(f"  {d}")

print(f"\n--- Domaines non résolus ---")
for d in sorted(ko_domains):
    print(f"  {d}")