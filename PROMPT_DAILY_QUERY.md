# Prompt Claude Code — Point 11 : Programme × Condition de marché

## Contexte

Le Point 11 de l'audit nécessite de comprendre comment chaque programme se comporte sous différentes conditions de marché (normal / elevated / extreme). Le SQL a été généré automatiquement depuis la base epochs et sauvegardé dans `data/sql_program_conditions.sql`.

La requête couvre **tout 2024+** (pas juste 30 jours) et classifie chaque jour via 68 CASE WHEN basés sur les `volatility_tag` des epochs. Les programmes avec >1M CU total sont inclus (~150-200 programmes).

## Étapes à suivre

### Étape 1 : Créer la requête sur Dune

1. Lire le fichier SQL :
   ```
   cat data/sql_program_conditions.sql
   ```

2. Créer la requête sur Dune via MCP :
   ```
   mcp__dune__createDuneQuery(
     name: "RAIKU: Per-program stats by market condition (2024+)",
     query_sql: <contenu du fichier SQL>,
     is_private: false
   )
   ```

3. Noter le `queryId` retourné.

4. Mettre à jour `config.py` : ajouter dans `DUNE_QUERIES` :
   ```python
   "program_conditions": <queryId>,
   ```

### Étape 2 : Exécuter la requête

```
mcp__dune__executeDuneQuery(queryId: <queryId>)
```

**Attention** : cette requête scanne TOUT 2024+ (≈800 jours × 5-10M tx/jour). Elle peut prendre **10-20 minutes**. Si timeout, ré-exécuter via MCP — le résultat sera en cache.

### Étape 3 : Récupérer les résultats

```
mcp__dune__getDuneQueryResults(queryId: <queryId>, limit: 50000)
```

Résultat attendu : ~150-500 lignes (programmes × 3 conditions).

Colonnes : `market_condition;program_id;tx_count;success_count;total_fees_sol;total_cu;avg_fee_per_cu_lamports;median_fee_per_cu_lamports;days_observed`

### Étape 4 : Sauvegarder en CSV

Sauvegarder dans `data/raw/dune_program_conditions.csv` au format semicolon-delimited (`;`), UTF-8.

### Étape 5 : Lancer le pipeline

```bash
cd C:\Users\Utilisateur\Dev-RAIKU\raiku-revenue-model

# 1. Build conditions analysis (pivot programme × condition → multipliers)
python 02_transform/build_program_conditions.py

# 2. Rebuild program database (enrichi avec fee_multiplier + congestion_sensitivity)
python 02_transform/build_program_database.py
```

### Étape 6 : Vérifier

Vérifier `data/processed/program_conditions.csv` :
- Programmes répartis en high / medium / low / unknown congestion_sensitivity
- fee_multiplier_elevated et fee_multiplier_extreme non-vides
- Les 3 conditions (normal, elevated, extreme) ont des données

Vérifier `data/processed/program_database.csv` :
- Colonnes fee_multiplier_elevated, fee_multiplier_extreme, congestion_sensitivity présentes et peuplées

## Structure des fichiers

```
Pipeline complet :
  01_extract/extract_program_conditions.py  → Génère le SQL + exécute sur Dune
  data/sql_program_conditions.sql           → SQL généré (68 CASE WHEN, 2024+)
  data/raw/dune_program_conditions.csv      → Résultat brut Dune (à créer)
  02_transform/build_program_conditions.py  → Pivot + multipliers + classification
  data/processed/program_conditions.csv     → Table finale programme × condition
  02_transform/build_program_database.py    → Enrichi avec les 3 colonnes de condition
  data/processed/program_database.csv       → DB enrichie (23 colonnes)
```

## Si la requête échoue ou timeout

1. Vérifier que le SQL utilise le pattern CTE avec `instructions[1].executing_account`
2. Si timeout, essayer de réduire : changer `HAVING SUM(compute_units_consumed) > 1000000` en `> 10000000` (10M CU)
3. Ou couper en 2 requêtes : 2024 et 2025-2026 séparément
