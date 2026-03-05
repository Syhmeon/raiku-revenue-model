# Plan Complet : RAIKU Revenue Model — Refonte Intégrale

## Vue d'ensemble

Le projet se décompose en **5 chantiers** qui s'enchaînent logiquement :

1. **DB Epoch** — Base de données propre des métriques Solana par epoch
2. **DB Programme** — Base de données par programme Solana (fees, CU, volumes)
3. **Mapping Programme** — Classification des programmes en archétypes RAIKU
4. **Modèles de Revenus** — JIT + AOT (top-down & bottom-up) en formules Sheet
5. **Export Google Sheet** — Poussée des données + écriture des formules + visualisation

Principe directeur : **Python = extraction brute uniquement**, **Google Sheet = tous les calculs via formules**.

---

## Chantier 1 : DB Epoch (données brutes par epoch)

### Ce qui existe
- `build_database.py` → déjà réécrit avec 23 colonnes RAW (+ 3 cross-check)
- 5 sources actives : Trillium (552+), Dune epochs/commissions/stake (150+), CoinGecko, Jito Foundation, Solana Compass
- CSV produit : `data/processed/solana_epoch_database.csv` (786 lignes)

### Colonnes RAW (A-W) — remplies par Python

| Col | Nom | Source | Description |
|-----|-----|--------|-------------|
| A | epoch | Toutes | Numéro d'epoch (clé primaire) |
| B | date | Trillium/Dune | Date de début |
| C | duration_days | Trillium/Dune | Durée en jours |
| D | inflation_rewards_sol | Trillium/Dune | Récompenses d'inflation (SOL) |
| E | total_fees_sol | Trillium/Dune | Total fees base+priority (SOL) |
| F | priority_fees_sol | Trillium (552+) | Priority fees seulement (SOL) |
| G | base_fees_sol | Trillium (552+) | Base/signature fees (SOL) |
| H | mev_jito_tips_sol | Trillium/Dune | Total Jito MEV tips (SOL) |
| I | mev_to_validators_sol | Trillium (552+) | Part MEV → validateurs |
| J | mev_to_stakers_sol | Trillium (552+) | Part MEV → stakers |
| K | mev_to_jito_sol | Trillium (552+) | Part MEV → Jito |
| L | validator_commissions_sol | Dune | Commissions sur inflation |
| M | avg_commission_rate | Dune/Trillium | Taux de commission moyen pondéré |
| N | validator_count | Trillium/Dune | Nombre de validateurs actifs |
| O | stake_accounts | Dune | Nombre de comptes de staking |
| P | active_stake_sol | Trillium/Dune | Stake actif total (SOL) |
| Q | sol_price_usd | Trillium/CoinGecko | Prix du SOL (USD) |
| R | fdv_usd | CoinGecko | Fully Diluted Valuation |
| S | epochs_per_year | Trillium (552+) | Epochs/an (= 365.25/C) |
| T | avg_cu_per_block | Trillium (552+) | CU moyen par bloc |
| U | total_user_txns | Trillium (552+) | Transactions non-vote |
| V | total_vote_txns | Trillium (552+) | Transactions de vote |
| W | total_blocks | Trillium (552+) | Blocs dans l'epoch |

→ **23 colonnes**, pas de cross-check dans le Sheet principal (Jito Foundation et Solana Compass servent de validation offline dans Python).

### Colonnes FORMULA (X-AO) — formules Google Sheet

| Col | Nom | Formule Sheet | Description |
|-----|-----|---------------|-------------|
| X | total_rewards_sol | `=D2+E2+H2` | Inflation + Fees + MEV |
| Y | total_rewards_usd | `=X2*Q2` | Total rewards en USD |
| Z | staker_rewards_sol | `=D2-L2` | Inflation - commissions |
| AA | fee_pct_of_total | `=IF(X2>0, E2/X2, "")` | Part fees / total |
| AB | mev_pct_of_total | `=IF(X2>0, H2/X2, "")` | Part MEV / total |
| AC | effective_commission | `=IF(D2>0, L2/D2, M2)` | Commission effective |
| AD | inflation_apr | `=IF(AND(P2>0,C2>0), D2*(365.25/C2)/P2, "")` | APR inflation annualisé |
| AE | fee_apr | `=IF(AND(P2>0,C2>0), E2*(365.25/C2)/P2, "")` | APR fees annualisé |
| AF | mev_apr | `=IF(AND(P2>0,C2>0), H2*(365.25/C2)/P2, "")` | APR MEV annualisé |
| AG | total_apr | `=AD2+AE2+AF2` | APR total |
| AH | total_apy | `=(1+AG2/365.25)^365.25-1` | APY composé |
| AI | total_supply_sol | `=IF(AND(R2<>"",Q2>0), R2/Q2, "")` | Supply = FDV/prix |
| AJ | staked_ratio | `=IF(AND(P2>0,AI2>0), P2/AI2, "")` | % stake |
| AK | burn_sol | `=IF(A2<620, E2*0.5, 0)` | Burn 50% fees pre-SIMD-96 |
| AL | net_inflation_sol | `=D2-AK2` | Inflation nette |
| AM | annual_total_fees_usd | `=IF(C2>0, E2*(365.25/C2)*Q2, "")` | Fees annualisées USD |
| AN | annual_mev_usd | `=IF(C2>0, H2*(365.25/C2)*Q2, "")` | MEV annualisé USD |
| AO | annual_priority_fees_usd | `=IF(AND(C2>0,F2<>""), F2*(365.25/C2)*Q2, "")` | Priority fees annualisées USD |

→ **18 colonnes FORMULA**, toutes vérifiables dans le Sheet.

### Action
- `build_database.py` est déjà prêt (23 colonnes RAW)
- Il suffit de re-run pour régénérer le CSV propre
- Les formules seront écrites par `sheets_export.py`

---

## Chantier 2 : DB Programme (données par programme Solana)

### Objectif
Disposer de données **par programme** pour :
1. Identifier quels programmes paient le plus de priority fees (= clients potentiels AOT)
2. Calculer le fee/CU réel par type de programme (paramètre clé du modèle bottom-up)
3. Estimer la taille du marché adressable (programmes latency-sensitive)

### Ce qui existe déjà

| Source | Fichier | Contenu | Couverture |
|--------|---------|---------|------------|
| Dune query 6777333 | `dune_fee_per_cu_by_program.csv` | 50 programmes, fee/CU (avg, median, p25, p75), CU consommé, total fees | 7 jours |
| Dune query 6777334 | `dune_daily_priority_fees.csv` | Priority fees daily agrégées (pas par programme) | 91 jours |
| SolWatch | `lead_pipeline_sheet.xlsx` | 1897 programmes, tx count, fail rate, priority fees, jito tips, CU | 13 jours (18 fév → 3 mars 2026) |

### Ce qui manque
1. **Historique par programme** — Les 7 jours de Dune et 13 jours de SolWatch ne suffisent pas pour un modèle fiable
2. **Classification complète** — 1812 programmes sur 1878 sont "unknown" dans SolWatch
3. **CU consommé vs CU alloué** — Ratio crucial pour évaluer l'efficacité et le potentiel AOT

### Plan d'extraction — Sources par ordre de priorité

#### Source A : Dune Analytics (PRIMAIRE)

**Nouvelle query Dune** : Per-program priority fees, CU, et tx count sur 30 jours minimum.

```sql
-- Query: Per-program fee/CU economics (30 days)
SELECT
    COALESCE(t.executing_account, 'unknown') AS program_id,
    DATE_TRUNC('day', t.block_time) AS day,
    COUNT(*) AS tx_count,
    COUNT(CASE WHEN t.success THEN 1 END) AS success_count,
    SUM(t.fee) / 1e9 AS total_fees_sol,
    SUM(COALESCE(t.priority_fee, 0)) / 1e9 AS priority_fees_sol,
    AVG(t.compute_units_consumed) AS avg_cu_consumed,
    SUM(t.compute_units_consumed) AS total_cu,
    -- fee per CU (en lamports)
    CASE
        WHEN SUM(t.compute_units_consumed) > 0
        THEN SUM(COALESCE(t.priority_fee, 0)) * 1.0 / SUM(t.compute_units_consumed)
        ELSE 0
    END AS fee_per_cu_lamports
FROM solana.transactions t
WHERE t.block_time >= NOW() - INTERVAL '30' DAY
    AND t.success = true
GROUP BY 1, 2
HAVING SUM(t.fee) > 0
ORDER BY priority_fees_sol DESC
```

**Avantages** :
- Gratuit (Dune free tier = 2500 credits/mois, cette query ≈ 50-100 credits)
- On a déjà le `DuneClient` qui fonctionne
- Extensible à 90 jours si besoin

**Deuxième query** : Agrégé par programme sur 30 jours (pour le mapping)

```sql
-- Query: Top programs by priority fees (30-day aggregate)
SELECT
    t.executing_account AS program_id,
    COUNT(*) AS tx_count,
    COUNT(CASE WHEN t.success THEN 1 END) AS success_count,
    SUM(t.fee) / 1e9 AS total_fees_sol,
    SUM(COALESCE(t.priority_fee, 0)) / 1e9 AS priority_fees_sol,
    SUM(t.compute_units_consumed) AS total_cu,
    AVG(t.compute_units_consumed) AS avg_cu_consumed,
    APPROX_PERCENTILE(
        CASE WHEN t.compute_units_consumed > 0
        THEN COALESCE(t.priority_fee, 0) * 1.0 / t.compute_units_consumed
        END, 0.5
    ) AS median_fee_per_cu,
    APPROX_PERCENTILE(
        CASE WHEN t.compute_units_consumed > 0
        THEN COALESCE(t.priority_fee, 0) * 1.0 / t.compute_units_consumed
        END, 0.25
    ) AS p25_fee_per_cu,
    APPROX_PERCENTILE(
        CASE WHEN t.compute_units_consumed > 0
        THEN COALESCE(t.priority_fee, 0) * 1.0 / t.compute_units_consumed
        END, 0.75
    ) AS p75_fee_per_cu
FROM solana.transactions t
WHERE t.block_time >= NOW() - INTERVAL '30' DAY
    AND t.success = true
GROUP BY 1
HAVING SUM(COALESCE(t.priority_fee, 0)) > 0
ORDER BY priority_fees_sol DESC
LIMIT 500
```

**Output** : `data/raw/dune_program_fees_30d.csv`

#### Source B : Solscan Pro API (COMPLÉMENT — test uniquement)

Clé API disponible (free tier). Endpoint : `GET /v2.0/program/analytics`

**Ce que Solscan donne** : tx count, success count, interaction volume, active users, instruction breakdown par jour.
**Ce que Solscan NE donne PAS** : priority fees, CU consumed, fee per CU.

→ **Verdict** : Utile uniquement pour valider les tx counts et obtenir les noms de programmes. Pas de données fee/CU. On l'utilisera comme complément pour le mapping (obtenir le nom d'un programme à partir de son address).

#### Source C : SolWatch / lead_pipeline (RÉFÉRENCE)

Déjà extrait dans `lead_pipeline_sheet.xlsx`. Colonnes utiles :
- Programme ID, Name, Category (même si 96% = "unknown")
- Priority Fees (SOL), Jito Tips (SOL), TX Count, Fail Rate
- Couverture : 13 jours

→ **Usage** : Cross-check avec Dune, source de noms de programmes connus.

### Fichier de sortie

`data/processed/program_database.csv` — colonnes :

| Col | Nom | Description |
|-----|-----|-------------|
| A | program_id | Adresse du programme (clé primaire) |
| B | program_name | Nom lisible (si connu) |
| C | category | Catégorie RAIKU (voir Chantier 3) |
| D | period | Période des données (ex: "30d") |
| E | tx_count | Nombre de transactions |
| F | success_count | Transactions réussies |
| G | fail_rate | Taux d'échec |
| H | total_fees_sol | Total fees (SOL) |
| I | priority_fees_sol | Priority fees (SOL) |
| J | total_cu | CU total consommé |
| K | avg_cu_per_tx | CU moyen par transaction |
| L | median_fee_per_cu | Median fee/CU (lamports) |
| M | p25_fee_per_cu | P25 fee/CU |
| N | p75_fee_per_cu | P75 fee/CU |
| O | pct_of_total_priority_fees | Part des priority fees totales |

### Nouveau fichier à créer

`01_extract/extract_dune_programs.py` — Extracteur Dune pour les données programme

---

## Chantier 3 : Mapping Programme → Archétypes RAIKU

### Les 6 archétypes (de `raiku_usecases.txt`)

| # | Archétype | Description | Fee/CU typique | Exemples |
|---|-----------|-------------|----------------|----------|
| 1 | PropAMM | Oracle/quote updates, AMM automatisé | 0.016-0.027 L/CU | BisonFi, HumidiFi, Tessera |
| 2 | Quant Trading | Exécution algo, position sizing | 0.10-0.20 L/CU | Desks propriétaires |
| 3 | Market Maker (Ops) | Margin, collateral, rollover | 0.05-0.15 L/CU | Wintermute, GSR, Alameda successors |
| 4 | DEX-DEX Arb | Arbitrage entre DEX (async) | 0.05-0.10 L/CU | Bots arb MEV |
| 5 | Protocol Cranker | Crank, DCA, rebalance, keeper | 0.02-0.06 L/CU | Drift, Jupiter DCA, Kamino, Marinade |
| 6 | CEX-DEX Arb | Arb centralisé-décentralisé | 0.30-0.80 L/CU | HFT firms, Jump, Wintermute |

### Approche de classification

**Fichier de mapping** : `data/mapping/program_categories.csv`

```
program_id;program_name;raiku_category;subcategory;source;notes
675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8;Raydium V4;dex;amm;manual;Top DEX
JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4;Jupiter V6;dex;aggregator;manual;Top aggregator
whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc;Orca Whirlpool;dex;clmm;manual;Concentrated liquidity
...
```

**Catégories possibles** :

| Catégorie | Usage dans le modèle |
|-----------|---------------------|
| `propamm` | → Archétype 1 (PropAMMs) |
| `arb_bot` | → Archétype 4 (DEX-DEX) ou 6 (CEX-DEX) selon le pattern |
| `dex` | Non directement client AOT, mais génère les priority fees que les arbs paient |
| `perps` | → Archétype 2 (Quant) ou 3 (Market Maker) |
| `lending` | → Archétype 5 (Protocol Cranker) pour liquidations |
| `oracle` | → Archétype 1 (PropAMMs) ou 5 (Cranker) |
| `staking` | → Archétype 5 (Cranker) |
| `nft` | Non pertinent |
| `bridge` | Non pertinent |
| `mev_bot` | → Archétype 6 (CEX-DEX Arb) |
| `unknown` | À classifier manuellement pour le top 50 |

### Comment classifier

1. **Top 50 programmes par priority fees** : classification manuelle (1h de travail)
2. **Programmes 51-200** : classification semi-automatique basée sur le nom + fee/CU pattern
3. **Programmes 200+** : "other" — contribution marginale

La classification manuelle doit être faite par Sylvain (connaissance domaine), assistée par les données fee/CU qui donnent des indices forts :
- Fee/CU > 0.3 L/CU = probablement arb CEX-DEX ou HFT
- Fee/CU 0.01-0.03 = probablement PropAMM ou oracle
- Fee/CU 0.05-0.10 = DEX ou arb DEX-DEX
- Fail rate > 30% = probablement arb bot (compétitif)

### Action
- Créer `data/mapping/program_categories.csv` avec les programmes connus
- Pré-remplir à partir de SolWatch "Programs" tab (24 dex, 8 arb, etc.)
- Compléter avec le top 50 de Dune (à classifier manuellement)

---

## Chantier 4 : Modèles de Revenus

### 4A. JIT Revenue Model

**Formule** : `Gross_Revenue = Total_Jito_Tips × RAIKU_Market_Share`

**Données source** (dans DB Epoch) :
- `annual_mev_usd` (colonne AN du Sheet) = Jito tips annualisés en USD
- Références config : Jito 2025 full year ($720M), Jito Q4-2025 annualisé ($100M)

**Dans le Google Sheet** — tab "JIT Model" :

| Paramètre | Cellule | Source |
|-----------|---------|--------|
| Total market (latest epoch) | Formule : `='Epoch Database'!AN[last]` | DB Epoch |
| Total market (avg 10 epochs) | Formule : `=AVERAGE(...)` | DB Epoch |
| Total market (Jito 2025 bear) | Constante : 100,000,000 | config.py |
| Total market (Jito 2025 bull) | Constante : 720,000,000 | config.py |
| Market share | Paramètre : 2%, 5%, 10%, 15%, 20% | Input |
| Protocol fee | Paramètre : 3.5%, 5% | Input |
| Gross revenue | `= total_market × market_share` | Formule |
| Validator revenue | `= gross × (1 - protocol_fee)` | Formule |
| Protocol revenue | `= gross × protocol_fee` | Formule |

→ Toutes les formules dans le Sheet. Python ne calcule rien.

### 4B. AOT Revenue — Top-Down

**Formule** : `Gross_Revenue = Total_Priority_Fees × Latency_Sensitive_% × RAIKU_Capture_%`

**Données source** (dans DB Epoch) :
- `annual_priority_fees_usd` (colonne AO du Sheet) = priority fees annualisées
- `fee_pct_of_total` (colonne AA) pour le contexte

**Dans le Google Sheet** — tab "AOT Top-Down" :

| Paramètre | Source |
|-----------|--------|
| Total priority fees (annualized) | Formule référençant DB Epoch col AO |
| Latency-sensitive share (30-60%) | Input — à calibrer avec DB Programme |
| RAIKU capture rate (5-20%) | Input — hypothèse business |
| Protocol fee (5%) | Constante config |

La valeur de **latency_sensitive_share** vient de l'analyse de la DB Programme :
`= SUM(priority_fees des programmes classés arb/propamm/hft) / SUM(priority_fees totales)`

C'est un lien direct entre DB Programme et le modèle top-down.

### 4C. AOT Revenue — Bottom-Up (3D Framework)

**Formule par archétype** :
```
Revenue_archétype = N_customers × Slots_actifs/an × CU_par_slot × Fee_per_CU × SOL_price
où Slots_actifs = Total_slots × Stake_% × Pct_slots_active
```

**Paramètres par archétype** (calibrés par DB Programme) :

| Paramètre | Source | Comment le calibrer |
|-----------|--------|---------------------|
| fee_per_cu | DB Programme | Median fee/CU du top programme de cette catégorie |
| cu_per_tx | DB Programme | Avg CU consumed des programmes de cette catégorie |
| pct_slots_active | Estimation | Basé sur le tx frequency des programmes |
| txs_per_slot | Estimation | 1-2 selon l'archétype |
| num_customers | Estimation | Low/Mid/High tiers |

**Dans le Google Sheet** — tab "AOT Bottom-Up" :

Structure : Une table de paramètres par archétype + une matrice de scénarios (stake % × CU %).

Les paramètres `fee_per_cu` et `cu_per_tx` seront **liés à la DB Programme** via des formules VLOOKUP/INDEX-MATCH :
```
fee_per_cu_propamm = VLOOKUP("propamm", 'Program Database'!C:L, 10, FALSE)
```

Ça permet de mettre à jour automatiquement les paramètres du modèle quand on re-run le pipeline et que les données programme changent.

### 4D. Revenue Summary

Tab synthèse qui agrège JIT + AOT pour différents scénarios :

| Scénario | JIT | AOT TD | AOT BU | Total | Protocol (5%) |
|----------|-----|--------|--------|-------|---------------|
| Conservative | formule | formule | formule | formule | formule |
| Base | formule | formule | formule | formule | formule |
| Optimistic | formule | formule | formule | formule | formule |

100% formules Sheet, zéro valeur hardcodée.

---

## Chantier 5 : Export Google Sheet

### Structure des tabs

| # | Tab | Contenu | Source |
|---|-----|---------|--------|
| 1 | **Epoch Database** | 23 cols RAW (A-W) + 18 cols FORMULA (X-AO) | Python RAW + Sheet formules |
| 2 | **Program Database** | Top 200 programmes par priority fees | Python RAW |
| 3 | **Program Mapping** | Classification → archétypes RAIKU | Semi-manuel |
| 4 | **JIT Model** | Scénarios JIT avec formules | Sheet formules |
| 5 | **AOT Top-Down** | Scénarios AOT macro avec formules | Sheet formules |
| 6 | **AOT Bottom-Up** | Scénarios AOT 3D avec formules | Sheet formules |
| 7 | **Revenue Summary** | Vue consolidée | Sheet formules |
| 8 | **Data Sources** | Documentation de chaque colonne | Python + manuel |

### Tabs à supprimer
- "Epoch Economics" (ancien IMPORTDATA, remplacé par Epoch Database)
- "JIT Scenarios" (ancien, remplacé par JIT Model)
- "AOT Scenarios" (ancien, remplacé par AOT Top-Down + Bottom-Up)
- "Revenue Summary" (ancien, sera recréé avec formules)

### Formatage
- Header figé (row 1)
- Colonnes RAW : fond bleu clair
- Colonnes FORMULA : fond blanc
- Formats numériques : SOL avec 2 décimales, USD avec $, % avec %, entiers sans décimales
- Largeur de colonnes automatique

---

## Ordre d'implémentation

### Phase A : Données brutes (Python)
1. **Re-run `build_database.py`** (déjà prêt, 23 colonnes RAW) → `solana_epoch_database.csv`
2. **Créer les Dune queries programme** (2 nouvelles queries : daily 30d + aggregate)
3. **Créer `extract_dune_programs.py`** → `dune_program_fees_30d.csv`
4. **Créer `build_program_database.py`** → merge Dune + SolWatch → `program_database.csv`

### Phase B : Mapping (semi-manuel)
5. **Créer `program_categories.csv`** avec les programmes connus de SolWatch
6. **Compléter** avec le top 50 de Dune (Sylvain classe manuellement)
7. **Valider** : calculer latency_sensitive_share à partir du mapping

### Phase C : Export Google Sheet
8. **Réécrire `sheets_export.py`** :
   - Push DB Epoch (RAW A-W)
   - Écrire formules (X-AO) via gspread
   - Push DB Programme
   - Push Program Mapping
   - Créer tabs JIT Model, AOT Top-Down, AOT Bottom-Up avec formules
   - Créer Revenue Summary avec formules
   - Créer Data Sources
   - Supprimer anciennes tabs

### Phase D : Vérification
9. **Cross-check** : comparer les valeurs Sheet avec les anciennes valeurs Python
10. **Spot-check** : epochs 150, 552, 800, 934 — APY, fees, MEV cohérents ?
11. **Cohérence modèles** : ratio top-down / bottom-up dans le même ordre de grandeur ?

### Phase E (future — pas maintenant)
12. Graphiques (évolution rewards, breakdown, APY)
13. Dashboard interactif ou Looker Studio
14. Automatisation (re-run périodique)

---

## Fichiers à créer/modifier

| Fichier | Action | Description |
|---------|--------|-------------|
| `01_extract/extract_dune_programs.py` | 🆕 CRÉER | Extracteur per-programme (Dune) |
| `02_transform/build_database.py` | ✅ DÉJÀ FAIT | 23 colonnes RAW |
| `02_transform/build_program_database.py` | 🆕 CRÉER | Merge Dune + SolWatch → DB Programme |
| `04_output/sheets_export.py` | 🔄 RÉÉCRIRE | RAW + formules + tabs modèles |
| `data/mapping/program_categories.csv` | 🆕 CRÉER | Mapping programme → archétype |
| `config.py` | 🔧 MODIFIER | Ajouter nouveaux Dune query IDs |

## Fichiers NON modifiés
- `01_extract/extract_trillium.py` — fonctionne
- `01_extract/extract_jito_mev.py` — fonctionne
- `01_extract/extract_solana_compass.py` — fonctionne
- `01_extract/dune_client.py` — fonctionne
- `01_extract/dune_epochs.py` — fonctionne
- `01_extract/coingecko_prices.py` — fonctionne
- `03_model/aot_revenue.py` — gardé pour cross-check Python
- `03_model/jit_revenue.py` — gardé pour cross-check Python

---

## Notes sur les données AOT existantes

Les scénarios AOT qui existaient dans le Google Sheet viennent de `aot_revenue.py` :
- **Top-down** : Priority fees annualisées (latest epoch ~$200M, avg 10 epochs ~$210M) × latency_sensitive (30-60%) × capture (5-20%) × protocol fee (5%)
- **Bottom-up** : 6 archétypes × stake% (1-20%) × CU% (5-15%) × customers (low/mid/high)
- Les paramètres fee_per_cu par archétype étaient hardcodés : PropAMMs=0.025, Quant=0.15, MM=0.10, DEX-DEX=0.087, Crankers=0.054, CEX-DEX=0.50

→ Ces paramètres doivent être **remplacés par des valeurs réelles** de la DB Programme. Le modèle bottom-up sera recalibré quand on aura les données programme sur 30 jours.

## Note sur Solscan

Clé API Solscan disponible (free tier). L'endpoint `/v2.0/program/analytics` donne des tx counts et volumes mais **PAS de données fee/CU**. Utile uniquement pour :
- Obtenir le nom d'un programme à partir de son adresse
- Valider les tx counts vs Dune

→ On l'utilisera comme complément, pas comme source primaire.
