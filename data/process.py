import pandas as pd
import json

# 1. Load Data
df = pd.read_csv('aiddata-countries-only.csv')

# --- NEW FILTERING STEP ---
# Remove rows where the purpose is "sectors not specified" (case insensitive)
# This removes it from the Top 5 AND the per-country calculations.
df = df[df['coalesced_purpose_name'].str.lower() != "sectors not specified"]
# --------------------------

# 2. Identify Top 5 Global Purposes (Frequency)
# Now this will automatically exclude the sector you filtered out above
top_5_global = df['coalesced_purpose_name'].value_counts().head(5).index.tolist()

# 3. Aggregate Data per Country
output_data = []
countries = df['recipient'].unique()

for country in countries:
    country_df = df[df['recipient'] == country]
    
    # Calculate Sums
    grouped = country_df.groupby('coalesced_purpose_name')['commitment_amount_usd_constant'].sum()
    
    # A: Global Top 5 Amounts
    global_breakdown = {p: grouped.get(p, 0) for p in top_5_global}
    
    # B: Dominant Purpose
    if not grouped.empty:
        dominant_purpose = grouped.idxmax()
        dominant_amount = grouped.max()
    else:
        dominant_purpose = "None"
        dominant_amount = 0
        
    # C: Local Top 5 Purposes
    local_top = grouped.sort_values(ascending=False).head(5)
    local_top_list = [{"purpose": k, "amount": v} for k, v in local_top.items()]
    
    output_data.append({
        "country": country,
        "global_breakdown": global_breakdown,
        "dominant_purpose": dominant_purpose,
        "dominant_amount": dominant_amount,
        "local_top_5": local_top_list
    })

# Save to JSON
with open('processed_aid_data.json', 'w') as f:
    json.dump(output_data, f)

print("Data processed! 'Sectors not specified' removed.")
print("Top 5 Global Purposes:", top_5_global)