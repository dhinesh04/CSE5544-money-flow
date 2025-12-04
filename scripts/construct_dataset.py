import pandas as pd
from pathlib import Path

# ========= 1. Explicit non-country donors (banks / funds / orgs) =========
NON_COUNTRY_DONORS = {
    "African Development Bank (AFDB)",
    "European Bank for Reconstruction & Development (EBRD)",
    "African Development Fund (AFDF)",
    "Asian Development Bank (ASDB)",
    "Caribbean Development Bank (CDB)",
    "Asian Development Fund (ASDF)",
    "Inter-American Development Bank (IADB)",
    "North American Development Bank (NADB)",
    "Nordic Development Fund (NDF)",
    "OPEC Fund for International Development (OFID)",
    "Nigerian Trust Fund (NTF)",
    "Arab Bank for Economic Development in Africa (BADEA)",
    "Arab Fund for Economic & Social Development (AFESD)",
    "World Bank - International Bank for Reconstruction and Development (IBRD)",
    "World Bank - International Development Association (IDA)",
    "United Nations Democracy Fund (UNDEF)",
    "International Fund for Agricultural Development (IFAD)",
    "International Monetary Fund (IMF)",
    "Global Fund to Fight Aids, Tuberculosis and Malaria (GFATM)",
    "Andean Development Corporation (CAF)",
    "African Capacity Building Foundation (ACBF)",
    "World Bank - International Finance Corporation (IFC)",
    "Islamic Development Bank (ISDB)",
    "World Bank - Carbon Finance Unit",
    "World Bank - Managed Trust Funds",
    "Global Environment Facility (GEF)",
    "Multilateral Fund for the Implementation of the Montreal Protocol",
    "World Bank - Debt Reduction Facility",
    "Congo Basin Forest Fund (CBFF)",
    "United Nations Children`s Fund (UNICEF)",
    "Global Alliance for Vaccines & Immunization (GAVI)",
    "European Communities (EC)",
    "United Nations Development Programme (UNDP)",
    "United Nations Economic Commission for Europe (UNECE)",
    "United Nations Population Fund (UNFPA)",
    "World Health Organization (WHO)",
    "Bill & Melinda Gates Foundation",
    "Joint United Nations Programme on HIV/AIDS (UNAIDS)",
    "World Trade Organization (WTO)",
    "Global Partnership for Education",
    "World Trade Organization (WTO) - International Trade Centre",
    "United Nations Economic and Social Commission for Asia and the Pacific (UNESCAP)",
    "United Nations Economic and Social Commission for Western Asia (UNESCWA)",
    "United Nations High Commissioner for Refugees (UNHCR)",
    "Organization for Security and Co-operation in Europe (OSCE)",
    "United Nations Peacebuilding Fund (UNPBF)",
    "Asian Development Bank (AsDB Special Funds)",
    "United Nations Relief and Works Agency for Palestine Refugees in the Near East (UNRWA)",
    "Global Green Growth Institute (GGGI)"
}

# ========= 2. Region / aggregate recipients to drop =========
# Anything here, or containing region keywords, will be removed.
NON_COUNTRY_RECIPIENTS_EXACT = {
    # big regional aggregates / unspecified
    "Africa, Regional Programs, Regional Programs",
    "America, Regional Programs, Regional Programs",
    "Europe, Regional Programs",
    "South & Central Asia, Regional Programs",
    "North & Central America, regional, Regional Programs",
    "Asia, Regional Programs, Regional Programs",
    "South America, Regional Programs",
    "Far East Asia, Regional Programs",
    "Middle East, Regional Programs",
    "Oceania, Regional Programs",
    "Arab Countries, Regional Programs, Regional Programs",
    "Africa, South of Sahara, Regional Programs",
    "Africa, North of Sahara, Regional Programs",
    "Mediterranean, Regional Programs, Regional Programs",
    "Africa, South of Sahara, Regional Programs Multi-Country",
    "Africa, South of Sahara Multi-Country",
    "Africa, Regional Programs Multi-Country",
    "Latin America, Regional Programs, Regional Programs",
    "Asia, Regional Programs Multi-Country",
    "North & Central America, Regional Programs",
    "South Asia, Regional Programs, Regional Programs",
    "Central Asia, Regional Programs, Regional Programs",
    "West Indies, Regional Programs, Regional Programs",
    "Ex-Yugoslavian States, Unspecified",
    "MADCT Unspecified",
    "Bilateral, unspecified",
    "Bilateral, unspecified Multi-Country",
    "Global",
    "no value",
    "European Commission",
}

# generic substrings that indicate a region / multi-country aggregate
REGION_KEYWORDS = [
    "Regional Programs",
    "regional, Regional Programs",  # appears literally in your list
    "Multi-Country",
    "Unspecified",
]

def is_region_or_aggregate(name: str) -> bool:
    """Return True if recipient looks like a region / unspecified aggregate."""
    if pd.isna(name):
        return True
    s = str(name)
    if s in NON_COUNTRY_RECIPIENTS_EXACT:
        return True
    for kw in REGION_KEYWORDS:
        if kw in s:
            return True
    return False

# ========= 3. Streaming filter over the big AidData CSV =========

input_csv  = Path("../data/AidDataCoreThin_ResearchRelease_Level1_v3.1.csv")  # <-- change to your actual filename
output_csv = Path("../data/updated_aiddata.csv")

usecols = [
    "aiddata_id",
    "aiddata_2_id",
    "year",
    "donor",
    "recipient",
    "commitment_amount_usd_constant",
    "coalesced_purpose_code",
    "coalesced_purpose_name",
]

def filter_chunk(df: pd.DataFrame) -> pd.DataFrame:
    # Drop weird year 9999
    df = df[df["year"] != 9999].copy()

    # Remove rows where donor is in the explicit org/bank/fund list
    mask_donor_ok = ~df["donor"].isin(NON_COUNTRY_DONORS)

    # Remove rows where recipient is a region/aggregate (exact or via keywords)
    mask_recipient_ok = ~df["recipient"].apply(is_region_or_aggregate)

    df = df[mask_donor_ok & mask_recipient_ok].copy()

    return df[usecols]

chunks = []
for chunk in pd.read_csv(input_csv, usecols=usecols, chunksize=500_000):
    chunks.append(filter_chunk(chunk))

result = pd.concat(chunks, ignore_index=True)
print(f"Rows after dropping banks/orgs/regions + year 9999: {len(result):,}")

result.to_csv(output_csv, index=False)
print(f"Saved cleaned file to: {output_csv}")