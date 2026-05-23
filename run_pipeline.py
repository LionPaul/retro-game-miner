import os
import re
import time
from io import StringIO
import pandas as pd
import requests

# ==============================================================================
# 🎛️ GLOBAL PIPELINE CONFIGURATION
# ==============================================================================
CONSOLE_NAME = ""

# URLs de Extração (Altere aqui para mudar o console alvo do Pipeline)
WIKI_LIST_URL = ""
WIKI_SALES_URL = ""

# Credenciais do desenvolvedor (Obtidas no Twitch Developer Portal)
CLIENT_ID = ''
CLIENT_SECRET = ''

DATASETS_DIR = "datasets"
os.makedirs(DATASETS_DIR, exist_ok=True)
# ==============================================================================

def authenticate_igdb(client_id, client_secret):
    """Gera o Access Token OAuth2 para consumir a API do IGDB."""
    url = f"https://id.twitch.tv/oauth2/token?client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials"
    response = requests.post(url)
    response.raise_for_status()
    return response.json()['access_token']

def fetch_score_api(game_name, headers):
    """Busca a nota crítica/pública do jogo no IGDB"""
    if not game_name or pd.isna(game_name) or str(game_name).strip() == "":
        return None
    url = "https://api.igdb.com/v4/games"
    query = f'search "{str(game_name).strip()}"; fields name, aggregated_rating, rating; where platforms = (19); limit 1;'
    try:
        response = requests.post(url, headers=headers, data=query)
        if response.status_code == 200:
            data = response.json()
            if data:
                game = data[0]
                score = game.get('aggregated_rating', game.get('rating', None))
                if score:
                    return round(score, 2)
    except Exception:
        pass
    return None

def fetch_genres_dictionary(headers):
    """Mapeia o dicionário global de IDs para nomes de Gêneros do IGDB."""
    url = "https://api.igdb.com/v4/genres"
    query = "fields id, name; limit 500;"
    response = requests.post(url, headers=headers, data=query)
    if response.status_code == 200:
        return {item['id']: item['name'] for item in response.json()}
    return {}

def fetch_game_genres_api(game_name, headers, genres_map):
    """Busca e unpacka até 5 gêneros associados ao jogo no IGDB."""
    if not game_name or pd.isna(game_name) or str(game_name).strip() == "":
        return [None] * 5
    url = "https://api.igdb.com/v4/games"
    query = f'search "{str(game_name).strip()}"; fields genres; where platforms = (19); limit 1;'
    try:
        response = requests.post(url, headers=headers, data=query)
        if response.status_code == 200:
            data = response.json()
            if data and 'genres' in data[0]:
                genre_ids = data[0]['genres']
                genre_names = [genres_map.get(gid) for gid in genre_ids if gid in genres_map]
                genre_names = genre_names[:5]
                while len(genre_names) < 5:
                    genre_names.append(None)
                return genre_names
    except Exception:
        pass
    return [None] * 5

def clean_human_strings(text):
    """Sanitiza strings limpando flags regionais e resíduos corporativos."""
    if not text or pd.isna(text):
        return "Unknown"
    text = str(text)
    parts = re.split(r'\(|/|•', text)
    clean_name = parts[0].strip()
    return re.sub(r'\[.*\]', '', clean_name).strip()

def gerar_5_estrategias_busca(nome_completo):
    """Algoritmo Pentatendativa: gera 5 variações textuais de busca."""
    nome = str(nome_completo)
    nome = re.sub(r'\[.*\]|\(.*\)', '', nome)
    partes = nome.split('•')
    
    nome_p = partes[0]
    nome_p = re.sub(r'\d{4}-\d{2}-\d{2}.*|\d{4}.*', '', nome_p)
    nome_p = re.sub(r'(PAL|JP|NA|USA|JPN|MX)$', '', nome_p, flags=re.IGNORECASE).strip()
    nome_p = nome_p.rstrip(':-— ,.')
    title_1 = nome_p if len(nome_p) > 1 else None

    title_2 = None
    if len(partes) > 1:
        nome_alt = partes[1]
        nome_alt = re.sub(r'\d{4}-\d{2}-\d{2}.*|\d{4}.*', '', nome_alt)
        nome_alt = re.sub(r'(PAL|JP|NA|USA|JPN|MX)$', '', nome_alt, flags=re.IGNORECASE).strip()
        nome_alt = nome_alt.rstrip(':-— ,.')
        if len(nome_alt) > 1:
            title_2 = nome_alt

    palavras = nome_p.split()
    title_3 = None
    if len(palavras) > 1:
        title_3 = " ".join(palavras[:2])
        if title_3.lower() in ['the', 'super', 'mega', 'a', 'an', 'legend of'] and len(palavras) > 2:
            title_3 = " ".join(palavras[:3])

    title_4 = None
    if len(palavras) >= 3:
        sigla = "".join([palavra[0] for word in palavras if (palavra := re.sub(r'\W+', '', word))])
        if len(sigla) >= 3:
            title_4 = sigla.upper()

    title_5 = None
    stopwords = ['the', 'of', 'and', 'to', 'a', 'an', 'in', 'for', 'on']
    palavras_filtradas = [w for w in palavras if w.lower() not in stopwords]
    if len(palavras_filtradas) < len(palavras) and len(palavras_filtradas) > 0:
        title_5 = " ".join(palavras_filtradas)

    return pd.Series([title_1, title_2, title_3, title_4, title_5])

# ==============================================================================
# 🚀 CORE ORCHESTRATION ENGINE
# ==============================================================================
def run_end_to_end_pipeline():
    print(f"🏁 Starting Full Automated Pipeline for: {CONSOLE_NAME.upper()}")
    
    # --- STAGE 1 & 2: BRONZE EXTRACTION & SILVER CLEANING ---
    print("🌐 Crawling Wikipedia raw dataset stream...")
    headers_web = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(WIKI_LIST_URL, headers=headers_web)
    tables = pd.read_html(StringIO(res.text))
    df = max(tables, key=len)
    
    # Padronização e Limpeza Silver
    mapeamento = {}
    for col in df.columns:
        c_clean = col.lower()
        if 'title' in c_clean: mapeamento[col] = 'Title'
        elif 'developer' in c_clean: mapeamento[col] = 'Developer'
        elif 'publisher' in c_clean: mapeamento[col] = 'Publisher'
        elif 'japan' in c_clean: mapeamento[col] = 'Release_Japan'
        elif 'north america' in c_clean or 'na' in c_clean: mapeamento[col] = 'Release_NA'
        elif 'pal region' in c_clean or 'pal' in c_clean: mapeamento[col] = 'Release_PAL'
    df = df.rename(columns=mapeamento)
    
    if 'Release_NA' in df.columns:
        df = df[~df['Release_NA'].astype(str).str.contains('Unreleased', na=False, case=False)]
        
    colunas_datas = ['Release_Japan', 'Release_NA', 'Release_PAL']
    colunas_presentes = [c for c in colunas_datas if c in df.columns]
    if colunas_presentes:
        for col in colunas_presentes:
            df[col] = df[col].astype(str).str.replace(r'\[.*\]', '', regex=True)
        datas_conv = df[colunas_presentes].apply(pd.to_datetime, errors='coerce')
        df['Original_Release'] = datas_conv.min(axis=1).dt.strftime('%Y-%m-%d')
        df = df.drop(columns=colunas_presentes, errors='ignore')
        
    df[['T1_Clean', 'T2_Fallback', 'T3_Keyword', 'T4_Acronym', 'T5_NoStopwords']] = df['Title'].apply(gerar_5_estrategias_busca)
    df.to_excel(os.path.join(DATASETS_DIR, f"2_{CONSOLE_NAME}_games_cleaned.xlsx"), index=False)

    # --- STAGE 3 & 4: API POPULATION & GATEKEEPER FILTERING ---
    print("🔑 Authenticating with IGDB REST API...")
    token = authenticate_igdb(CLIENT_ID, CLIENT_SECRET)
    headers_api = {'Client-ID': CLIENT_ID, 'Authorization': f'Bearer {token}'}
    
    print("⚡ Mining game scores via Pentatendativa cascade...")
    final_scores = []
    for idx, row in df.iterrows():
        strategies = [row['T1_Clean'], row['T2_Fallback'], row['T3_Keyword'], row['T4_Acronym'], row['T5_NoStopwords']]
        score = None
        for term in strategies:
            if pd.notna(term):
                score = fetch_score_api(term, headers_api)
                if score: break
                time.sleep(0.1)
        final_scores.append(score)
        time.sleep(0.26)
        
    df['IGDB_Score'] = final_scores
    df.to_excel(os.path.join(DATASETS_DIR, f"3_{CONSOLE_NAME}_games_populated.xlsx"), index=False)
    
    # Filtragem cirúrgica dos válidos (Notebook 4)
    df_filtered = df.dropna(subset=['IGDB_Score']).copy()
    df_filtered = df_filtered.drop_duplicates(subset=['T1_Clean'])
    df_filtered.to_excel(os.path.join(DATASETS_DIR, f"4_{CONSOLE_NAME}_games_scored_only.xlsx"), index=False)

    # --- STAGE 5: CATEGORY MAPPING ---
    print("🏷️ Unpacking and mapping multi-dimensional genres...")
    genres_map = fetch_genres_dictionary(headers_api)
    cat1, cat2, cat3, cat4, cat5 = [], [], [], [], []
    
    for idx, row in df_filtered.iterrows():
        g_genres = fetch_game_genres_api(row['T1_Clean'], headers_api, genres_map)
        cat1.append(g_genres[0]); cat2.append(g_genres[1]); cat3.append(g_genres[2]); cat4.append(g_genres[3]); cat5.append(g_genres[4])
        time.sleep(0.26)
        
    df_filtered['Category_Primary'] = cat1
    df_filtered['Category_Secondary'] = cat2
    df_filtered['Category_Tertiary'] = cat3
    df_filtered['Category_Quaternary'] = cat4
    df_filtered['Category_Quinary'] = cat5
    df_filtered.to_excel(os.path.join(DATASETS_DIR, f"5_{CONSOLE_NAME}_games_with_genres.xlsx"), index=False)

    # --- STAGE 6 & 7: SALES JOIN & MARKET ISOLATION ---
    print("💰 Integrating economic metrics from Wikipedia sales stream...")
    res_sales = requests.get(WIKI_SALES_URL, headers=headers_web)
    tables_sales = pd.read_html(StringIO(res_sales.text))
    df_sales = max(tables_sales, key=len)
    df_sales.columns = [col.split('[')[0].strip() for col in df_sales.columns]
    
    col_g = [c for c in df_sales.columns if 'Game' in c or 'Title' in c][0]
    col_s = [c for c in df_sales.columns if 'Sales' in c or 'Copies' in c][0]
    df_sales_clean = df_sales[[col_g, col_s]].copy()
    df_sales_clean.columns = ['Match_Title', 'Copies_Sold_Millions']
    df_sales_clean['Copies_Sold_Millions'] = df_sales_clean['Copies_Sold_Millions'].astype(str).str.replace(r'million.*|\[.*\]', '', regex=True).str.strip().astype(float)
    
    df_filtered['join_key'] = df_filtered['T1_Clean'].astype(str).str.lower().str.replace(r'\W+', '', regex=True)
    df_sales_clean['join_key'] = df_sales_clean['Match_Title'].astype(str).str.lower().str.replace(r'\W+', '', regex=True)
    df_sales_clean = df_sales_clean.drop_duplicates(subset=['join_key'])
    
    df_perfect = pd.merge(df_filtered, df_sales_clean[['join_key', 'Copies_Sold_Millions']], on='join_key', how='left')
    df_perfect['Copies_Sold_Millions'] = df_perfect['Copies_Sold_Millions'].fillna(0)
    df_perfect = df_perfect.drop(columns=['join_key']).sort_values(by=['IGDB_Score', 'Copies_Sold_Millions'], ascending=[False, False])
    df_perfect.to_excel(os.path.join(DATASETS_DIR, f"6_{CONSOLE_NAME}_games_perfect_list.xlsx"), index=False)
    
    # Best-sellers isolados (Notebook 7)
    df_bestsellers = df_perfect[df_perfect['Copies_Sold_Millions'] > 0].copy().sort_values(by='Copies_Sold_Millions', ascending=False)
    df_bestsellers.to_excel(os.path.join(DATASETS_DIR, f"7_{CONSOLE_NAME}_games_bestsellers.xlsx"), index=False)

    # --- STAGE 8 & 9: PRODUCTION DATA MARTS GENERATION ---
    print("💎 Shifting data shapes into production-grade BI Data Marts...")
    
    # Mart 1: Financial Performance (Usa os Best-sellers como herança)
    df_bestsellers['Publisher'] = df_bestsellers['Publisher'].apply(clean_human_strings)
    df_bestsellers['Developer'] = df_bestsellers['Developer'].apply(clean_human_strings)
    df_fi_mart = pd.DataFrame({
        'Title': df_bestsellers['T1_Clean'], 'Publisher': df_bestsellers['Publisher'], 'Developer': df_bestsellers['Developer'],
        'Category': df_bestsellers['Category_Primary'].fillna('Unclassified'), 'Release': df_bestsellers['Original_Release'],
        'Score': df_bestsellers['IGDB_Score'], 'Sold': df_bestsellers['Copies_Sold_Millions']
    })
    df_fi_mart.to_excel(os.path.join(DATASETS_DIR, f"{CONSOLE_NAME}_financial_performance.xlsx"), index=False)
    
    # Mart 2: Technical Performance (Usa os dados com categorias completos)
    df_filtered['Publisher'] = df_filtered['Publisher'].apply(clean_human_strings)
    df_filtered['Developer'] = df_filtered['Developer'].apply(clean_human_strings)
    df_tech_mart = pd.DataFrame({
        'Title': df_filtered['T1_Clean'], 'Publisher': df_filtered['Publisher'], 'Developer': df_filtered['Developer'],
        'Score': df_filtered['IGDB_Score'], 'Category': df_filtered['Category_Primary'].fillna('Unclassified'), 'Release': df_filtered['Original_Release']
    })
    df_tech_mart.to_excel(os.path.join(DATASETS_DIR, f"{CONSOLE_NAME}_technical_performance.xlsx"), index=False)
    
    print(f"🏆 SUCCESS! Full pipeline executed. Deliverables saved in /{DATASETS_DIR}")

if __name__ == "__main__":
    # Garanta que colocou seu Client ID e Secret da Twitch antes de executar!
    if CLIENT_ID == "" or CLIENT_SECRET == "":
        print("❌ Blocked: Please provide your Twitch/IGDB API credentials inside the script configuration block.")
    else:
        run_end_to_end_pipeline()
