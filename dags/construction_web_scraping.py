from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import sqlite3
import subprocess
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# MinIO client
from minio import Minio

######################
# MinIO CONFIG
######################
MINIO_ENDPOINT = "host.docker.internal:9000"       # e.g., "play.min.io" or "127.0.0.1:9000"
MINIO_ACCESS_KEY = "ROOTUSER"
MINIO_SECRET_KEY = "CHANGEME123"
MINIO_BUCKET = "construction-web-scraping"
# Remove or comment out the global definition that uses today_str:
# MINIO_CSV_OBJECT = f"teduh/construction_{today_str}.csv"  # Path in your bucket 

def create_minio_client():
    """Helper to create and return a Minio client instance."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # or True if using HTTPS
    )

######################
# SCRAPING FUNCTIONS
######################
def scrape_additional_data(link):
    daerah_projek = ""
    negeri_projek = ""
    harga_min = ""
    harga_maks = ""

    try:
        response = requests.get(link, timeout=10)
        if response.status_code != 200:
            return daerah_projek, negeri_projek, harga_min, harga_maks
        
        soup = BeautifulSoup(response.content, 'html.parser')

        # B. MAKLUMAT PROJEK
        section_heading = soup.find(
            lambda tag: tag.name == "p" 
            and "B. MAKLUMAT PROJEK" in tag.text 
            and "font-bold" in tag.get("class", [])
        )
        
        if section_heading:
            container = section_heading.find_parent("div", class_="row")
            while container:
                rows = container.find_all("div", class_="col-12")
                for row in rows:
                    label = row.find("p", class_="font-bold")
                    value = row.find("p", class_="font-medium")
                    
                    if label and value:
                        label_text = label.get_text(strip=True).upper()
                        value_text = value.get_text(strip=True)
                        if "DAERAH" in label_text:
                            daerah_projek = value_text
                        elif "NEGERI" in label_text:
                            negeri_projek = value_text
                container = container.find_next_sibling("div", class_="row")

        # C. MAKLUMAT HARGA
        price_heading = soup.find(
            lambda tag: tag.name == "p" 
            and "C. MAKLUMAT HARGA" in tag.text 
            and "font-bold" in tag.get("class", [])
        )
        
        if price_heading:
            price_container = price_heading.find_parent("div", class_="row")
            while price_container:
                rows = price_container.find_all("div", class_="col-12")
                for row in rows:
                    label = row.find("p", class_="font-bold")
                    value = row.find("p", class_="font-medium")
                    
                    if label and value:
                        label_text = label.get_text(strip=True).upper()
                        value_text = value.get_text(strip=True)
                        
                        if "HARGA MINIMUM" in label_text:
                            harga_min = value_text.split("RM")[-1].strip()
                        elif "HARGA MAKSIMUM" in label_text:
                            harga_maks = value_text.split("RM")[-1].strip()
                
                price_container = price_container.find_next_sibling("div", class_="row")

        # D. STATUS TERKINI PROJEK
        status_section = soup.find('p', class_='font-bold', string='D. STATUS TERKINI PROJEK')
        if status_section:
            table_container = status_section.find_next(
                'div', class_='col-12 overflow-x-scroll no-scrollbar'
            )
            if table_container:
                table = table_container.find('table')
                if table:
                    headers = [th.get_text(strip=True) for th in table.select('thead th')]
                    try:
                        min_idx = headers.index('Harga Minimum (RM)')
                        max_idx = headers.index('Harga Maksimum (RM)')
                    except ValueError:
                        return daerah_projek, negeri_projek, harga_min, harga_maks

                    row = table.select_one('tbody tr')
                    if row:
                        cells = row.find_all('td')
                        if len(cells) > max(min_idx, max_idx):
                            harga_min = cells[min_idx].get_text(strip=True)
                            harga_maks = cells[max_idx].get_text(strip=True)

    except Exception as e:
        print(f"Error scraping {link}: {str(e)}")
    
    return daerah_projek, negeri_projek, harga_min, harga_maks

def scrape_page(page_number):
    url = f'https://teduh.kpkt.gov.my/project-swasta?page={page_number}'
    response = requests.get(url)
    if response.status_code != 200:
        return None, None

    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table')
    if not table:
        return None, None

    thead = table.find('thead')
    headers = [th.text.strip() for th in thead.find_all('th')]
    
    rows = []
    tbody = table.find('tbody')
    for tr in tbody.find_all('tr'):
        row_data = {}
        cells = tr.find_all('td')
        for idx, td in enumerate(cells):
            header = headers[idx]
            # For "RINGKASAN PROJEK", extract the link
            if header.upper() == "RINGKASAN PROJEK":
                a_tag = td.find('a')
                link = a_tag['href'] if a_tag and a_tag.has_attr('href') else ''
                row_data[header] = link
            else:
                row_data[header] = td.text.strip()
        rows.append(row_data)
    return headers, rows

######################
# 2) MAIN SCRAPING LOGIC
######################
def run_scraper(**kwargs):
    all_rows = []
    orig_headers = None

    page_number = 1
    while True:
    # Limit to first 5 pages for testing
    # for page_number in range(1, 2):
        print(f"Scraping page {page_number}...")
        current_headers, rows = scrape_page(page_number)
        if rows is None or len(rows) == 0:
            break
        if orig_headers is None:
            orig_headers = current_headers

        for row in rows:
            link = row.get("RINGKASAN PROJEK", "")
            if link:
                if not link.startswith("http"):
                    link = "https://teduh.kpkt.gov.my" + link
                daerah, negeri, harga_min, harga_maks = scrape_additional_data(link)
                row["Daerah Projek"] = daerah
                row["Negeri Projek"] = negeri
                row["Harga Minimum (RM)"] = harga_min
                row["Harga Maksimum (RM)"] = harga_maks
            else:
                row["Daerah Projek"] = ""
                row["Negeri Projek"] = ""
                row["Harga Minimum (RM)"] = ""
                row["Harga Maksimum (RM)"] = ""
        all_rows.extend(rows)
        time.sleep(1) # Polite delay between pages
        page_number += 1

    df = pd.DataFrame(all_rows)

    print("Columns before rename:", df.columns.tolist())

    df.columns = df.columns.str.strip()

    # Rename columns
    df = df.rename(columns={
        'BIL.': 'Bil',
        'KOD PROJEK': 'Kod Pemajuan',
        'PEMAJU': 'Nama Pemaju',
        'PROJEK': 'Nama Projek',
        'NO. PERMIT': 'No. Permit'
    })

    desired_order = [
        'Bil',
        'Kod Pemajuan',
        'Nama Pemaju',
        'Nama Projek',
        'Daerah Projek', 
        'Negeri Projek',
        'No. Permit',
        'Harga Minimum (RM)',
        'Harga Maksimum (RM)',
        'STATUS PROJEK KESELURUHAN',
        'RINGKASAN PROJEK'
    ]

    # Reorder if all columns exist
    missing_columns = [col for col in desired_order if col not in df.columns]
    if missing_columns:
        print("Missing columns:", missing_columns)
    else:
        df = df[desired_order]

    # Show sample
    print(df.head(10))

    # -----------
    # 3) SAVE TO CSV with dynamic file name construction_YYYYMMDD.csv
    # --------------------
    today_str = datetime.now().strftime("%Y%m%d")
    csv_path = f"/tmp/construction_{today_str}.csv"
    if os.path.exists(csv_path):
        os.remove(csv_path)

    df.to_csv(csv_path, index=False, encoding="utf-8")
    print("CSV file created:", csv_path)

    # --------------------
    # 4) UPLOAD TO MINIO
    # --------------------
    client = create_minio_client()

    found = client.bucket_exists(MINIO_BUCKET)
    if not found:
        client.make_bucket(MINIO_BUCKET)

    # For clarity, rename MINIO_SQL_OBJECT to something like MINIO_CSV_OBJECT
    MINIO_CSV_OBJECT = f"teduh/construction_{today_str}.csv"  # Adjust as needed

    client.fput_object(
        bucket_name=MINIO_BUCKET,
        object_name=MINIO_CSV_OBJECT,
        file_path=csv_path
    )
    print(f"Uploaded {csv_path} to minio://{MINIO_BUCKET}/{MINIO_CSV_OBJECT}")

    # Cleanup local file if desired
    os.remove(csv_path)
    print("Scraping and upload done!")

default_args = {
    'owner': 'waiyong',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30)  # Adjust as needed
}

with DAG(
    dag_id='construction_web_scraping_v01',
    default_args=default_args,
    description='Web scape construction data',
    start_date=datetime(2025, 3, 1),
    schedule_interval='0 0 * * 0', # every sunday at 00:00
) as dag:
    scrape_and_store_task = PythonOperator(
        task_id='scrape_and_store',
        python_callable=run_scraper
    )

    scrape_and_store_task