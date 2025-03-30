from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
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
# MAIN SCRAPING LOGIC
######################
def run_scraper(**kwargs):
    ti = kwargs['ti']
    last_page = ti.xcom_pull(key='last_page', task_ids='scrape_and_store')
    if last_page is None:
        current_page = kwargs.get("start_page", 1)
    else:
        current_page = last_page + 1

    batch_size = 10
    # Retrieve start_page from kwargs; default to 1 if not provided
    start_page = kwargs.get("start_page", 1)
    total_pages = kwargs.get("total_pages", 1306)  # Total pages if known
    batch_number = ((start_page - 1) // batch_size) + 1
    total_rows = 0

    while current_page <= total_pages:
        print(f"Processing batch {batch_number}: pages {current_page} to {current_page + batch_size - 1}")
        try:
            batch_rows = []
            orig_headers = None
            # Process pages for this batch
            for page in range(current_page, current_page + batch_size):
                print(f"Scraping page {page}...")
                current_headers, rows = scrape_page(page)
                # If no rows found on a page, assume it's the end.
                if rows is None or len(rows) == 0:
                    print(f"No rows found on page {page}. Ending batch loop.")
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
                batch_rows.extend(rows)
                time.sleep(1)  # one‑second pause between page requests

            # If no rows in this batch, exit loop
            if not batch_rows:
                print(f"No data in batch {batch_number}. Exiting loop.")
                break

            total_rows += len(batch_rows)
            df = pd.DataFrame(batch_rows)
            print("Columns before rename:", df.columns.tolist())
            df.columns = df.columns.str.strip()
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
            missing_columns = [col for col in desired_order if col not in df.columns]
            if missing_columns:
                print("Missing columns:", missing_columns)
            else:
                df = df[desired_order]
            print(df.head(10))

            # Save CSV for this batch
            today_str = datetime.now().strftime("%Y%m%d")
            csv_filename = f"construction_{today_str}_batch_{batch_number}_pages_{current_page}_{current_page+batch_size-1}.csv"
            csv_path = f"/tmp/{csv_filename}"
            if os.path.exists(csv_path):
                os.remove(csv_path)
            df.to_csv(csv_path, index=False, encoding="utf-8")
            print("CSV file created:", csv_path)

            # Upload to MinIO
            client = create_minio_client()
            if not client.bucket_exists(MINIO_BUCKET):
                client.make_bucket(MINIO_BUCKET)
            minio_object = f"teduh/{csv_filename}"
            client.fput_object(
                bucket_name=MINIO_BUCKET,
                object_name=minio_object,
                file_path=csv_path
            )
            print(f"Uploaded {csv_path} to minio://{MINIO_BUCKET}/{minio_object}")

            # Cleanup local CSV file
            os.remove(csv_path)
            print(f"Batch {batch_number} processed with {len(batch_rows)} rows.")

        except Exception as e:
            print(f"Error processing batch {batch_number}: {str(e)}")
            # Optionally, log the error or push partial results if available.
            # Continue with the next batch even if this one fails.
        
        # Push the last processed page number to XCom so that next run can resume
        ti.xcom_push(key='last_page', value=current_page + batch_size - 1)
        
        total_rows += len(batch_rows)
        # Proceed to the next batch
        current_page += batch_size
        batch_number += 1

    print(f"Scraping complete. Total rows scraped: {total_rows}")

default_args = {
    'owner': 'waiyong',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    #'execution_timeout': timedelta(minutes=30)  # Maximum allowed runtime
}

with DAG(
    dag_id='split_web_scraping_v02',
    default_args=default_args,
    description='Web scrape construction data',
    start_date=datetime(2025, 3, 22),
    schedule_interval='@weekly',  # every Sunday at 00:00
    catchup=False
) as dag:
    scrape_and_store_task = PythonOperator(
        task_id='scrape_and_store',
        python_callable=run_scraper,
        op_kwargs={'start_page': 1161, 'total_pages': 1306}  # Change these values for different batches
    )

    scrape_and_store_task
