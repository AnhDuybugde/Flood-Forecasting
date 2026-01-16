from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import os

def merge_pdfs():
    from pypdf import PdfReader, PdfWriter

    INPUT_DIR = "/opt/airflow/data/input/using"
    OUTPUT_PDF = "/opt/airflow/data/merged_2014_2024.pdf"

    # 👉 nếu đã merge rồi thì skip
    if os.path.exists(OUTPUT_PDF):
        print(f"⏭️  SKIP merge – file đã tồn tại: {OUTPUT_PDF}")
        return
    
    writer = PdfWriter()
    total_pages = 0
    pdf_count = 0

    print(f"📂 Reading PDFs from: {INPUT_DIR}")

    for fname in sorted(os.listdir(INPUT_DIR)):
        if fname.lower().endswith(".pdf"):
            pdf_path = os.path.join(INPUT_DIR, fname)
            reader = PdfReader(pdf_path)
            pdf_count += 1

            print(f"➡️  Adding {fname} | pages: {len(reader.pages)}")

            for page in reader.pages:
                writer.add_page(page)
                total_pages += 1

    with open(OUTPUT_PDF, "wb") as f:
        writer.write(f)

    print("✅ MERGE DONE")
    print(f"📄 PDFs merged : {pdf_count}")
    print(f"📄 Total pages: {total_pages}")
    print(f"📁 Output file: {OUTPUT_PDF}")


def extract_pdf():
    import pdfplumber, pandas as pd, re, os
    from unidecode import unidecode

    PDF_DIR = "/opt/airflow/data/input/using"
    OUTPUT_CSV = "/opt/airflow/data/output/sea_level_2014_2024_monthly.csv"

    rows = []
    months = ["Jan","Feb","Mar","Apr","May","Jun",
              "Jul","Aug","Sep","Oct","Nov","Dec"]

    def normalize(text):
        if not isinstance(text, str):
            return ""
        text = unidecode(text.lower())
        text = re.sub(r"\btram\b", "", text)
        return re.sub(r"\s+", " ", text).strip()

    for pdf_file in sorted(os.listdir(PDF_DIR)):
        if not pdf_file.lower().endswith(".pdf"):
            continue

        # 👉 lấy năm từ tên file
        m = re.search(r"(20\d{2})", pdf_file)
        if not m:
            continue
        year = int(m.group(1))

        print(f"📄 Processing {pdf_file} ({year})")

        with pdfplumber.open(os.path.join(PDF_DIR, pdf_file)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                for line in text.split("\n"):
                    nums = re.findall(r"[-+]?\d+(?:\.\d+)?", line)

                    # 👉 cần đúng 12 tháng
                    if len(nums) < 12:
                        continue

                    station_raw = re.sub(r"[-+]?\d+(?:\.\d+)?", "", line)
                    station = normalize(station_raw)

                    if not station:
                        continue

                    values = nums[:12]

                    for mth, val in zip(months, values):
                        try:
                            rows.append({
                                "Trạm": station.title(),
                                "Năm": year,
                                "Tháng": mth,
                                "Giá trị": float(val)
                            })
                        except:
                            pass

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["Trạm","Năm","Tháng"])
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(f"✅ Extract xong: {len(df)} records")


def done():
    print("✅ PDF → CSV hoàn tất")


with DAG(
    dag_id="pdf_merge_extract_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pdf", "etl"],
) as dag:

    merge = PythonOperator(
        task_id="merge_pdfs",
        python_callable=merge_pdfs
    )

    t1 = PythonOperator(
        task_id="extract_pdf",
        python_callable=extract_pdf
    )

    t2 = PythonOperator(
        task_id="done",
        python_callable=done
    )

    merge >> t1 >> t2

