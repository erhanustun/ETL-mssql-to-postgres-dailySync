"""
## DAG: MSSQL'den PostgreSQL'e Günlük Artımlı Senkronizasyon

Amaç:
Bu DAG, kaynak(MSSQL) veritabanındaki 'transactions' tablosundan,
hedef PostgreSQL veritabanındaki 'transactions' tablosuna günlük olarak artımlı
veri senkronizasyonunu gerçekleştirir. Senkronizasyon, kaydın oluşturulma tarihi
('OrderCreatedAt') baz alınarak yapılır.

İş Akışı:
1.  Extract: Her gün, bir önceki günün mantıksal tarihi ('logical_date' veya 'ds') için
    MSSQL 'transactions' tablosundan 'OrderCreatedAt' si o tarihe denk gelen
    kayıtları seçer.
2.  Load: Çekilen veriyi geçici bir dosyaya yazar, ardından bu dosyayı okuyarak
    PostgreSQL 'transactions' tablosuna yükler. Yükleme sırasında, önceden tanımlanmış
    benzersiz anahtar sütunlarına ('UPSERT_CONFLICT_COLUMNS') göre çakışma kontrolü
    yapılır ('ON CONFLICT DO UPDATE'). Bu sayede, hedefte zaten var olan kayıtlar
    güncellenir, yeni kayıtlar eklenir (UPSERT).
3.  Validate: MSSQL'den çekilen satır sayısı ile PostgreSQL'e yüklenen (ve o gün
    için hedef tabloda bulunan) gerçek satır sayısını karşılaştırır ve sonucu loglar.

Çalışma Prensibi:
*   Zamanlama: 'schedule=timedelta(days=1)' ile günlük olarak çalışır.
*   Catchup: 'catchup=True' ile, start_date'ten itibaren eksik kalan günler için
    otomatik olarak çalışarak geçmiş veriyi işler (backfill).
*   Sıralı İşleme: 'depends_on_past=True' ve 'max_active_runs=1' ile backfill
    işleminin sıralı ve kontrollü yapılması sağlanır.
*   Idempotency: UPSERT mekanizması sayesinde DAG idempotenttir; aynı gün için
    tekrar çalıştırılması hedef veritabanında veri tekrarına veya bozulmaya yol açmaz.
*   E-posta Uyarıları: 'default_args' içinde yapılandırıldıysa, görev başarısızlıklarında
    belirtilen alıcılara e-posta gönderir.

Notlar:
*   Güncelleme Takibi Yok: Bu versiyon, sadece 'OrderCreatedAt' ye göre veri çeker.
    Kaynakta oluşturulduktan sonraki günlerde güncellenen kayıtlar (`UpdatedDateUtc`
    değişirse) bu DAG tarafından yakalanmaz. Güncellemelerin yakalanması için CDC gereklidir.
*   Catchup Yönetimi: Geçmiş verinin eksiksiz işlenmesi için 'catchup=True' olarak ayarlanmıştır. 
    Tüm geçmiş verinin senkronizasyonu tamamlandıktan sonra, DAG'ın sadece zamanlanmış günlük çalıştırmalara odaklanması için 'catchup=False' olarak güncellenmesi yapılabilir.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import pendulum 
import psycopg2
import pyodbc
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from psycopg2.extras import execute_values

from airflow.utils.dates import days_ago


# Loglama ve Ortam Değişkenleri Yükleme
env_path = os.getenv('AIRFLOW_ENV_PATH', os.path.join(os.path.dirname(__file__), '..', '.env'))
if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path, override=True)
    logging.info(f".env dosyası yüklendi: {env_path}")
else:
    logging.warning(f".env dosyası bulunamadı: {env_path}. Ortam değişkenleri veya varsayılanlar kullanılacak.")

# E-posta Uyarı Alıcıları
alert_recipients_str = os.getenv("ALERT_EMAIL_RECIPIENTS", "")
ALERT_EMAIL_RECIPIENTS = [email.strip() for email in alert_recipients_str.split(',') if email.strip() and '@' in email] # Basit format kontrolü

if not ALERT_EMAIL_RECIPIENTS:
    logging.warning("ALERT_EMAIL_RECIPIENTS ortam değişkeni bulunamadı veya geçerli e-posta içermiyor. E-posta gönderilmeyecek.")
else:
    logging.info(f"E-posta uyarıları şu alıcılara gönderilecek: {ALERT_EMAIL_RECIPIENTS}")


# MSSQL Bağlantı Bilgileri (Kaynak)
MSSQL_CONN_PARAMS = {
    "server": os.getenv("DB_SERVER", "mssql,1433"),
    "database": os.getenv("DB_DATABASE", "source_db"),
    "username": os.getenv("DB_USERNAME", "sa"),
    "password": os.getenv("DB_PASSWORD", "MyPass123"),
    "driver": os.getenv("DB_DRIVER", "{ODBC Driver 18 for SQL Server}"),
    "timeout": int(os.getenv("MSSQL_TIMEOUT", "120")),
    "extra_opts": os.getenv("MSSQL_EXTRA_OPTS", "Encrypt=no;TrustServerCertificate=yes"),
}

# PostgreSQL Bağlantı Bilgileri (Hedef)
POSTGRES_CONN_DETAILS = {
    "dbname": os.getenv("PG_DBNAME", "airflow"),
    "user": os.getenv("PG_USER", "airflow"),
    "password": os.getenv("PG_PASSWORD", "airflow"), 
    "host": os.getenv("PG_HOST", "postgresql"),
    "port": os.getenv("PG_PORT", "5432"),
    "connect_timeout": int(os.getenv("PG_TIMEOUT", "60")),
}

# Geçici Dosya ve Tablo Adları
TEMP_FILE_PATH = os.getenv("TEMP_FILE_PATH", "/opt/airflow/shared/mssql_extract_for_pg.csv")
SOURCE_TABLE_NAME = os.getenv("SOURCE_TABLE_NAME", "orders")
TARGET_TABLE_NAME = os.getenv("TARGET_TABLE_NAME", "orders")


# PostgreSQL UPSERT Ayarları
_default_conflict_cols = "order_id"
UPSERT_CONFLICT_COLUMNS = [
    col.strip().lower() for col in os.getenv("UNIQUE_KEY_COLUMNS", _default_conflict_cols).split(',') if col.strip()]

TARGET_INCOMPLETE_TABLE_NAME = os.getenv("TARGET_INCOMPLETE_TABLE_NAME", "incomplete_orders") # Yeni tablo adı
TEMP_NULL_FILE_PATH = os.getenv("TEMP_NULL_FILE_PATH", "/tmp/mssql_null_orders_extract.csv") # Yeni geçici dosya yolu


if not UPSERT_CONFLICT_COLUMNS:
    raise ValueError("UNIQUE_KEY_COLUMNS ortam değişkeni bulunamadı veya boş!")

# Performans Ayarları
PG_PAGE_SIZE = int(os.getenv("PG_PAGE_SIZE", "100"))
MSSQL_EXTRACT_CHUNKSIZE = int(os.getenv("MSSQL_EXTRACT_CHUNKSIZE", "5000"))

# --- DAG Tanımı ---
FIRST_DATA_DATE_IN_MSSQL = pendulum.datetime(2025, 1, 1, tz="UTC")

# --- DAG Tanımı ---
FIRST_DATA_DATE_IN_MSSQL = pendulum.datetime(2025, 1, 1, tz="UTC") # MSSQL'deki en eski OrderCreatedAt tarihi

with DAG(
    dag_id='orders_daily_sync',
    start_date=FIRST_DATA_DATE_IN_MSSQL,
    schedule=timedelta(days=1),
    catchup=True, # Geçmiş veriyi yakalamak için True
    max_active_runs=1,
    default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'email': ALERT_EMAIL_RECIPIENTS if ALERT_EMAIL_RECIPIENTS else [],
    'email_on_failure': bool(ALERT_EMAIL_RECIPIENTS),
    'email_on_retry': False,
    },
    tags=['mssql', 'postgres', 'sync', 'incremental', 'backfill', 'etl', 'alerting'],
    doc_md="""
    ## DAG: MSSQL'den PostgreSQL'e Günlük Artımlı Senkronizasyon (Tamamlanmış ve Eksik Siparişler)

    Bu DAG, kaynak (MSSQL) veritabanındaki 'orders' tablosundan, hedef PostgreSQL veritabanındaki
    'orders' tablosuna günlük olarak artımlı veri senkronizasyonunu gerçekleştirir.
    Senkronizasyon, kaydın oluşturulma tarihi ('OrderCreatedAt') baz alınarak yapılır.

    Ek olarak, 'OrderCreatedAt' değeri NULL olan kayıtlar ayrı bir 'incomplete_orders' tablosuna
    her çalıştırmada TRUNCATE edilerek yeniden yüklenir. Bu, henüz tamamlanmamış siparişlerin
    takibini sağlar.
    """,
    description=f"Daily sync from MSSQL.{SOURCE_TABLE_NAME} to PG.{TARGET_TABLE_NAME} with email alerts, including NULL OrderCreatedAt handling.",
) as dag:

    # Ortak yardımcı fonksiyonlar
    def _build_mssql_conn_str() -> str:
        """MSSQL için pyodbc bağlantı dizesini oluşturur."""
        log = logging.getLogger(__name__)
        log_params = {k: v for k, v in MSSQL_CONN_PARAMS.items() if k != 'password'}
        log.debug(f"MSSQL bağlantı parametreleri (şifre hariç): {log_params}")
        conn_parts = [f"DRIVER={MSSQL_CONN_PARAMS['driver']}", f"SERVER={MSSQL_CONN_PARAMS['server']}", f"DATABASE={MSSQL_CONN_PARAMS['database']}", f"UID={MSSQL_CONN_PARAMS['username']}", f"PWD={MSSQL_CONN_PARAMS['password']}"]
        extra = MSSQL_CONN_PARAMS.get('extra_opts')
        if extra: conn_parts.append(extra)
        conn_str = ";".join(conn_parts)
        log.debug(f"Oluşturulan MSSQL bağlantı dizesi (şifre hariç): {conn_str.replace(MSSQL_CONN_PARAMS['password'], '******')}")
        return conn_str

    def _cleanup_temp_file(file_path: str):
        """Belirtilen geçici dosyayı siler (varsa)."""
        log = logging.getLogger(__name__)
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                log.info(f"Geçici dosya silindi: {file_path}")
            except OSError as e:
                log.warning(f"Geçici dosya silinemedi {file_path}: {e}")

    # --- AKIŞ 1: OrderCreatedAt NULL OLMAYAN KAYITLAR ---

    def extract_complete_orders_data(**context):
        """
        Belirtilen mantıksal tarih için MSSQL'den OrderCreatedAt NULL olmayan veriyi çeker,
        geçici bir CSV dosyasına yazar ve satır sayısını XCom'a gönderir.
        """
        log = logging.getLogger(__name__)
        logical_date_str = context['ds']
        task_instance = context['ti']
        log.info(f"MSSQL'den tamamlanmış sipariş verisi çıkarma işlemi başladı. Tablo: '{SOURCE_TABLE_NAME}', Tarih: {logical_date_str}")
        conn_str = _build_mssql_conn_str()
        conn = None
        extracted_count = 0
        try:
            log.info(f"MSSQL'e bağlanılıyor: Sunucu={MSSQL_CONN_PARAMS['server']}...")
            conn = pyodbc.connect(conn_str, timeout=MSSQL_CONN_PARAMS['timeout'], autocommit=True)
            log.info("MSSQL bağlantısı başarılı.")
            # OrderCreatedAt IS NOT NULL koşulu kaldırıldı, çünkü CAST otomatik olarak NULL'ları atlar.
            query = f"SELECT * FROM dbo.{SOURCE_TABLE_NAME} WHERE CAST(OrderCreatedAt AS DATE) = ?"
            log.info(f"Sorgu çalıştırılıyor (tarih: {logical_date_str})...")

            chunk_list = []
            df_iterator = pd.read_sql(query, conn, params=[logical_date_str], chunksize=MSSQL_EXTRACT_CHUNKSIZE)
            for i, chunk in enumerate(df_iterator):
                 if chunk.empty: continue
                 chunk_list.append(chunk)
                 extracted_count += len(chunk)
                 if (i + 1) % 10 == 0: log.info(f"{i+1} chunk okundu. Toplam: {extracted_count}")

            if not chunk_list:
                log.warning(f"MSSQL'de {logical_date_str} için tamamlanmış sipariş verisi bulunamadı.")
                _cleanup_temp_file(TEMP_FILE_PATH) # Kendi geçici dosyasını temizle
            else:
                df = pd.concat(chunk_list, ignore_index=True)
                log.info(f"Toplam {extracted_count} satır DataFrame'e alındı.")
                df.columns = df.columns.str.lower()
                if 'id' in df.columns: df = df.drop(columns=["id"]) # 'id' sütununu bırak
                log.info(f"Geçici dosyaya yazılıyor: {TEMP_FILE_PATH}")
                df.to_csv(TEMP_FILE_PATH, index=False, header=True, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S.%f')
                log.info("Geçici dosyaya yazma tamamlandı.")

            task_instance.xcom_push(key='extracted_row_count', value=extracted_count)
            log.info(f"XCom'a gönderildi: extracted_row_count = {extracted_count}")
        except pyodbc.Error as db_err:
            log.error(f"MSSQL Hatası (tamamlanmış sipariş çıkarma): {db_err}", exc_info=True)
            task_instance.xcom_push(key='extracted_row_count', value=0) # Hata durumunda 0 gönder
            raise
        except Exception as e:
            log.error(f"Beklenmedik Hata (tamamlanmış sipariş çıkarma): {e}", exc_info=True)
            task_instance.xcom_push(key='extracted_row_count', value=0) # Hata durumunda 0 gönder
            raise
        finally:
            if conn:
                try: conn.close(); log.info("MSSQL bağlantısı kapatıldı.")
                except pyodbc.Error as close_err: log.warning(f"MSSQL kapatma hatası: {close_err}")

    def load_complete_orders_to_postgres(**context):
        """
        Geçici CSV dosyasını okur ve tamamlanmış sipariş verisini PostgreSQL'e UPSERT ile yükler.
        """
        log = logging.getLogger(__name__)
        task_instance = context['ti']
        log.info(f"PostgreSQL'e (orders) yükleme işlemi başladı. Kaynak: {TEMP_FILE_PATH}, Hedef: '{TARGET_TABLE_NAME}'")
        extracted_count = task_instance.xcom_pull(task_ids='extract_complete_orders_data', key='extracted_row_count')
        file_exists = os.path.exists(TEMP_FILE_PATH)

        if not file_exists:
            if extracted_count == 0 or extracted_count is None:
                log.info("Geçici dosya yok ve çıkarılan satır 0/bilinmiyor. Yükleme atlanıyor.")
                task_instance.xcom_push(key='loaded_row_count', value=0)
                return
            else:
                log.error(f"KRİTİK: Dosya {TEMP_FILE_PATH} yok ama extract {extracted_count} raporladı!")
                raise FileNotFoundError(f"Tutarsızlık: {TEMP_FILE_PATH} yok ama satır çıkarıldı.")

        df = None; values = []
        try:
            log.info(f"CSV okunuyor: {TEMP_FILE_PATH}...")
            df = pd.read_csv(TEMP_FILE_PATH, low_memory=False)
            log.info(f"CSV'den {len(df)} satır okundu.")
            if df.empty:
                log.warning("CSV boş. Yükleme atlanıyor.")
                task_instance.xcom_push(key='loaded_row_count', value=0)
                return

            log.info("Veri tipleri PG için hazırlanıyor...")
            # NaN/NaT değerleri None'a dönüştürme
            df = df.fillna(value=pd.NA).replace({pd.NA: None})

            # Tarih/Saat sütunları için dönüşüm ve timezone temizliği
            for col in df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
                 if hasattr(df[col], 'dt') and df[col].dt.tz is not None:
                     df[col] = df[col].dt.tz_convert(None) # UTC'den None'a çevir
                 # Pandas NaT değerlerini Python None'a çevir
                 df[col] = df[col].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)

            # Boolean sütunları için dönüşüm
            for col in df.select_dtypes(include=['boolean']).columns:
                df[col] = df[col].apply(lambda x: bool(x) if pd.notna(x) else None)

            # Tam sayı sütunları için dönüşüm
            for col in df.select_dtypes(include=['Int32', 'Int64']).columns:
                df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) else None)

            # Ondalıklı sayı sütunları için dönüşüm
            for col in df.select_dtypes(include=['float64']).columns:
                df[col] = df[col].apply(lambda x: float(x) if pd.notna(x) else None)


            values = [tuple(x) for x in df.to_numpy()]
            columns = list(df.columns)
            columns_quoted = [f'"{col}"' for col in columns]
            columns_str = ', '.join(columns_quoted)
            log.info(f"{len(values)} kayıt tuple'a çevrildi.")

            conflict_target = ", ".join([f'"{col}"' for col in UPSERT_CONFLICT_COLUMNS])
            log.info(f"UPSERT conflict target: ({conflict_target})")

            update_columns_list = [f'"{col}" = EXCLUDED."{col}"' for col in columns if col not in UPSERT_CONFLICT_COLUMNS]
            if not update_columns_list:
                raise ValueError("UPSERT UPDATE SET boş olamaz.")
            update_set_str = ", ".join(update_columns_list)

            upsert_query = f"""
                INSERT INTO public."{TARGET_TABLE_NAME}" ({columns_str})
                VALUES %s
                ON CONFLICT ({conflict_target}) DO UPDATE SET {update_set_str};
            """
            log.debug(f"UPSERT Sorgusu (kısmi): {upsert_query[:500]}...")

        except pd.errors.EmptyDataError:
            log.warning("Geçici dosya okuma sırasında boş olduğu anlaşıldı. Yükleme atlanıyor.")
            task_instance.xcom_push(key='loaded_row_count', value=0)
            return
        except Exception as prep_err:
            log.error(f"Veri hazırlama/okuma hatası: {prep_err}", exc_info=True)
            raise

        conn_pg = None; cur = None; affected_rows = 0
        try:
            conn_pg = psycopg2.connect(**POSTGRES_CONN_DETAILS)
            conn_pg.autocommit = False
            cur = conn_pg.cursor()
            log.info("PG bağlantısı başarılı.")

            log.info(f"UPSERT execute_values (page size: {PG_PAGE_SIZE})...")
            execute_values(cur, upsert_query, values, page_size=PG_PAGE_SIZE)
            affected_rows = cur.rowcount
            log.info(f"execute_values bitti. Etkilenen: {affected_rows}")
            conn_pg.commit()
            log.info("Transaction commit edildi.")

            if affected_rows == -1: # Bazı sürücüler -1 döndürebilir
                affected_rows = len(values)
                log.warning("rowcount -1, satır sayısı kullanıldı.")

            task_instance.xcom_push(key='loaded_row_count', value=affected_rows)
            log.info(f"XCom'a gönderildi: loaded_row_count = {affected_rows}")

        except psycopg2.Error as db_err:
            log.error(f"PostgreSQL Hatası (UPSERT): {db_err}", exc_info=True)
            if conn_pg: conn_pg.rollback() # Hata durumunda rollback
            raise
        except Exception as e:
            log.error(f"Beklenmedik Hata (PG yükleme): {e}", exc_info=True)
            raise
        finally:
            if cur: cur.close()
            if conn_pg:
                try: conn_pg.close(); log.info("PostgreSQL bağlantısı kapatıldı.")
                except psycopg2.Error as close_err: log.warning(f"PG kapatma hatası: {close_err}")
            _cleanup_temp_file(TEMP_FILE_PATH) # Kendi geçici dosyasını temizle

    # --- AKIŞ 2: OrderCreatedAt NULL OLAN KAYITLAR ---

    def extract_incomplete_orders_data(**context):
        """
        MSSQL'den OrderCreatedAt NULL olan verileri çeker ve ayrı bir geçici CSV dosyasına yazar.
        Satır sayısını XCom'a gönderir.
        """
        log = logging.getLogger(__name__)
        task_instance = context['ti']
        log.info("MSSQL'den OrderCreatedAt NULL olan tamamlanmamış sipariş verisi çekme işlemi başladı.")
        conn_str = _build_mssql_conn_str()
        conn = None
        extracted_count = 0
        try:
            log.info(f"MSSQL'e bağlanılıyor: Sunucu={MSSQL_CONN_PARAMS['server']}...")
            conn = pyodbc.connect(conn_str, timeout=MSSQL_CONN_PARAMS['timeout'], autocommit=True)
            log.info("MSSQL bağlantısı başarılı.")

            # MSSQL'den OrderCreatedAt NULL olan verileri çek
            query = f"SELECT * FROM dbo.{SOURCE_TABLE_NAME} WHERE OrderCreatedAt IS NULL;"
            log.info(f"Sorgu çalıştırılıyor (NULL değerler için): {query}")

            chunk_list = []
            df_iterator = pd.read_sql(query, conn, chunksize=MSSQL_EXTRACT_CHUNKSIZE)

            for i, chunk in enumerate(df_iterator):
                if chunk.empty: continue
                chunk_list.append(chunk)
                extracted_count += len(chunk)
                if (i + 1) % 10 == 0: log.info(f"{i+1} chunk okundu. Toplam: {extracted_count}")

            if not chunk_list:
                log.warning("MSSQL'de OrderCreatedAt NULL olan kayıt bulunamadı.")
                _cleanup_temp_file(TEMP_NULL_FILE_PATH) # Kendi geçici dosyasını temizle
            else:
                df = pd.concat(chunk_list, ignore_index=True)
                log.info(f"Toplam {extracted_count} satır (OrderCreatedAt NULL) DataFrame'e alındı.")
                df.columns = df.columns.str.lower()
                if 'id' in df.columns: df = df.drop(columns=["id"]) # 'id' sütununu bırak
                log.info(f"Geçici dosyaya yazılıyor: {TEMP_NULL_FILE_PATH}")
                df.to_csv(TEMP_NULL_FILE_PATH, index=False, header=True, encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S.%f')
                log.info("Geçici dosyaya yazma tamamlandı.")

            task_instance.xcom_push(key='null_extracted_row_count', value=extracted_count)
            log.info(f"XCom'a gönderildi: null_extracted_row_count = {extracted_count}")

        except pyodbc.Error as db_err:
            log.error(f"MSSQL Hatası (NULL sipariş çıkarma): {db_err}", exc_info=True)
            task_instance.xcom_push(key='null_extracted_row_count', value=0) # Hata durumunda 0 gönder
            raise
        except Exception as e:
            log.error(f"Beklenmedik Hata (NULL sipariş çıkarma): {e}", exc_info=True)
            task_instance.xcom_push(key='null_extracted_row_count', value=0) # Hata durumunda 0 gönder
            raise
        finally:
            if conn:
                try: conn.close(); log.info("MSSQL NULL veri çekme bağlantısı kapatıldı.")
                except pyodbc.Error as close_err: log.warning(f"MSSQL NULL kapatma hatası: {close_err}")

    def load_incomplete_orders_to_postgres(**context):
        """
        Geçici CSV dosyasını okur ve OrderCreatedAt NULL olan veriyi PostgreSQL'deki
        'incomplete_orders' tablosuna TRUNCATE ederek yükler.
        """
        log = logging.getLogger(__name__)
        task_instance = context['ti']
        null_extracted_row_count = task_instance.xcom_pull(task_ids='extract_incomplete_orders_data', key='null_extracted_row_count')
        if null_extracted_row_count is None:
            log.error("XCom'dan null_extracted_row_count çekilemedi. İşleme devam edilemiyor.")
            raise ValueError("XCom değeri eksik.")

        log.info(f"PostgreSQL'e (incomplete_orders) yükleme işlemi başladı. Kaynak: {TEMP_NULL_FILE_PATH}, Hedef: '{TARGET_INCOMPLETE_TABLE_NAME}'")
        log.info(f"Çekilen NULL OrderCreatedAt kayıt sayısı: {null_extracted_row_count}")

        if not os.path.exists(TEMP_NULL_FILE_PATH) or null_extracted_row_count == 0:
            log.info(f"Geçici dosya {TEMP_NULL_FILE_PATH} bulunamadı veya hiç NULL kayıt çekilmedi. Yükleme atlanıyor.")
            task_instance.xcom_push(key='loaded_null_row_count', value=0)
            return

        conn_pg = None; cur = None; affected_rows = 0
        try:
            log.info(f"CSV okunuyor: {TEMP_NULL_FILE_PATH}...")
            df = pd.read_csv(TEMP_NULL_FILE_PATH, low_memory=False)
            log.info(f"CSV'den {len(df)} satır okundu.")

            if df.empty:
                log.warning("Okunan DataFrame boş. Yükleme işlemi atlanıyor.")
                task_instance.xcom_push(key='loaded_null_row_count', value=0)
                return

            log.info("Veri tipleri PG için hazırlanıyor...")
            # NaN/NaT değerleri None'a dönüştürme
            df = df.fillna(value=pd.NA).replace({pd.NA: None})

            # Tarih/Saat sütunları için dönüşüm ve timezone temizliği
            for col in df.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns:
                 if hasattr(df[col], 'dt') and df[col].dt.tz is not None:
                     df[col] = df[col].dt.tz_convert(None)
                 df[col] = df[col].apply(lambda x: x.to_pydatetime() if pd.notna(x) else None)

            # Boolean sütunları için dönüşüm
            for col in df.select_dtypes(include=['boolean']).columns:
                df[col] = df[col].apply(lambda x: bool(x) if pd.notna(x) else None)

            # Tam sayı sütunları için dönüşüm
            for col in df.select_dtypes(include=['Int32', 'Int64']).columns:
                df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) else None)

            # Ondalıklı sayı sütunları için dönüşüm
            for col in df.select_dtypes(include=['float64']).columns:
                df[col] = df[col].apply(lambda x: float(x) if pd.notna(x) else None)

            # loaded_at_timestamp sütununu ekle (eğer DataFrame'de yoksa)
            # Bu sütun PostgreSQL'de DEFAULT CURRENT_TIMESTAMP ile tanımlı olduğu için
            # doğrudan INSERT yaparken belirtmemize gerek yok, PostgreSQL otomatik doldurur.
            # Ancak, eğer manuel olarak bir değer atamak isterseniz, burada ekleyebilirsiniz.
            # df['loaded_at_timestamp'] = datetime.now() # Örnek: Manuel atama

            values = [tuple(x) for x in df.to_numpy()]
            columns = list(df.columns)
            columns_quoted = [f'"{col}"' for col in columns] # Sütun adlarını tırnak içine al
            columns_str = ', '.join(columns_quoted)
            log.info(f"{len(values)} kayıt tuple'a çevrildi.")

        except pd.errors.EmptyDataError:
            log.warning("Geçici dosya okuma sırasında boş olduğu anlaşıldı. Yükleme atlanıyor.")
            task_instance.xcom_push(key='loaded_null_row_count', value=0)
            return
        except Exception as prep_err:
            log.error(f"Veri hazırlama/okuma hatası (incomplete_orders): {prep_err}", exc_info=True)
            raise

        try:
            conn_pg = psycopg2.connect(**POSTGRES_CONN_DETAILS)
            conn_pg.autocommit = False
            cur = conn_pg.cursor()
            log.info("PG bağlantısı (incomplete_orders) başarılı.")

            # TRUNCATE işlemi - Tabloyu boşalt
            log.info(f"'{TARGET_INCOMPLETE_TABLE_NAME}' tablosu TRUNCATE ediliyor...")
            cur.execute(f'TRUNCATE TABLE public."{TARGET_INCOMPLETE_TABLE_NAME}" RESTART IDENTITY;')
            log.info(f"'{TARGET_INCOMPLETE_TABLE_NAME}' tablosu TRUNCATE edildi.")

            # INSERT sorgusu
            insert_query = f"""
                INSERT INTO public."{TARGET_INCOMPLETE_TABLE_NAME}" ({columns_str})
                VALUES %s
            """
            log.info(f"INSERT execute_values (page size: {PG_PAGE_SIZE})...")
            execute_values(cur, insert_query, values, page_size=PG_PAGE_SIZE)
            affected_rows = cur.rowcount
            conn_pg.commit()
            log.info(f"execute_values bitti. Etkilenen: {affected_rows}")

            if affected_rows == -1: # Bazı sürücüler -1 döndürebilir
                affected_rows = len(values)
                log.warning("rowcount -1, satır sayısı kullanıldı.")

            task_instance.xcom_push(key='loaded_null_row_count', value=affected_rows)
            log.info(f"XCom'a gönderildi: loaded_null_row_count = {affected_rows}")

        except psycopg2.Error as db_err:
            log.error(f"PostgreSQL Hatası (incomplete_orders yükleme): {db_err}", exc_info=True)
            if conn_pg: conn_pg.rollback() # Hata durumunda rollback
            raise
        except Exception as e:
            log.error(f"Beklenmedik Hata (PG incomplete_orders yükleme): {e}", exc_info=True)
            raise
        finally:
            if cur: cur.close()
            if conn_pg:
                try: conn_pg.close(); log.info("PostgreSQL (incomplete_orders) bağlantısı kapatıldı.")
                except psycopg2.Error as close_err: log.warning(f"PG incomplete_orders kapatma hatası: {close_err}")
            _cleanup_temp_file(TEMP_NULL_FILE_PATH) # Kendi geçici dosyasını temizle


    def validate_data_counts(**context):
        """
        MSSQL'den çıkarılan ve PostgreSQL'deki gerçek satır sayılarını karşılaştırır.
        Bu doğrulama sadece ana 'orders' tablosu içindir.
        """
        log = logging.getLogger(__name__)
        logical_date_str = context['ds']
        task_instance = context['ti']
        log.info(f"Veri sayısı doğrulaması başlatıldı: Tarih = {logical_date_str}")

        extracted_count = task_instance.xcom_pull(task_ids='extract_complete_orders_data', key='extracted_row_count')
        if extracted_count is None:
            extracted_count = 0
            log.warning("XCom'dan 'extracted_count' alınamadı, 0 varsayılıyor.")
        log.info(f"Doğrulama: MSSQL'den çıkarılan (XCom) tamamlanmış sipariş: {extracted_count}")

        if extracted_count == 0:
            log.info("Doğrulama: Çıkarılan tamamlanmış sipariş satırı 0. PG sayımı atlanıyor.")
            loaded_affected_count = task_instance.xcom_pull(task_ids='load_complete_orders_to_postgres', key='loaded_row_count')
            if loaded_affected_count == 0 or loaded_affected_count is None:
                log.info("Doğrulama Başarılı: İki sayı da 0.")
            else:
                log.warning(f"Doğrulama Uyarısı: Çıkarılan 0, ama yüklenen/etkilenen {loaded_affected_count}.")
            return

        conn_pg = None; cur_pg = None; pg_actual_count = -1
        try:
            conn_pg = psycopg2.connect(**POSTGRES_CONN_DETAILS)
            cur_pg = conn_pg.cursor()
            log.info("PG doğrulama bağlantısı başarılı.")
            # Sadece OrderCreatedAt NULL olmayan kayıtları say
            query_pg = f'SELECT COUNT(*) FROM public."{TARGET_TABLE_NAME}" WHERE CAST("ordercreatedat" AS DATE) = %s'
            log.info(f"PG COUNT sorgusu çalıştırılıyor (tamamlanmış siparişler için)...")
            cur_pg.execute(query_pg, (logical_date_str,))
            result = cur_pg.fetchone()
            if result:
                pg_actual_count = result[0]
                log.info(f"Doğrulama: PG gerçek satır sayısı (tamamlanmış siparişler): {pg_actual_count}")
            else:
                pg_actual_count = 0
                log.warning("PG COUNT sorgusu sonuç döndürmedi.")
        except Exception as e:
            log.error(f"PG Hatası (doğrulama sayımı): {e}", exc_info=True)
            return
        finally:
            if cur_pg: cur_pg.close()
            if conn_pg:
                try: conn_pg.close(); log.info("PostgreSQL doğrulama bağlantısı kapatıldı.")
                except psycopg2.Error as close_err: log.warning(f"PG doğrulama kapatma hatası: {close_err}")

        log.info(f"Son Karşılaştırma: MSSQL Çıkarılan (tamamlanmış) = {extracted_count}, PostgreSQL Gerçek Sayım (tamamlanmış) = {pg_actual_count}")
        if extracted_count == pg_actual_count:
            log.info(f"Doğrulama Başarılı: Satır sayıları ({extracted_count}) {logical_date_str} için eşleşiyor.")
        else:
            log.warning(f"Doğrulama UYARISI: Satır sayıları EŞLEŞMİYOR! MSSQL={extracted_count}, PG={pg_actual_count} (Tarih: {logical_date_str})")


    # --- DAG GÖREV TANIMLARI VE BAĞIMLILIKLARI ---

    # Akış 1: Tamamlanmış Siparişler
    extract_complete_orders_task = PythonOperator(
        task_id='extract_complete_orders_data',
        python_callable=extract_complete_orders_data,
        doc_md="MSSQL kaynak tablosundan günlük tamamlanmış sipariş verilerini çeker (OrderCreatedAt IS NOT NULL).",
    )

    load_complete_orders_task = PythonOperator(
        task_id='load_complete_orders_to_postgres',
        python_callable=load_complete_orders_to_postgres,
        doc_md="Çekilen tamamlanmış sipariş verilerini PostgreSQL'deki 'orders' tablosuna UPSERT ile yükler.",
    )

    validate_complete_orders_task = PythonOperator(
        task_id='validate_complete_orders_data_transfer',
        python_callable=validate_data_counts, # Mevcut validate fonksiyonunu kullan
        doc_md="MSSQL'den çekilen tamamlanmış sipariş ile PostgreSQL'e yüklenen veriyi doğrular.",
    )

    # Akış 2: Tamamlanmamış Siparişler (OrderCreatedAt NULL)
    extract_incomplete_orders_task = PythonOperator(
        task_id='extract_incomplete_orders_data',
        python_callable=extract_incomplete_orders_data,
        doc_md="MSSQL kaynak tablosundan OrderCreatedAt NULL olan tamamlanmamış sipariş verilerini çeker.",
    )

    load_incomplete_orders_task = PythonOperator(
        task_id='load_incomplete_orders_to_postgres',
        python_callable=load_incomplete_orders_to_postgres,
        doc_md="Çekilen tamamlanmamış sipariş verilerini PostgreSQL'deki 'incomplete_orders' tablosuna TRUNCATE ederek yükler.",
    )

    # Görev Bağımlılıkları
    # Akış 1
    extract_complete_orders_task >> load_complete_orders_task >> validate_complete_orders_task

    # Akış 2
    extract_incomplete_orders_task >> load_incomplete_orders_task

    # İsteğe bağlı: Eğer tüm ETL sürecinin bitmesini bekleyen bir sonraki görev varsa,
    # bu iki akışın da tamamlanmasını bekleyebiliriz.
    # Örneğin: [validate_complete_orders_task, load_incomplete_orders_task] >> next_downstream_task