## CSV'den MSSQL'e Staging ve Merge ile Yükleme

# Bu betik, lokaldeki bir CSV dosyasından (orders.csv) veriyi okur,
# gerekli veri tipi dönüşümlerini ve temizliğini yapar, ardından veriyi
# bir ara MSSQL tablosuna (orders_staging) yükler. Son olarak, staging
# tablosundaki veriyi hedef MSSQL tablosuna ('orders') 'MERGE' ifadesi
# kullanarak aktarır. Bu yöntem, hedef tabloda belirtilen iş anahtarlarına
# göre veri tekrarını önler (UPSERT mantığı).
#
# Yapılandırma ayarları (veritabanı bağlantıları, dosya yolları, tablo adları,
# benzersiz anahtarlar vb.) betiğin bulunduğu dizindeki '.env' dosyasından veya
# ortam değişkenlerinden okunur.
#
# Not: Bu betik genellikle bir Airflow DAG'i içinden veya Docker konteyneri içinde
# çalıştırılmak üzere tasarlanmıştır, bu nedenle dosya yolları ve sunucu isimleri
# Docker ortamına göre ayarlanır.

import logging
import os
import sys
from datetime import date, datetime
from decimal import Decimal
from urllib.parse import quote_plus

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import (create_engine, exc as sqlalchemy_exc, text,
                         types as sqlalchemy_types)

# --- Yapılandırma ve Loglama Ayarları ---

def setup_logging():
    """
    Konsola loglama için temel yapılandırmayı ayarlar.

    Returns:
        logging.Logger: Yapılandırılmış logger nesnesi.
    """
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger = logging.getLogger(__name__)
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(log_formatter)
        logger.addHandler(stdout_handler)
        logger.propagate = False
    return logger

def load_configuration() -> dict:
    """
    Yapılandırma ayarlarını ortam değişkenlerinden veya .env dosyasından yükler.

    .env dosyasının, bu betik dosyasının bulunduğu dizinde olduğunu varsayar.
    Gerekli tüm yapılandırma anahtarlarının varlığını kontrol eder.
    Benzersiz anahtar sütunlarını bir listeye çevirir.

    Returns:
        dict: Yapılandırma ayarlarını içeren bir sözlük.

    Raises:
        ValueError: Gerekli bir ortam değişkeni eksik veya boşsa.
    """
    logger = logging.getLogger(__name__)
    dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
    logger.debug(f".env dosyası aranıyor: {dotenv_path}")
    if os.path.exists(dotenv_path):
        loaded = load_dotenv(dotenv_path=dotenv_path, override=True, verbose=True)
        if loaded:
            logger.info(f".env dosyası başarıyla yüklendi: {dotenv_path}")
        else:
            logger.warning(f".env dosyası bulundu ancak yüklenemedi veya boş: {dotenv_path}")
    else:
        logger.warning(f".env dosyası bulunamadı: {dotenv_path}. Ortam değişkenleri kullanılacak.")

    # orders.csv görselinize göre 'OrderID' benzersiz anahtar olarak belirlendi
    default_unique_keys = "OrderID"

    config = {
        "csv_file_path": os.getenv("CSV_FILE_PATH"),
        "db_driver": os.getenv("DB_DRIVER", "{ODBC Driver 18 for SQL Server}"),
        "db_server": os.getenv("DB_SERVER"),
        "db_database": os.getenv("DB_DATABASE"),
        "db_username": os.getenv("DB_USERNAME"),
        "db_password": os.getenv("DB_PASSWORD"),
        "staging_table_name": os.getenv("STAGING_TABLE_NAME", "orders_staging"),
        "target_table_name": os.getenv("TARGET_TABLE_NAME", "orders"),
        "staging_load_chunksize": int(os.getenv("STAGING_LOAD_CHUNKSIZE", "5000")),
        "unique_key_columns_str": os.getenv("UNIQUE_KEY_COLUMNS", default_unique_keys),
    }

    required_keys = ["csv_file_path", "db_server", "db_database", "db_username", "db_password", "unique_key_columns_str"]
    missing_keys_list = [key for key in required_keys if not config[key]]
    if missing_keys_list:
        error_msg = f"Yapılandırma hatası: Şu ortam değişkenleri eksik veya boş: {', '.join(missing_keys_list)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    config["unique_key_columns"] = [col.strip() for col in config["unique_key_columns_str"].split(',') if col.strip()]
    if not config["unique_key_columns"]:
        error_msg = "Yapılandırma hatası: UNIQUE_KEY_COLUMNS boş olamaz."
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info("Yapılandırma başarıyla yüklendi ve doğrulandı.")
    logger.debug(f"Yüklenen yapılandırma (şifre hariç): {{'csv_file_path': '{config['csv_file_path']}', 'db_driver': '{config['db_driver']}', 'db_server': '{config['db_server']}', 'db_database': '{config['db_database']}', 'db_username': '{config['db_username']}', 'staging_table_name': '{config['staging_table_name']}', 'target_table_name': '{config['target_table_name']}', 'staging_load_chunksize': {config['staging_load_chunksize']}, 'unique_key_columns': {config['unique_key_columns']}}}")
    return config

# --- Veritabanı Fonksiyonları ---

def create_db_engine(config: dict):
    """
    Verilen yapılandırma ile bir SQLAlchemy veritabanı motoru (engine) oluşturur.

    ODBC sürücüsü ve diğer bağlantı parametrelerini kullanarak MSSQL için
    bir bağlantı dizesi hazırlar. fast_executemany=True performans için etkindir.

    Args:
        config (dict): Veritabanı bağlantı bilgilerini içeren yapılandırma sözlüğü.
                       ('db_driver', 'db_server', 'db_database', 'db_username', 'db_password' anahtarlarını içermelidir).

    Returns:
        sqlalchemy.engine.Engine: Oluşturulan SQLAlchemy motor nesnesi.

    Raises:
        Exception: Engine oluşturma sırasında herhangi bir hata oluşursa.
    """
    logger = logging.getLogger(__name__)
    try:
        log_server = config.get('db_server', 'Bilinmeyen Sunucu')
        log_db = config.get('db_database', 'Bilinmeyen DB')
        logger.info(f"Veritabanı için engine oluşturulmaya çalışılıyor: DB='{log_db}', Sunucu='{log_server}'")

        params = quote_plus(
            f"DRIVER={{{config['db_driver']}}};"
            f"SERVER={config['db_server']};"
            f"DATABASE={config['db_database']};"
            f"UID={config['db_username']};"
            f"PWD={config['db_password']};"
            f"Encrypt=no;"
            f"TrustServerCertificate=yes;"
        )
        connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
        logger.debug(f"Oluşturulan bağlantı dizesi (parametreler gizlenmiş): mssql+pyodbc:///?odbc_connect=...")

        engine = create_engine(connection_string, fast_executemany=True, echo=False)
        logger.info("SQLAlchemy engine başarıyla oluşturuldu.")
        return engine
    except Exception as e:
        logger.error(f"SQLAlchemy engine oluşturulamadı: {e}", exc_info=True)
        raise

def test_db_connection(engine) -> bool:
    """
    Verilen SQLAlchemy motoru kullanarak veritabanı bağlantısını test eder.

    Basit bir 'SELECT 1' sorgusu çalıştırarak bağlantının kurulup kurulmadığını kontrol eder.

    Args:
        engine (sqlalchemy.engine.Engine): Test edilecek SQLAlchemy motor nesnesi.

    Returns:
        bool: Bağlantı başarılıysa True, değilse False.
    """
    logger = logging.getLogger(__name__)
    try:
        logger.info("Veritabanı bağlantısı test ediliyor...")
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.info("Veritabanı bağlantısı başarılı.")
        return True
    except sqlalchemy_exc.SQLAlchemyError as e:
        logger.error(f"Veritabanı bağlantısı başarısız oldu (SQLAlchemy Hatası): {e}", exc_info=True)
        if hasattr(e, 'orig') and e.orig:
            logger.error(f"Orijinal DBAPI Hatası: {e.orig}")
        return False
    except Exception as e:
        logger.error(f"Bağlantı testi sırasında beklenmedik bir hata oluştu: {e}", exc_info=True)
        return False

# --- Veri İşleme Fonksiyonları ---

def read_and_transform_csv(csv_path: str, expected_columns: list[str], config: dict) -> pd.DataFrame:
    """
    Belirtilen CSV dosyasını okur, sütunları doğrular, veri tiplerini dönüştürür
    ve kritik sütunlardaki boş değerleri temizler.

    Args:
        csv_path (str): Okunacak CSV dosyasının yolu.
        expected_columns (list[str]): DataFrame'de bulunması beklenen sütunların listesi
                                     (hedef tablo sırasına göre olmalı).
        config (dict): Yapılandırma sözlüğü ('unique_key_columns' anahtarını içerir).

    Returns:
        pd.DataFrame: İşlenmiş ve temizlenmiş verileri içeren Pandas DataFrame.
                      Eğer CSV boşsa veya okuma hatası olursa boş bir DataFrame dönebilir.

    Raises:
        FileNotFoundError: CSV dosyası belirtilen yolda bulunamazsa.
        pd.errors.ParserError: CSV dosyası ayrıştırılamazsa.
        KeyError: expected_columns içinde DataFrame'de olmayan bir sütun varsa.
        ValueError: Kritik sütunlar (benzersiz anahtar vb.) eksikse.
        TypeError: Veri tipi dönüşümü sırasında beklenmedik bir hata oluşursa.
        Exception: Diğer beklenmedik okuma/işleme hataları için.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"CSV okuma işlemi başlıyor: {csv_path}")

    try:
        df = pd.read_csv(csv_path, header=0, quotechar='"', sep=",", low_memory=False)
        logger.info(f"CSV başarıyla okundu. Başlangıç boyut: {df.shape}")
    except FileNotFoundError:
        logger.error(f"HATA: CSV dosyası bulunamadı: {csv_path}")
        raise
    except pd.errors.EmptyDataError:
        logger.warning(f"Uyarı: CSV dosyası boş: {csv_path}. Boş DataFrame döndürülüyor.")
        return pd.DataFrame(columns=expected_columns)
    except pd.errors.ParserError as parse_err:
        logger.error(f"HATA: CSV dosyası ayrıştırılamadı: {csv_path} - Hata: {parse_err}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"HATA: CSV okuma sırasında genel hata: {csv_path} - Hata: {e}", exc_info=True)
        raise

    logger.info(f"Beklenen sütunların varlığı kontrol ediliyor...")
    missing_cols = [col for col in expected_columns if col not in df.columns]
    if missing_cols:
        logger.warning(f"Uyarı: CSV'de şu sütunlar eksik, boş olarak eklenecek: {missing_cols}")
        for col in missing_cols:
            df[col] = pd.NA

    try:
        df = df[expected_columns]
        logger.info("DataFrame sütunları beklenen sıraya göre hizalandı.")
    except KeyError as e:
        logger.error(f"HATA: DataFrame sütunları hizalanamadı. Beklenmeyen sütun sorunu: {e}", exc_info=True)
        logger.error(f"DataFrame'deki mevcut sütunlar: {df.columns.tolist()}")
        logger.error(f"Beklenen sütunlar: {expected_columns}")
        raise KeyError(f"DataFrame sütun hizalama hatası. CSV başlıklarını kontrol edin. Hata: {e}")

    # orders.csv görselinize göre veri tipi listeleri güncellendi
    bigint_cols = ['OrderID', 'UserID'] # OrderID ve UserID büyük tam sayı
    decimal_cols = ['Amount'] # Amount ondalıklı sayı
    datetime_cols = ['AddedToCartAt', 'OrderCreatedAt'] # Tarih sütunları
    bit_cols = ['IsDelivered'] # Boolean (True/False)
    int_cols = [] # Şu an bu tabloda başka int sütun yok

    logger.info("Veri tipi dönüşümleri başlıyor...")
    conversion_errors = {}

    def log_coercion(col_name, initial_nulls, series_after_conversion):
        new_nulls = series_after_conversion.isnull().sum()
        if new_nulls > initial_nulls:
            coerced_count = new_nulls - initial_nulls
            conversion_errors[col_name] = coerced_count
            logger.warning(f"Sütun '{col_name}': {coerced_count} değer tipi dönüştürülemedi ve Null/NA olarak ayarlandı.")

    try:
        for col in bigint_cols:
            if col in df.columns:
                initial_nulls = df[col].isnull().sum()
                converted_series = pd.to_numeric(df[col], errors='coerce')
                log_coercion(col, initial_nulls, converted_series)
                df[col] = converted_series.astype('Int64')

        for col in decimal_cols:
            if col in df.columns:
                initial_nulls = df[col].isnull().sum()
                converted_series = pd.to_numeric(df[col], errors='coerce')
                log_coercion(col, initial_nulls, converted_series)
                df[col] = converted_series

        for col in datetime_cols:
            if col in df.columns:
                initial_nulls = df[col].isnull().sum()
                # Önceki kod: converted_series = pd.to_datetime(df[col], format='%m/%d/%Y %H:%M', errors='coerce')

                # DENEME 1: Eğer format M/D/YYYY H:MM ise (tek haneli ay/gün için)
                # Özellikle tek haneli ay/gün için 'infer_datetime_format=True' veya
                # çoklu format listesi kullanmak daha güvenli olabilir.
                # Ya da aşağıdaki gibi önce %m/%d/%Y %H:%M'i deneyip olmazsa başka formatları denemek.

                try:
                    converted_series = pd.to_datetime(df[col], format='%m/%d/%Y %H:%M', errors='raise')
                except ValueError:
                    # Eğer yukarısı hata verirse, otomatik algılamayı deneyin.
                    # Bu, M/D/YYYY gibi tek haneli ay/gün durumlarını daha iyi işleyebilir.
                    logger.warning(f"Sütun '{col}': Belirtilen format ('%m/%d/%Y %H:%M') ile dönüştürülemedi, otomatik algılama denenecek.")
                    converted_series = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
                    if converted_series.isnull().all() and not df[col].isnull().all():
                        logger.error(f"Sütun '{col}': Otomatik algılama ile de tüm değerler dönüştürülemedi. Veri formatını kontrol edin.")
                except Exception as e:
                    logger.error(f"Sütun '{col}': Tarih dönüşümünde beklenmedik hata: {e}", exc_info=True)
                    converted_series = pd.Series([pd.NaT] * len(df), index=df.index) # Tümünü NaT yap

                log_coercion(col, initial_nulls, converted_series)
                df[col] = converted_series

        for col in bit_cols:
            if col in df.columns:
                initial_nulls = df[col].isnull().sum()
                if df[col].dtype == 'object': # CSV'den string olarak gelebilir
                    # 'False', 'True' gibi stringleri boolean'a çevir
                    df[col] = df[col].astype(str).str.upper().map({'TRUE': True, 'FALSE': False}).fillna(pd.NA)

                try:
                    converted_series = df[col].astype('boolean')
                except (TypeError, ValueError):
                    # Fallback for unexpected values
                    def to_bool_safe(x):
                        if pd.isna(x): return pd.NA
                        if isinstance(x, bool): return x
                        if str(x).upper() in ['TRUE', '1', 'YES', 'T']: return True
                        if str(x).upper() in ['FALSE', '0', 'NO', 'F']: return False
                        return pd.NA
                    converted_series = df[col].apply(to_bool_safe).astype('boolean')

                log_coercion(col, initial_nulls, converted_series)
                df[col] = converted_series

        for col in int_cols: # Şu anda boş ama gelecekte eklenebilir
            if col in df.columns:
                initial_nulls = df[col].isnull().sum()
                converted_series = pd.to_numeric(df[col], errors='coerce')
                log_coercion(col, initial_nulls, converted_series)
                df[col] = converted_series.astype('Int32')

    except (TypeError, ValueError) as type_convert_error:
        logger.error(f"HATA: Veri tipi dönüşümü sırasında hata oluştu: {type_convert_error}", exc_info=True)
        raise

    if conversion_errors:
        logger.warning(f"Tip dönüşümü özeti: Şu sütunlarda değerler Null/NA olarak zorlandı: {conversion_errors}")
    else:
        logger.info("Tip dönüşümleri önemli bir zorlama olmadan tamamlandı.")

    # Kritik sütunlar (benzersiz anahtarlar ve önemli diğer sütunlar)
    # OrderID ve OrderCreatedAt'ın boş olmaması gerektiğini varsayıyorum.
    critical_not_null_cols = config.get("unique_key_columns", [])
    critical_not_null_cols = sorted(list(set(critical_not_null_cols)))

    cols_to_check = [col for col in critical_not_null_cols if col in df.columns]

    if not cols_to_check:
        logger.warning("Uyarı: Null kontrolü için kritik sütun tanımlanmamış veya DataFrame'de bulunamadı.")
    else:
        logger.info(f"MERGE/yükleme için kritik sütunlarda null kontrolü yapılıyor: {cols_to_check}")
        initial_rows = df.shape[0]

        null_rows_mask = df[cols_to_check].isnull().any(axis=1)
        rows_with_nulls = null_rows_mask.sum()

        if rows_with_nulls > 0:
            logger.warning(f"Uyarı: Kritik sütunlarda ({cols_to_check}) null değer içeren {rows_with_nulls} satır bulundu ve silinecek.")

            df = df.dropna(subset=cols_to_check)
            final_rows = df.shape[0]
            rows_dropped = initial_rows - final_rows
            logger.info(f"{rows_dropped} satır kritik null değerler nedeniyle silindi.")
        else:
            logger.info("Kritik sütun kontrollerine göre silinen satır yok.")
        logger.info(f"Null kontrolleri sonrası DataFrame boyutu: {df.shape}")

    try:
        mem_usage = df.memory_usage(deep=True).sum() / (1024*1024)
        logger.info(f"İşlenen DataFrame'in tahmini bellek kullanımı: {mem_usage:.2f} MB")
    except Exception:
        pass

    return df

# --- Staging ve Merge Fonksiyonu ---

def load_via_staging_and_merge(df: pd.DataFrame, engine, config: dict):
    """
    Veriyi bir staging tablosu aracılığıyla yükler ve ardından MERGE ifadesi
    kullanarak hedef tabloya aktarır (UPSERT mantığı).

    Args:
        df (pd.DataFrame): Yüklenecek işlenmiş veriyi içeren DataFrame.
        engine (sqlalchemy.engine.Engine): Hedef MSSQL veritabanına bağlantı için SQLAlchemy motoru.
        config (dict): Yapılandırma sözlüğü ('staging_table_name', 'target_table_name',
                       'unique_key_columns', 'staging_load_chunksize' anahtarlarını içerir).

    Raises:
        ValueError: Benzersiz anahtar sütunları DataFrame'de bulunamazsa.
        sqlalchemy_exc.SQLAlchemyError: Staging veya MERGE sırasında veritabanı hatası oluşursa.
        Exception: Diğer beklenmedek hatalar için.
    """
    logger = logging.getLogger(__name__)
    staging_table = config['staging_table_name']
    target_table = config['target_table_name']
    unique_key_columns = config['unique_key_columns']
    chunk_size = config['staging_load_chunksize']

    if df.empty:
        logger.warning("Girdi DataFrame boş. Staging yüklemesi ve MERGE işlemi atlanıyor.")
        return

    record_count = len(df)
    logger.info(f"Yükleme süreci başlıyor: {record_count} satır -> Staging ('{staging_table}') -> Hedef ('{target_table}') [MERGE]")

    if not all(key in df.columns for key in unique_key_columns):
        missing_keys = [key for key in unique_key_columns if key not in df.columns]
        error_msg = f"HATA: Benzersiz anahtar sütunları {missing_keys} DataFrame'de bulunamadı. MERGE yapılamaz."
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.debug("MERGE için SQL ifadeleri hazırlanıyor...")
    all_columns_quoted = [f"[{col}]" for col in df.columns]
    column_list_sql = ", ".join(all_columns_quoted)

    on_condition = " AND ".join([f"t.[{key}] = s.[{key}]" for key in unique_key_columns])

    update_set_list = []
    for col in df.columns:
        # OrderCreatedAt gibi tarihlerin her zaman güncellenmesini istemeyebilirsiniz.
        # Eğer bu sütunların yalnızca ilk kez eklendiğinde değeri almasını,
        # sonrasında ise güncellenmemesini istiyorsanız, bu listeye ekleyebilirsiniz.
        # Şimdilik OrderCreatedAt'ı hariç tuttum.
        if col not in unique_key_columns and col != 'OrderCreatedAt':
            update_set_list.append(f"t.[{col}] = s.[{col}]")

    if not update_set_list:
        logger.warning("Uyarı: UPDATE SET için güncellenecek sütun bulunamadı (belki tüm sütunlar anahtar?).")
        update_set_sql = ""
    else:
        update_set_sql = ",\n                     ".join(update_set_list)

    insert_columns_sql = column_list_sql
    insert_values_sql = ", ".join([f"s.[{col}]" for col in df.columns])

    sql_merge = f"""
    MERGE dbo.{target_table} AS t
    USING dbo.{staging_table} AS s
    ON ({on_condition})
    WHEN MATCHED THEN
        UPDATE SET
                     {update_set_sql}
    WHEN NOT MATCHED BY TARGET THEN
        INSERT ({insert_columns_sql})
        VALUES ({insert_values_sql});
    """
    logger.debug(f"Oluşturulan MERGE SQL:\n{sql_merge}")

    try:
        with engine.connect() as connection:
            with connection.begin():
                logger.info(f"Staging tablosu ('{staging_table}') boşaltılıyor (TRUNCATE)...")
                connection.execute(text(f"TRUNCATE TABLE dbo.{staging_table}"))
                logger.info("Staging tablosu başarıyla boşaltıldı.")

                logger.info(f"{record_count} satır staging tablosuna ('{staging_table}') yükleniyor (chunksize: {chunk_size})...")
                df.to_sql(
                    name=staging_table,
                    con=connection,
                    schema='dbo',
                    if_exists='append',
                    index=False,
                    chunksize=chunk_size,
                    method=None
                )
                logger.info("Veri staging tablosuna başarıyla yüklendi.")

                logger.info(f"MERGE ifadesi çalıştırılıyor ('{staging_table}' -> '{target_table}')...")
                result = connection.execute(text(sql_merge))

            logger.info(f"MERGE işlemi ve transaction başarıyla tamamlandı.")

    except sqlalchemy_exc.SQLAlchemyError as e:
        logger.error(f"Staging/MERGE sürecinde veritabanı hatası oluştu: {e}", exc_info=True)

        if hasattr(e, 'orig') and e.orig: logger.error(f"Orijinal DBAPI Hatası: {e.orig}")

        if isinstance(e, sqlalchemy_exc.DBAPIError) and ("schema" in str(e).lower() or "column" in str(e).lower()):
            logger.error("Potansiyel ŞEMA UYUŞMAZLIĞI! DataFrame sütun/tiplerini staging/hedef tablo ile karşılaştırın.")
            logger.error(f"DataFrame Sütunları: {df.columns.tolist()}")
            logger.error(f"DataFrame Tipleri:\n{df.dtypes}")
        raise
    except Exception as e:
        logger.error(f"Staging/MERGE sırasında beklenmedik bir hata oluştu: {e}", exc_info=True)
        raise



def main():
    """Ana ETL sürecini yönetir."""
    logger = setup_logging()
    logger.info("CSV -> MSSQL ETL Betiği Başlatıldı (Staging + MERGE).")

    config = None
    engine = None

    try:

        config = load_configuration()

        engine = create_db_engine(config)
        if not test_db_connection(engine):
            raise sqlalchemy_exc.SQLAlchemyError("Veritabanı bağlantı testi başarısız oldu. Betik durduruluyor.")

        # orders.csv görselinize göre beklenen sütunlar güncellendi
        expected_columns = [
            'OrderID', 'UserID', 'AddedToCartAt', 'OrderCreatedAt', 'Amount', 'Product', 'IsDelivered'
        ]
        logger.debug(f"Hedef tablo için beklenen sütunlar tanımlandı ({len(expected_columns)} adet).")

        df_transformed = read_and_transform_csv(config["csv_file_path"], expected_columns, config)

        load_via_staging_and_merge(df_transformed, engine, config)

        logger.info("ETL Betiği Başarıyla Tamamlandı.")

    except FileNotFoundError:
        logger.error(f"Kritik Hata: Girdi CSV dosyası bulunamadı. Betik sonlandırılıyor.")
        sys.exit(1)
    except ValueError as ve:
        logger.error(f"Kritik Hata (ValueError): {ve}. Betik sonlandırılıyor.")
        sys.exit(1)
    except (pd.errors.EmptyDataError, pd.errors.ParserError) as pd_read_error:
        logger.error(f"Kritik Hata (Pandas Okuma/Ayrıştırma): {pd_read_error}. Betik sonlandırılıyor.")
        sys.exit(1)
    except (KeyError, TypeError) as transform_error:
        logger.error(f"Kritik Hata (Veri Dönüşümü): {transform_error}. Betik sonlandırılıyor.", exc_info=True)
        sys.exit(1)
    except sqlalchemy_exc.SQLAlchemyError as db_error:
        logger.error(f"Kritik Hata (Veritabanı): {db_error}. Betik sonlandırılıyor.", exc_info=False)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Beklenmedik Kritik Hata: {e}. Betik sonlandırılıyor.", exc_info=True)
        sys.exit(1)

    finally:
        if engine:
            try:
                logger.info("Veritabanı motoru kaynakları serbest bırakılıyor (dispose)...")
                engine.dispose()
                logger.info("Veritabanı motoru başarıyla dispose edildi.")
            except Exception as e_dispose:
                logger.warning(f"Veritabanı motorunu dispose ederken hata oluştu: {e_dispose}")
        logger.info("ETL Betik Çalışması Tamamlandı (finally bloğu).")

if __name__ == "__main__":
    main()