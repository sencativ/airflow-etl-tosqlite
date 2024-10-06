from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import os
import json
import requests

default_args = {"retries": 3, "retry_delay": 30}


@dag(
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        "jenis_ekstensi": Param(
            "csv", description="Isi dengan salah satu ekstensi csv/api/json"
        ),
        "url": Param("https://example.com/data.csv", description="Isi dengan URL Data"),
        "nama_tabel": Param(
            "default_table", description="Isi dengan nama tabel yang diinginkan"
        ),
    },
)
def extract_load_sqlite_dag():

    @task
    def start_task():
        pass

    # Branch task untuk memilih ekstensi yang digunakan, dan returnnya akan digunakan sebagai xcom untuk memilih task selanjutnya dan task load to sql
    @task.branch
    def choose_extract_task(**kwargs):
        # Mengambil parameter untuk menentukan tipe jenis task yang akan
        # Dikerjakan berdasarkan ekstensinya.
        jenis_ekstensi = kwargs["dag_run"].conf.get("jenis_ekstensi")
        if jenis_ekstensi == "api":
            return "extract_from_api"
        elif jenis_ekstensi == "json":
            return "extract_from_json"
        elif jenis_ekstensi == "csv":
            return "extract_from_csv"
        else:
            raise ValueError("Jenis ekstensi tersebut tidak didukung.")

    # Ini bagian untuk ngehandle ekstrak dari API
    @task(task_id="extract_from_api")
    def extract_from_api(**kwargs):
        # Mengambil parameter untuk memproses ekstrak.
        conf = kwargs["dag_run"].conf
        url = conf.get("url")
        staging_filename = "staging_data_dari_API.parquet"

        # Cek apakah params url kosong
        if not url:
            raise ValueError("Tolong isi bagian URL.")

        # Memastikan kalo response status api nya success
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
        else:
            raise Exception(f"Gagal download data dari {url}")

        # Konversi data dari JSON ke DataFrame
        if isinstance(data, dict):
            data = [data]

        # normalisasi data
        df = pd.json_normalize(data)

        # Ubah tipe data dict/json ke string agar bisa diload ke sqlite
        for col in df.columns:
            df[col] = df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
            )

        # Simpan ke Parquet di staging area
        staging_path = f"data/{staging_filename}"
        df.to_parquet(staging_path, index=False)

        # Karena nama parquet tiap ekstrak berbeda, jadi di return agar jadi xcom untuk task task load sqlite
        return staging_path

    # Handle ekstrak dari json
    @task(task_id="extract_from_json")
    def extract_from_json(**kwargs):

        conf = kwargs["dag_run"].conf
        url = conf.get("url")
        staging_filename = "staging_data_dari_Json.parquet"

        # Cek apakah params url kosong
        if not url:
            raise ValueError("Tolong isi bagian URL.")

        # Cek koneksi terhadap url
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
        else:
            raise Exception(f"Gagal download data dari {url}")

        # Konversi ke DataFrame
        if isinstance(data, dict):
            data = [data]

        # normalisasi data
        df = pd.json_normalize(data)

        # Ubah tipe data dict/json ke string agar bisa diload ke sqlite
        for col in df.columns:
            df[col] = df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
            )

        # Simpan ke Parquet di staging area
        staging_path = f"data/{staging_filename}"
        df.to_parquet(staging_path, index=False)

        # Karena nama parquet tiap ekstrak berbeda, jadi di return agar jadi xcom untuk task load sqlite
        return staging_path

    # Menangani case jika ekstensinya csv
    @task(task_id="extract_from_csv")
    def extract_from_csv(**kwargs):
        # Mengambil parameter
        conf = kwargs["dag_run"].conf
        url = conf.get("url")
        staging_filename = "staging_data_dari_CSV.parquet"

        # Cek apakah params url kosong
        if not url:
            raise ValueError("Tolong isi bagian URL")

        # Cek koneksi terhadap url
        response = requests.get(url)
        if response.status_code == 200:
            data_content = response.content.decode("utf-8")
        else:
            raise Exception(f"Gagal download data dari {url}")

        # menggunakam import string io disini untuk best practice
        from io import StringIO

        # Menggunakan string io untuk membaca dari memori
        df = pd.read_csv(StringIO(data_content))

        # Ubah tipe data dict/json ke string agar bisa diload ke sqlite
        for col in df.columns:
            df[col] = df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
            )

        # Simpan ke Parquet di staging area
        staging_path = f"data/{staging_filename}"
        df.to_parquet(staging_path, index=False)

        # Karena nama parquet tiap ekstrak berbeda, jadi di return agar jadi xcom untuk task load sqlite
        return staging_path

    # Load hasil ekstrak ke sqlite
    @task(trigger_rule=TriggerRule.ONE_SUCCESS)
    def load_to_sqlite(**kwargs):
        # Mengambil parameter nama tabel
        conf = kwargs["dag_run"].conf
        # Menggunakan fungsi get jika nama tabel tidak diisi/tidak ada maka akan ditulis default table
        nama_tabel = conf.get("nama_tabel", "default_table")

        # Mengambil informasi task instances
        ti = kwargs["ti"]

        # Karena hanya satu task yang dipilih, jadi tarik xcom return value dari task id yang dipilih oleh branch
        previous_task_id = ti.xcom_pull(
            task_ids="choose_extract_task", key="return_value"
        )

        # Tarik staging path dari tugas ekstraksi yang dipilih (akan diambil xcomnya karena sudah direturn path)
        staging_path = ti.xcom_pull(task_ids=previous_task_id, key="return_value")

        # Cek apakah staging_path berhasil diambil
        if not staging_path:
            raise ValueError("Staging path tidak ada.")

        # memasukan data menjadi dataframe ke variabel df untuk nanti di load ke sqlite
        df = pd.read_parquet(staging_path)

        # buat engine sqlitenya
        engine = create_engine(f"sqlite:///data/{nama_tabel}.db")

        # load ke sqlitenya
        df.to_sql(nama_tabel, con=engine, if_exists="replace", index=False)

    @task
    def end_task():
        pass

    (
        start_task()
        >> choose_extract_task()
        >> [extract_from_api(), extract_from_json(), extract_from_csv()]
        >> load_to_sqlite()
        >> end_task()
    )


dag = extract_load_sqlite_dag()
