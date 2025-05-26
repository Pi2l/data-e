import csv
import datetime
import os


class MonthlyFileHandler:
    def __init__(self, minio_client):
        self.current_month = None
        self.month_year_file = None
        self.dict_writer = None
        self.minio_client = minio_client

    def open_file(self, month_year, init_data):
        os.makedirs('./out', exist_ok=True)
        path = os.path.join('./out', f'{month_year}.csv')
        file_exists = os.path.isfile(path)
        month_year_file = open(path, 'a', newline='', encoding='utf-8')
        dict_writer = csv.DictWriter(month_year_file, fieldnames=init_data.keys())
        if not file_exists:
            dict_writer.writeheader()
        dict_writer.writerow(init_data)
        month_year_file.flush()
        return month_year_file, dict_writer

    def process_message(self, message):
        start_time = message.get('start_time', '')
        if start_time:
            dt = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            month_year = f"{dt.strftime('%B').lower()}_{dt.year}"
            month_processed = dt.strftime('%B').lower()

            if self.current_month is None:
                # print(f"if self.current_month is None: {self.current_month}")
                self.current_month = month_processed
                self.month_year_file, self.dict_writer = self.open_file(month_year, message)
            elif self.current_month == month_processed:
                # print(f"if self.current_month == month_processed: {self.current_month}")
                self.dict_writer.writerow(message)
                self.month_year_file.flush()
            else:
                # print(f"else: {self.current_month}, {self.month_year_file}, {self.dict_writer}")
                previous_month_year = f"{self.current_month}_{dt.year}"
                self.month_year_file.close()
                self.month_year_file, self.dict_writer = self.open_file(month_year, message)
                upload_to_minio(os.path.join('./out', f"{previous_month_year}.csv"), self.minio_client)
                os.remove(os.path.join('./out', f"{previous_month_year}.csv"))
                self.current_month = month_processed


BUCKET_NAME = os.getenv('MINIO_BUCKET', 'transportation-bucket')

def upload_to_minio(filepath, minio_client):
    try:
        print(f"Uploading {filepath} to MinIO")
        minio_client.fput_object(BUCKET_NAME, os.path.basename(filepath), filepath)
        # print(f"Uploaded {filepath} to MinIO")
    except Exception as e:
        print(f"Error uploading to MinIO: {e}")
