import openpyxl
import csv
from pathlib import Path
from django.core.management.base import BaseCommand
from app.models import Crane
from datetime import datetime

class Command(BaseCommand):
    help = "Import Crane data from Excel (.xlsx/.xlsm/.xltx/.xltm) or CSV (.csv)"

    def add_arguments(self, parser):
        parser.add_argument('file_path', type=str, help="Path to Excel/CSV file")

    def _rows_from_file(self, file_path):
        suffix = Path(file_path).suffix.lower()

        if suffix == '.csv':
            return self._read_csv_rows(file_path)

        if suffix in {'.xlsx', '.xlsm', '.xltx', '.xltm'}:
            wb = openpyxl.load_workbook(file_path)
            sheet = wb.active
            return list(sheet.iter_rows(values_only=True))

        raise ValueError(
            "Unsupported file format. Please use one of: .csv, .xlsx, .xlsm, .xltx, .xltm"
        )

    def _read_csv_rows(self, file_path):
        encodings = ['utf-8-sig', 'cp1252', 'latin-1', 'utf-16']
        last_error = None

        for encoding in encodings:
            try:
                with open(file_path, mode='r', encoding=encoding, newline='') as csv_file:
                    sample = csv_file.read(4096)
                    csv_file.seek(0)

                    try:
                        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                    except csv.Error:
                        dialect = csv.excel

                    reader = csv.reader(csv_file, dialect)
                    return list(reader)
            except UnicodeDecodeError as exc:
                last_error = exc
                continue

        if last_error:
            raise last_error

        raise ValueError("Could not read CSV file.")

    def clean(self, value):
        """Convert any non-date value into clean string."""
        if value is None:
            return ""
        if isinstance(value, float):
            if value.is_integer():
                return str(int(value))
            return str(value)
        return str(value)

    def clean_date(self, value):
        """Convert Excel date -> 'YYYY-MM-DD', fallback to string."""
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d")
        if value is None:
            return ""
        # If it's already a string like '2014-02-17'
        try:
            parsed = datetime.strptime(str(value), "%Y-%m-%d")
            return parsed.strftime("%Y-%m-%d")
        except:
            return str(value)

    def clean_int(self, value):
        """Convert anything to safe integer."""
        if isinstance(value, datetime):
            return int(value.strftime("%Y%m%d"))
        if value is None:
            return 0
        if isinstance(value, float):
            return int(value)
        try:
            return int(value)
        except:
            return 0

    def handle(self, *args, **kwargs):
        file_path = kwargs['file_path']
        print(f"Reading: {file_path}")

        rows = self._rows_from_file(file_path)

        if not rows:
            print("No data found in file.")
            return

        data_rows = rows[1:]  # skip header

        count = 0
        last_lg = ""
        last_kundenummer = ""

        for row in data_rows:
            row = list(row)
            if len(row) < 16:
                row += [None] * (16 - len(row))

            lg_value = self.clean(row[3])
            kundenummer_value = self.clean(row[4])

            if lg_value:
                last_lg = lg_value
            else:
                lg_value = last_lg

            if kundenummer_value:
                last_kundenummer = kundenummer_value
            else:
                kundenummer_value = last_kundenummer

            Crane.objects.create(
                kran_typ=self.clean(row[0]),
                fabrik_nr=self.clean(row[1]),
                kunde=self.clean(row[2]),
                lg=lg_value,
                kundenummer=kundenummer_value,
                version=self.clean(row[5]),
                serien_nr=self.clean(row[6]),
                tel_nr=self.clean(row[7]),
                ip=self.clean(row[8]),
                rueckmeldung=self.clean(row[9]),
                it_nr=self.clean(row[10]),
                kundenkran=self.clean(row[11]),
                lizenz_ja=self.clean(row[12]),
                lizenzdatum=self.clean_date(row[13]),
                bezahlt_bis_rg_erstellt=self.clean_date(row[14]),
                servicemeldung=self.clean_int(row[15])
            )

            count += 1

        print(f"Successfully imported {count} rows!")