import sqlite3


class ExtractionLogDb:
    def __init__(self):
        self.__db_name = "extraction_log.db"
        self.__conn = sqlite3.connect(self.__db_name)
        self.__cursor = self.__conn.cursor()

    def dates_to_convert(self):
        try:
            result = self.__cursor.execute('''SELECT extraction_date
                                FROM extraction
                                WHERE parquet_converted IS NULL
                                AND extraction_date < CURRENT_DATE;
                                ''')
        except sqlite3.Error as exc:
            self.close_connection()
            raise sqlite3.Error from exc
        return result.fetchall()

    def set_date_false(self, date: str):
        try:
            self.__cursor.execute('''UPDATE extraction
                                SET parquet_converted = "FALSE",
                                    uploaded = "FALSE"
                                WHERE extraction_date = ?
                                ''', (date,))
        except sqlite3.Error as exc:
            self.close_connection()
            raise sqlite3.Error from exc
        else:
            self.__conn.commit()
            return 0

    def set_parquet_converted_true(self, date: str):
        try:
            self.__cursor.execute('''UPDATE extraction
                                SET parquet_converted = "TRUE"
                                WHERE extraction_date = ?
                                ''', (date,))
        except sqlite3.Error as exc:
            self.close_connection()
            raise sqlite3.Error from exc
        else:
            self.__conn.commit()
            return 0

    def dates_to_upload(self):
        try:
            result = self.__cursor.execute('''SELECT extraction_date
                                FROM extraction
                                WHERE uploaded IS NULL
                                AND parquet_converted = "TRUE"
                                AND extraction_date < CURRENT_DATE;
                                ''')
        except sqlite3.Error as exc:
            self.close_connection()
            raise sqlite3.Error from exc
        return result.fetchall()

    def set_uploaded_true(self, date: str):
        try:
            self.__cursor.execute('''UPDATE extraction
                                SET uploaded = "TRUE"
                                WHERE extraction_date = ?
                                ''', (date,))
        except sqlite3.Error as exc:
            self.close_connection()
            raise sqlite3.Error from exc
        else:
            self.__conn.commit()
            return 0

    def close_connection(self):
        self.__conn.close()
        return 0
