import sqlite3
import requests
import json


def convert_dict_to_string(dict_entry):
    if dict_entry.get('geolocation', None) is None:
        return None
    else:
        return json.dumps(dict_entry['geolocation'])


def main():
    response_opj = requests.get('https://data.nasa.gov/resource/gh4g-9sfh.json')

    json_obj = response_opj.json()

    # print(json_obj[2])
    # print(json_obj[2]['geolocation'])
    # print(json_obj[2]['geolocation']['longitude'])

    db_connection = None
    try:
        db_name = 'meteorite_db_all.db'
        # this connects to a database (creates it if it doesn't exist)
        db_connection = sqlite3.connect(db_name)

        # creates cursor
        db_cursor_obj = db_connection.cursor()

        # creates all_meteorites table if it doesn't exist
        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS all_meteorites(
                                name TEXT,
                                id INTEGER,
                                nametype TEXT,
                                recclass TEXT,
                                mass TEXT,
                                fall TEXT,
                                year TEXT,
                                reclat TEXT,
                                reclong TEXT,
                                geolocation TEXT,
                                states TEXT,
                                counties TEXT);''')

        # clear the table if it already has data in it from the last time we ran this program
        db_cursor_obj.execute('''DELETE FROM all_meteorites''')

        for dict_entry in json_obj:

            # inputs data values into all_meteorites table
            db_cursor_obj.execute('''INSERT INTO all_meteorites VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                  (dict_entry.get('name', None),
                                   int(dict_entry.get('id', None)),
                                   dict_entry.get('nametype', None),
                                   dict_entry.get('recclass', None),
                                   dict_entry.get('mass', None),
                                   dict_entry.get('fall', None),
                                   dict_entry.get('year', None),
                                   dict_entry.get('reclat', None),
                                   dict_entry.get('reclong', None),
                                   convert_dict_to_string(dict_entry),
                                   dict_entry.get(':@computed_region_cbhk_fwbd', None),
                                   dict_entry.get(':@computed_region_nnga_25f4', None)))

        # grabs table entries where the id value is less than or equal to 1000
        db_cursor_obj.execute('SELECT * FROM all_meteorites WHERE id <= 1000')
        q1_result = db_cursor_obj.fetchall()

        # creates new table with the entries that were just grabbed
        db_cursor_obj.execute('''CREATE TABLE IF NOT EXISTS filtered_data(
                                        name TEXT,
                                        id INTEGER,
                                        nametype TEXT,
                                        recclass TEXT,
                                        mass TEXT,
                                        fall TEXT,
                                        year TEXT,
                                        reclat TEXT,
                                        reclong TEXT,
                                        geolocation TEXT,
                                        states TEXT,
                                        counties TEXT);''')

        # clears table
        db_cursor_obj.execute('''DELETE FROM filtered_data''')

        for tuple_entry in q1_result:
            db_cursor_obj.execute('''INSERT INTO filtered_data VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', tuple_entry)

        db_connection.commit()

    except sqlite3.Error as db_error:
        print(f'A database error has occurred: {db_error}')
    finally:
        if db_connection:
            db_connection.close()
            print('The database has been closed')


if __name__ == '__main__':
    main()
