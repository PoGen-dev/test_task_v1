import pandas as pd
import psycopg2

from sqlalchemy.exc import IntegrityError
from fastapi import FastAPI, UploadFile

app = FastAPI()


@app.post("/upload/")
async def upload_file(file: UploadFile):
    if not file.filename.endswith(".csv"):
        return {"error": "Invalid file format"}

    df = pd.read_csv(file.file, sep=";")
    df = prepare_df(df)

    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT MAX(version) as version FROM new_projects")
    max_version = cursor.fetchone()[0]
    max_version = max_version if max_version else 0
    current_version = max_version + 1

    # Установите соединение с базой данных
    try:
        for row in df.itertuples():
            # Вставьте данные в таблицу
            cursor.execute(
                """INSERT INTO new_projects (code, project, year, value, version) 
                   VALUES (%s, %s, %s, %s, %s)""",
                [
                    row.code,
                    row.project,
                    row.year,
                    row.value,
                    current_version,
                ],
            )

        conn.commit()
        return {"message": "File uploaded successfully"}
    except IntegrityError as e:
        conn.rollback()
        return {"error": "Data integrity violation"}
    finally:
        cursor.close()
        conn.close()


@app.get("/download/")
async def download_data(version: int):
    try:
        # Запросите данные из базы данных для заданной версии
        conn = get_connection()
        cursor = conn.cursor()
        query = """
        SELECT code, project, year, SUM(value) as value
        FROM new_projects
        WHERE version = %s
        GROUP BY code, project, year
        ORDER BY code;
        """

        result = cursor.execute(query, [version,])
        result = cursor.fetchall()

        db_df = pd.DataFrame(result, columns=["code", "project", "year", "value"])

        #  get df in input format
        rows = []
        for i, x in db_df.groupby(by="code"):
            first_part = {"code": i, "project": x.iloc[0].project}
            second_part = x.set_index("year")["value"].to_dict()
            rows.append({**first_part, **second_part})
        df = pd.DataFrame(rows)

        #  make dict like {'1.2': [6, 7, 8], ...} where '1.2' the code and
        #  [6, 7, 8] the indexes of lines in df which relate to him
        hierarchy_df = get_hierarchy_df(df["code"])
        result_dict = {}
        for index, row in hierarchy_df.items():
            if len(row) > 1:
                parent = ".".join(row[-1].split(".")[:-1])
                if parent in result_dict:
                    result_dict[parent].append(index)
                else:
                    result_dict[parent] = [index]

        #  get the list according to the hierarchy from bottom to top
        split_list = [list(map(int, item.split("."))) for item in result_dict.keys()]
        split_list.sort()
        sorted_list = [".".join(map(str, item)) for item in split_list]
        sorted_list.reverse()

        #  the final preparation for the input format
        for key in sorted_list:
            current_index = df[df["code"] == key].index.values[0]
            for year in db_df["year"].unique():
                value = df[df.index.isin(result_dict[key])][year].sum()
                df.at[current_index, year] = value
        
        return df.to_dict()
    except:
        return {"error": "can't prepare for the input format"}
    finally: 
        cursor.close()
        conn.close()


def get_connection():
    # Define the connection parameters
    db_params = {
        "host": "localhost",
        "port": "5432",
        "database": "postgres",
        "user": "postgres",
        "password": "root",
    }

    # Establish a connection to the database
    connection = psycopg2.connect(**db_params)
    return connection


def create_table():
    con = get_connection()
    cursor = con.cursor()
    cursor.execute(
        """CREATE TABLE IF NOT EXISTS new_projects (
    id SERIAL PRIMARY KEY,
    code VARCHAR(255),
    project VARCHAR(255),
    year INT,
    value FLOAT,
    version INT
);"""
    )
    con.commit()
    cursor.close()
    con.close()


def is_integer(string: str):
    try:
        int(string)
        return True
    except ValueError:
        return False


def prepare_df(df: pd.DataFrame):
    year_columns = [year for year in df.columns if is_integer(year)]
    result_df = pd.DataFrame([], columns=["code", "project", "year", "value"])
    index = 0
    for i in range(len(year_columns)):
        for row in df.itertuples():
            result_df.at[index, "code"] = row.code
            result_df.at[index, "project"] = row.project
            result_df.at[index, "year"] = year_columns[i]
            column_index = df.columns.get_loc(year_columns[i])
            result_df.at[index, "value"] = row[column_index + 1]
            index += 1

    return result_df


def get_hierarchy(code):
    parts = code.split(".")
    hierarchy = []
    for i in range(1, len(parts) + 1):
        hierarchy.append(".".join(parts[:i]))
    return hierarchy


def get_hierarchy_df(df: pd.DataFrame):
    df["hierarchy"] = df.apply(get_hierarchy)
    hierarchy_df = df["hierarchy"]
    hierarchy_df = hierarchy_df.drop_duplicates()
    hierarchy_df.reset_index(drop=True, inplace=True)
    return hierarchy_df


if __name__ == "__main__":
    import uvicorn

    create_table()
    uvicorn.run(app, host="0.0.0.0", port=8000)
