import html
import gc
import time
import warnings
import traceback
import mysql.connector
import re
import requests
import pandas as pd
from datetime import datetime
import logging
import configparser
import urllib3
# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

# Setup logging
logging.basicConfig(
    filename='assessment_etl_student.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# Read config
config = configparser.ConfigParser()
config.read('config.ini')

api_url_base = config.get('api', 'url', fallback='https://akanksha.edustems.com')
api_key = config.get('api', 'key', fallback='default_api_key')

db_config = {
    'user': config['mysql']['user'],
    'password': config['mysql']['password'],
    'host': config['mysql']['host'],
    'port': int(config['mysql']['port']),
    'database': config['mysql']['database']
}

school_names = [
    "ABMPS", "ANWEMS", "BNMCEMS", "BNPS", "BOPEMS", "CSMEMS",
    "DNMPS", "KCTVN", "LAPMEMS", "LBBNMCEMS", "LBBNPS", "LDRKEMS",
    "LGMNPS", "LGRMNMCEMS", "LNMPS", "MEMS", "MLMPS", "MPMMPS",
    "NMMC93", "NNMPS", "PKGEMS", "RDNMCEMS", "RDNPS", 
    "RMNMCEMS", "RMNPS"
]


assessment_types = [
    "BOY", "MOY", "EOY", "UNIT 1", "UNIT 2", "UNIT 3", "UNIT 4", "UNIT 5",
    "Unit 1A", "Unit 2A", "Unit 3A", "Unit 4A", "Unit 5A",
    "unit 1 A", "unit 2 A", "unit 3 A", "unit 4 A", "unit 5 A",
    "unit 1 B", "unit 2 B", "unit 3 B", "unit 4 B", "unit 5 B",
    "Unit 1B", "Unit 2B", "Unit 3B", "Unit 4B", "Unit 5B",
    "Weekly 1", "Weekly 2", "Weekly 3", "Weekly 4", "Weekly 5",
    "Prelim 1", "Prelim 2", "Prelim 3", "Prelim 4", "Prelim 5"]

def trim_string(value):
    return html.unescape(str(value).strip()) if isinstance(value, str) else value

def camel_to_snake_case(name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower() if isinstance(name, str) else name

import re
from datetime import datetime

def generate_assessment_id(row):
    # Clean question name: first 2 words only, alphanumeric
    question = row.get('question_name', '') or ''
    words = re.findall(r'\w+', question)
    short_question = "_".join(words[:2]) if words else ''
    
    # Shorten subject and assessment type
    subject = (row.get('subject_name', '') or '')[:3]
    assessment_type = (row.get('assessment_type', '') or '')[:3]
    
    # Competency: take first letter of each word
    competency = row.get('competency_name', '') or ''
    comp_letters = "".join(word[0] for word in competency.upper().split() if word.isalpha())
    
    # Convert date to YYMMDD (if valid)
    raw_date = str(row.get('assessment_date', ''))
    date_str = ''
    try:
        date_obj = datetime.strptime(raw_date, "%Y-%m-%d")
        date_str = date_obj.strftime("%y%m%d")  # e.g. 2023-10-25 → 231025
    except ValueError:
        date_str = raw_date[:6]  # fallback if date is not valid

    # Build ID in required order
    parts = [
        str(row.get('student_id', '')),
        assessment_type,
        date_str,
        subject,
        comp_letters,
        short_question
    ]
    assessment_id = "_".join(p.strip().replace(" ", "_").upper() for p in parts if p)

    # Ensure max 64 chars: trim question first if too long
    if len(assessment_id) > 64 and short_question:
        # rebuild without question part
        parts_no_q = parts[:-1]
        assessment_id = "_".join(p.strip().replace(" ", "_").upper() for p in parts_no_q if p)
    return assessment_id[:64]

def clean_gender(gender):
    if not isinstance(gender, str):
        return None
    gender = gender.strip().lower()
    female_values = {'f', 'female', 'femal', 'fem', 'girl', 'girls', 'gril', 'gurl', 'g'}
    male_values = {'m', 'male', 'mal', 'boy', 'boys', 'boi', 'b'}
    if gender in female_values:
        return 'F'
    elif gender in male_values:
        return 'M'
    else:
        return None

def connect_to_mysql():
    try:
        conn = mysql.connector.connect(**db_config, charset='utf8mb4')
        if conn.is_connected():
            logging.info("Connected to MySQL")
            return conn
    except mysql.connector.Error as err:
        logging.error(f"MySQL connection failed: {err}")
    return None

def create_table_if_not_exists(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS student_full_assessment_data (
        id INT(11) NOT NULL AUTO_INCREMENT,
        student_id VARCHAR(100),
        student_name VARCHAR(255),
        gender VARCHAR(1),
        school_name VARCHAR(100),
        subject_name VARCHAR(100),
        assessment_type VARCHAR(100),
        academic_year VARCHAR(20),
        grade_name VARCHAR(50),
        course_name VARCHAR(100),
        division_name VARCHAR(10),
        competency_level_name TEXT,
        assessment_category VARCHAR(50),
        assessment_date DATE,
        obtained_marks FLOAT,
        max_marks FLOAT,
        percentage FLOAT,
        description TEXT,
        question_name TEXT,
        present_absent VARCHAR(1),
        assessment_id VARCHAR(255),
        assessment_id_generated VARCHAR(255),
        created_at DATETIME,
        last_updated_at DATETIME,
        PRIMARY KEY (id),
        UNIQUE KEY uniq_assessment_generated (assessment_id_generated(191)),
        KEY idx_full_dashboard_filters (
            assessment_type,
            academic_year,
            subject_name,
            school_name,
            grade_name,
            division_name
        ),
        KEY idx_full_competency_level (competency_level_name(191))
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
            conn.commit()
            logging.info("Table ensured to exist.")
    except mysql.connector.Error as err:
        logging.error(f"Failed to create table: {err}")

def insert_student_assessment_data(conn, records):
    if not records:
        return 0

    columns = [
        'student_id', 'student_name', 'gender', 'school_name', 'subject_name',
        'assessment_type', 'academic_year', 'grade_name', 'course_name',
        'division_name', 'competency_level_name', 'assessment_category',
        'assessment_date', 'obtained_marks', 'max_marks', 'percentage',
        'description', 'question_name', 'present_absent', 'assessment_id', 'assessment_id_generated', 'created_at', 'last_updated_at'
    ]

    placeholders = ', '.join(['%s'] * len(columns))
    update_clause = ', '.join([f"{col}=VALUES({col})" for col in columns if col not in ['assessment_id_generated', 'created_at']])

    query = f"""
        INSERT INTO student_full_assessment_data ({', '.join(columns)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause};
    """

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    values = []

    for r in records:
        r['assessment_id_generated'] = generate_assessment_id(r)
        values.append([
            r.get('student_id'), r.get('student_name'), r.get('gender'),
            r.get('school_name'), r.get('subject_name'), r.get('assessment_type'),
            r.get('academic_year'), r.get('grade_name'), r.get('course_name'),
            r.get('division_name'), r.get('competency_level_name'), r.get('assessment_category'),
            r.get('assessment_date'), r.get('obtained_marks'), r.get('max_marks'),
            r.get('percentage'), r.get('description'), r.get('question_name'),
            r.get('present_absent'), r.get('assessment_id'), r.get('assessment_id_generated'), now, now
        ])

    try:
        with conn.cursor() as cursor:
            cursor.executemany(query, values)
            conn.commit()
            return cursor.rowcount
    except mysql.connector.Error as err:
        logging.error(f"Insert failed: {err}")
        conn.rollback()
        return 0

def clean_and_format_text(df):
    text_cols = ['student_name', 'subject_name', 'question_name', 'description', 'competency_level_name']
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(trim_string)
            df[col] = df[col].str.replace(r'\s+', ' ', regex=True).str.strip()
            if col in text_cols:
                df[col] = df[col].apply(lambda x: ' '.join([w.capitalize() for w in x.split()]) if isinstance(x, str) else x)
    return df

def extract_division_name(division):
    if not isinstance(division, str):
        return None
    match = re.search(r'\b([A-Za-z]{1,3})\b$', division.strip())
    return match.group(1).upper() if match else division.strip().upper()

def standardize_grade(grade):
    if not isinstance(grade, str):
        return None
    grade = grade.strip().lower()
    pre_primary_map = {
        'nursery': 'NURSERY',
        'jr kg': 'JUNIOR KG', 'jrkg': 'JUNIOR KG', 'junior kg': 'JUNIOR KG',
        'sr kg': 'SENIOR KG', 'srkg': 'SENIOR KG', 'senior kg': 'SENIOR KG',
        'j.k.g.': 'JUNIOR KG', 's.k.g.': 'SENIOR KG',
        'lkg': 'JUNIOR KG', 'ukg': 'SENIOR KG'
    }
    for key, value in pre_primary_map.items():
        if key in grade:
            return value
    roman_map = {'i': 1, 'ii': 2, 'iii': 3, 'iv': 4, 'v': 5,
                 'vi': 6, 'vii': 7, 'viii': 8, 'ix': 9, 'x': 10}
    roman_match = re.search(r"(grade)?\s*(i{1,3}|iv|v|vi{0,3}|ix|x)\b", grade)
    if roman_match:
        roman = roman_match.group(2).lower()
        if roman in roman_map:
            return f"GRADE {roman_map[roman]}"
    number_match = re.search(r"(grade|grdae|graed)?\s*(\d{1,2})\b", grade)
    if number_match:
        return f"GRADE {int(number_match.group(2))}"
    return grade.upper()

def run_student_level_etl(start_year=2021, assessment_category='Non-Standardized'):
    conn = connect_to_mysql()
    if not conn:
        return

    create_table_if_not_exists(conn)
    total_records = 0
    current_year = datetime.now().year
    current_month = datetime.now().month
    latest_academic_year = current_year if current_month >= 6 else current_year - 1

    for year in range(start_year, latest_academic_year + 1):
        academic_year = f"{year}-{year + 1}"

        for school in school_names:
            for assessment_type in assessment_types:
                logging.info(f"Starting: {school} - {academic_year} - {assessment_type} - {assessment_category}")
                params = {
                    'api-key': api_key,
                    'school_name': school,
                    'academic_year': academic_year,
                    'assessment_type': assessment_type
                }
                url = f"{api_url_base}/getAssessmentMarks.htm" if assessment_category.lower() == 'standardized' else f"{api_url_base}/getSchoolExamMarks.htm"
                try:
                    res = requests.get(url, params=params, timeout=600, verify=False)
                    res.raise_for_status()
                    data = res.json().get('data', [])

                    if not data:
                        logging.info(f"No data: {school} - {academic_year} - {assessment_type}")
                        continue

                    df = pd.DataFrame(data)
                    df.columns = [camel_to_snake_case(c) for c in df.columns]
                    df['academic_year'] = academic_year
                    df['assessment_type'] = assessment_type
                    df['assessment_category'] = assessment_category

                    df = clean_and_format_text(df)
                    df['gender'] = df['gender'].apply(clean_gender)
                    df['grade_name'] = df['grade_name'].apply(standardize_grade)
                    df['division_name'] = df['division_name'].apply(extract_division_name)

                    if assessment_category.lower() == 'non-standardized':
                        df['competency_level_name'] = df.apply(
                            lambda row: row['description'] if pd.isna(row.get('competency_level_name')) or row['competency_level_name'] in [None, '', 'NaN'] else row['competency_level_name'],
                            axis=1
                        )

                    # robust date parsing
                    df['assessment_date'] = pd.to_datetime(df['assessment_date'], errors='coerce', dayfirst=True)
                    df['assessment_date'] = df['assessment_date'].dt.strftime('%Y-%m-%d')

                    records = df.where(pd.notnull(df), None).to_dict('records')
                    count = insert_student_assessment_data(conn, records)
                    total_records += count

                    logging.info(f"✅ Completed: {school} - {academic_year} - {assessment_type} | Records: {count}")
                    gc.collect()
                    time.sleep(1)

                except Exception as e:
                    logging.error(f"❌ Error: {school} - {academic_year} - {assessment_type}")
                    logging.error(f"Exception: {str(e)}")
                    logging.error(traceback.format_exc())

    if conn.is_connected():
        conn.close()

    logging.info(f"🎯 Total records inserted/updated: {total_records}")

if __name__ == '__main__':
    run_student_level_etl(start_year=2022, assessment_category='Standardized')
    run_student_level_etl(start_year=2022, assessment_category='Non-Standardized')
