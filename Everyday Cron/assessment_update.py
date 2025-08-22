import html
import gc
import time
import warnings
import traceback
import mysql.connector
import re
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import configparser
import urllib3
# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

# Setup logging
import os

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, 'assessment_etl_update.log')
config_file = os.path.join(script_dir, 'config.ini')

logging.basicConfig(
    filename=log_file,
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
config.read(config_file)

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
    "ABMPS", "ANWEMS", "BNMCEMS", "BOPEMS", "CSMEMS",
    "DNMPS", "KCTVN", "LAPMEMS", "LBBNMCEMS",  "LDRKEMS",
    "LGRMNMCEMS", "LNMPS", "MEMS", "MLMPS", "MPMMPS",
    "NMMC93", "NNMPS", "PKGEMS", "RDNMCEMS", 
    "RMNMCEMS", "SMCMPS", "SMPS", "WBMPS", "SBP", "SBPMO", "RNMCEMS"
]

# Separate the assessment types into two lists
standardized_types = ["BOY", "MOY", "EOY"]
non_standardized_types = [
    "UNIT 1", "UNIT 2", "UNIT 3", "UNIT 4", "UNIT 5",
    "Unit 1A", "Unit 2A", "Unit 3A", "Unit 4A", "Unit 5A",
    "unit 1 A", "unit 2 A", "unit 3 A", "unit 4 A", "unit 5 A",
    "unit 1 B", "unit 2 B", "unit 3 B", "unit 4 B", "unit 5 B",
    "Unit 1B", "Unit 2B", "Unit 3B", "Unit 4B", "Unit 5B",
    "Weekly 1", "Weekly 2", "Weekly 3", "Weekly 4", "Weekly 5",
    "Prelim 1", "Prelim 2", "Prelim 3", "Prelim 4", "Prelim 5", 
    "Unit 1", "Unit 2", "Unit 3", "Unit 4", "Unit 5"
]


# Helper Functions
def trim_string(value):
    return html.unescape(str(value).strip()) if isinstance(value, str) else value

def camel_to_snake_case(name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower() if isinstance(name, str) else name

def generate_assessment_id(row):
    question = row.get('question_name', '') or ''
    words = re.findall(r'\w+', question)
    short_question = "_".join(words[:2]) if words else ''
    
    subject = (row.get('subject_name', '') or '')[:3]
    assessment_type = (row.get('assessment_type', '') or '')[:3]
    
    competency = row.get('competency_name', '') or ''
    comp_letters = "".join(word[0] for word in competency.upper().split() if word.isalpha())
    
    raw_date = str(row.get('assessment_date', ''))
    date_str = ''
    try:
        date_obj = datetime.strptime(raw_date, "%Y-%m-%d")
        date_str = date_obj.strftime("%y%m%d")
    except ValueError:
        date_str = raw_date[:6]

    parts = [
        str(row.get('student_id', '')),
        assessment_type,
        date_str,
        subject,
        comp_letters,
        short_question
    ]
    assessment_id = "_".join(p.strip().replace(" ", "_").upper() for p in parts if p)

    if len(assessment_id) > 64 and short_question:
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

def upsert_student_assessment_data(conn, records):
    """
    Inserts or updates records using a single ON DUPLICATE KEY UPDATE query.
    Assumes `assessment_id_generated` is a unique key in the database table.
    """
    if not records:
        return 0

    columns = [
        'student_id', 'student_name', 'gender', 'school_name', 'subject_name',
        'assessment_type', 'academic_year', 'grade_name', 'course_name',
        'division_name', 'competency_level_name', 'assessment_category',
        'assessment_date', 'obtained_marks', 'max_marks', 'percentage',
        'description', 'question_name', 'present_absent', 'assessment_id', 
        'assessment_id_generated', 'created_at', 'last_updated_at'
    ]

    insert_placeholders = ', '.join(['%s'] * len(columns))
    
    update_columns = [
        'student_id', 'student_name', 'gender', 'school_name', 'subject_name',
        'assessment_type', 'academic_year', 'grade_name', 'course_name',
        'division_name', 'competency_level_name', 'assessment_category',
        'assessment_date', 'obtained_marks', 'max_marks', 'percentage',
        'description', 'question_name', 'present_absent', 'assessment_id',
    ]
    set_clause = ', '.join([f"{col} = VALUES({col})" for col in update_columns]) 
    set_clause += f", last_updated_at = NOW()"

    query = f"""
        INSERT INTO student_full_assessment_data ({', '.join(columns)})
        VALUES ({insert_placeholders})
        ON DUPLICATE KEY UPDATE {set_clause};
    """

    data_to_upsert = []
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for r in records:
        r['assessment_id_generated'] = generate_assessment_id(r)
        record_values = [
            r.get('student_id'), r.get('student_name'), r.get('gender'),
            r.get('school_name'), r.get('subject_name'), r.get('assessment_type'),
            r.get('academic_year'), r.get('grade_name'), r.get('course_name'),
            r.get('division_name'), r.get('competency_level_name'), r.get('assessment_category'),
            r.get('assessment_date'), r.get('obtained_marks'), r.get('max_marks'),
            r.get('percentage'), r.get('description'), r.get('question_name'),
            r.get('present_absent'), r.get('assessment_id'), r.get('assessment_id_generated'),
            now, now
        ]
        data_to_upsert.append(record_values)

    total_affected = 0
    try:
        with conn.cursor() as cursor:
            cursor.executemany(query, data_to_upsert)
            total_affected = cursor.rowcount
            conn.commit()
            logging.info(f"Upserted {total_affected} records.")
    except mysql.connector.Error as err:
        logging.error(f"Upsert failed: {err}")
        conn.rollback()

    return total_affected


def update_assessments(assessment_types_list, assessment_category='Non-Standardized'):
    conn = connect_to_mysql()
    if not conn:
        return

    total_records = 0
    current_year = datetime.now().year
    current_month = datetime.now().month
    academic_year = f"{current_year-1}-{current_year}" if current_month < 6 else f"{current_year}-{current_year+1}"
    
    # Use a 60-day window to capture late entries
    date_threshold = datetime.now() - timedelta(days=60)

    for school in school_names:
        for assessment_type in assessment_types_list:
            logging.info(f"Processing: {school} - {academic_year} - {assessment_type} - {assessment_category}")
            
            params = {
                'api-key': api_key,
                'school_name': school,
                'academic_year': academic_year,
                'assessment_type': assessment_type
            }
            
            url = f"{api_url_base}/getAssessmentMarks.htm" if assessment_category.lower() == 'standardized' else f"{api_url_base}/getSchoolExamMarks.htm"
            
            try:
                logging.info(f"Making API request to: {url}")
                res = requests.get(url, params=params, timeout=600, verify=False)
                res.raise_for_status()
                data = res.json().get('data', [])

                if not data:
                    logging.info(f"No data for: {school} - {academic_year} - {assessment_type}")
                    continue

                df = pd.DataFrame(data)
                df.columns = [camel_to_snake_case(c) for c in df.columns]
                
                # Filter data early to reduce processing load
                df['assessment_date'] = pd.to_datetime(df['assessment_date'], errors='coerce', dayfirst=True)
                df = df[df['assessment_date'] >= date_threshold]
                
                if df.empty:
                    logging.info(f"No data within the last 60 days for: {school} - {academic_year} - {assessment_type}")
                    continue

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

                df['assessment_date'] = df['assessment_date'].dt.strftime('%Y-%m-%d')

                records = df.where(pd.notnull(df), None).to_dict('records')
                count = upsert_student_assessment_data(conn, records)
                total_records += count

                logging.info(f"✅ Processed: {school} - {academic_year} - {assessment_type} | Records affected: {count}")
                gc.collect()
                time.sleep(1)

            except Exception as e:
                logging.error(f"❌ Error processing: {school} - {academic_year} - {assessment_type}")
                logging.error(f"Exception: {str(e)}")
                logging.error(traceback.format_exc())

    if conn and conn.is_connected():
        conn.close()

    logging.info(f"🎯 Total records affected: {total_records}")

if __name__ == '__main__':
    # Process standardized assessments
    update_assessments(standardized_types, assessment_category='Standardized')
    update_assessments(non_standardized_types, assessment_category='Non-Standardized')