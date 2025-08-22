import sys
import mysql.connector
import json
import logging
import requests
import re
from datetime import datetime
import configparser
import urllib3
# ---------- Path setup ----------
import os
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(script_dir, 'active_students_update.log')
config_file = os.path.join(script_dir, 'config.ini')

# ---------- Logging setup ----------
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(log_formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# ---------- Config load ----------
config = configparser.ConfigParser()
if not os.path.exists(config_file):
    logger.error(f"Config file not found at: {config_file}")
    sys.exit(1)
config.read(config_file)

# Disable SSL warning since we're using verify=False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

api_base_url = config['api']['url'].rstrip('/')
api_key = config['api']['key']
api_url = f"{api_base_url}/getActiveStudents.htm"

db_config = {
    'user': config['mysql']['user'],
    'password': config['mysql']['password'],
    'host': config['mysql']['host'],
    'port': int(config['mysql']['port']),
    'database': config['mysql']['database']
}

# ---------- API fetch ----------
def get_api_session():
    try:
        session = requests.Session()
        session.verify = False
        session.headers.update({
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'application/json'
        })
        return session
    except Exception as e:
        logger.error(f"Failed to create session: {str(e)}")
        return None

def fetch_data_from_api():
    try:
        logger.info("Fetching data from API...")
        session = get_api_session()
        if not session:
            return None

        params = {
            'api-key': api_key,
            'school_name': 'ALL'
        }
        # Use a single request with the correct parameters
        response = session.get(api_url, params=params, timeout=600)  # Increased timeout to match assessment.py
        logger.info(f"Response status code: {response.status_code}")
        
        if response.status_code == 200:
            try:
                data = response.json()
                if data and isinstance(data, dict):
                    logger.info(f"Data fetched successfully. Records found: {len(data.get('data', []))}")
                    return data
                else:
                    logger.error("Invalid JSON response format")
                    return None
            except json.JSONDecodeError as je:
                logger.error(f"Failed to parse JSON response: {str(je)}")
                logger.error(f"Response content: {response.text[:500]}...")  # Log first 500 chars
                return None
        else:
            logger.error(f"Failed to retrieve data. Status code: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Request error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return None

# ---------- Cleaning functions ----------
def clean_student_name(value):
    return re.sub(r'\s+', " ", value).strip().title() if value else None

def convert_grade_name(value):
    if not value:
        return None
    if value == "Jr.KG":
        return "JR.KG"
    elif value == "Sr.KG":
        return "SR.KG"
    roman_to_number_pattern = re.compile(r"GRADE (\w+)")
    roman_to_number = {
        "I": "1", "II": "2", "III": "3", "IV": "4", "V": "5", 
        "VI": "6", "VII": "7", "VIII": "8", "IX": "9", "X": "10"
    }
    match = roman_to_number_pattern.match(value)
    if match:
        roman = match.group(1)
        return f"GRADE {roman_to_number.get(roman, roman)}"
    return value

def format_date_column(original_date):
    try:
        return datetime.strptime(original_date, '%d/%m/%Y').strftime('%Y-%m-%d')
    except Exception:
        logger.warning(f"Invalid date format: {original_date}")
        return None

def clean_gender(value):
    if not value:
        return None
    value = value.strip().upper()
    return "M" if value == "MALE" else "F" if value == "FEMALE" else value

def extract_division(value):
    match = re.search(r'[A-Za-z]+', value)
    return match.group(0) if match else value

# ---------- Unique key generator (NO MD5) ----------
def generate_unique_key(record):
    return f"{record['school_name'].strip()}_{record['student_id']}_{record['academic_year']}_{record['grade_name']}"

# ---------- MySQL connection ----------
def connect_to_mysql():
    try:
        logger.info("Connecting to MySQL...")
        # Try different authentication methods
        auth_methods = [
            {'use_pure': True},  # Try pure Python implementation
            {'auth_plugin': 'caching_sha2_password'},  # MySQL 8.0 default
            {'auth_plugin': 'mysql_native_password'}  # MySQL 5.7 default
        ]
        
        conn = None
        last_error = None
        
        for auth in auth_methods:
            try:
                connection_config = db_config.copy()
                connection_config.update(auth)
                conn = mysql.connector.connect(**connection_config)
                if conn and conn.is_connected():
                    logger.info("Connected to MySQL.")
                    return conn
            except mysql.connector.Error as e:
                last_error = e
                continue
                
        if not conn and last_error:
            raise last_error
            
    except Exception as err:
        logger.error(f"MySQL connection failed: {err}")
        return None
    
    return None

def create_tables_if_not_exist(conn):
    try:
        with conn.cursor() as cursor:
            # Create main table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS active_student_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                created_date DATE,
                school_name VARCHAR(255) NOT NULL,
                status VARCHAR(50),
                grade_name VARCHAR(50),
                student_name VARCHAR(500) NOT NULL,
                student_id VARCHAR(50) NOT NULL,
                gender CHAR(50),
                division_name VARCHAR(10) NOT NULL,
                academic_year VARCHAR(10) NOT NULL,
                unique_key VARCHAR(255) NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_school_grade (school_name, grade_name),
                INDEX idx_student (student_id),
                INDEX idx_academic_year (academic_year),
                UNIQUE INDEX idx_unique_key (unique_key)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            
            logger.info("Tables and indexes created successfully.")
            conn.commit()
    except mysql.connector.Error as err:
        logger.error(f"Error creating tables: {err}")
        conn.rollback()

# ---------- Insert or update ----------
def insert_data_to_mysql(cursor, record):
    sql = """
    INSERT INTO active_student_data (
        created_date, school_name, status, grade_name, student_name, student_id, gender,
        division_name, academic_year, unique_key, timestamp
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        created_date = VALUES(created_date),
        status = VALUES(status),
        grade_name = VALUES(grade_name),
        student_name = VALUES(student_name),
        gender = VALUES(gender),
        division_name = VALUES(division_name),
        academic_year = VALUES(academic_year),
        timestamp = VALUES(timestamp)
    """
    try:
        now = datetime.now()
        academic_year = f"{now.year}-{now.year + 1}" if now.month >= 5 else f"{now.year - 1}-{now.year}"
        grade_clean = convert_grade_name(record.get('grade_name'))

        unique_key = generate_unique_key({
            'school_name': record.get('school_name'),
            'student_id': record.get('student_id'),
            'academic_year': academic_year,
            'grade_name': grade_clean
        })

        created_date_raw = record.get('created_date')
        created_date = format_date_column(created_date_raw) if created_date_raw else None

        cursor.execute(sql, (
            created_date,
            record.get('school_name'),
            record.get('status'),
            grade_clean,
            clean_student_name(record.get('student_name')),
            record.get('student_id'),
            clean_gender(record.get('gender')),
            extract_division(record.get('division_name')),
            academic_year,
            unique_key,
            now.strftime('%Y-%m-%d %H:%M:%S')
        ))

        if cursor.rowcount == 1:
            logger.info(f"[INSERT] Student ID: {record.get('student_id')} | Key: {unique_key}")
        elif cursor.rowcount == 2:
            logger.info(f"[UPDATE] Student ID: {record.get('student_id')} | Key: {unique_key}")

    except mysql.connector.Error as err:
        logger.error(f"MySQL insert/update error: {err}")

# ---------- Main ----------
def main():
    logger.info("==== Starting Active Student Update ====")
    print("Starting update process.")

    # First establish database connection and create tables
    conn = connect_to_mysql()
    if not conn:
        sys.exit()

    # Create the tables if they don't exist
    create_tables_if_not_exist(conn)

    # Then fetch and process the data
    json_response = fetch_data_from_api()
    if not json_response:
        logger.error("No data fetched. Exiting.")
        sys.exit()

    students_data = json_response.get('data', [])
    if not students_data:
        logger.error("API returned empty student data.")
        sys.exit()

    cursor = conn.cursor()
    print("Inserting/updating records...")
    logger.info(f"Processing {len(students_data)} records.")

    for record in students_data:
        insert_data_to_mysql(cursor, record)

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Process completed.")
    logger.info("==== Update Process Complete ====")

# ---------- Entry ----------
if __name__ == "__main__":
    main()
