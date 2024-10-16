import boto3 , csv , random , time , os , logging , string
from io import StringIO
import pandas as pd
from dotenv import load_dotenv
from faker import Faker

# Load the .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# AWS credentials
aws_access_key_id = os.getenv('access_key_id')
aws_secret_access_key = os.getenv('secret_access_key')
region_name = 'us-east-2'  # e.g., 'us-west-1'

# Configure AWS S3 with authentication
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

bucket_name = 'landing-phcj'
s3_folder = 'data/'  # Optional, if you want to upload to a specific folder


fake = Faker()

def generate_random_string(length):
    characters = string.ascii_letters + string.digits  # a-z, A-Z, 0-9
    random_string = ''.join(random.choices(characters, k=length))
    return random_string

def generate_random_numeric_string(length):
    numbers = string.digits  # '0123456789'
    random_numbers = ''.join(random.choices(numbers, k=length))
    return random_numbers

def generate_random_letters(length):
    letters = string.ascii_letters  # a-z and A-Z
    random_letters = ''.join(random.choices(letters, k=length))
    return random_letters

def generate_random_data():
    """Generate random data for the CSV."""

    logging.info(f"Generated random data ... ")

    data = []
    lenght_random = random.randint(1, 1000)

    for i in range( 0 , lenght_random ):
        data.append(
            {
                'id_': generate_random_numeric_string(50),
                'name': fake.name(),
                'city' : fake.city(),
                'phone_number' : fake.basic_phone_number(),
                'email' : fake.ascii_email(),
                'date_': fake.date(),
                'timestamp_': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            }
        )
    
    return data

def create_csv(data):
    """Create a CSV file in memory using the data."""
    csv_buffer = StringIO()
    df = pd.DataFrame(data)
    df.to_csv(csv_buffer, index=False)
    logging.info("CSV file created in memory")
    return csv_buffer.getvalue()

def upload_to_s3(csv_content, filename):
    """Upload the generated CSV file to S3."""
    try:
        s3.put_object(Body=csv_content, Bucket=bucket_name, Key=f"{s3_folder}{filename}")
        logging.info(f"Uploaded {filename} to S3")
    except Exception as e:
        logging.error(f"Failed to upload {filename} to S3: {e}")

def main():
    """Main function to generate and upload CSVs to S3 every second."""
    logging.info("Starting the CSV generation and upload process")
    while True:
        try:
            data = generate_random_data()
            csv_content = create_csv(data)
            filename = f'random_data_{int(time.time())}.csv'  # Unique filename using the current timestamp
            upload_to_s3(csv_content, filename)
        except Exception as e:
            logging.error(f"An error occurred: {e}")

        time_to_sleep = random.randint(10, 100)
        logging.info(f"Sleeping for {time_to_sleep} seconds before the next iteration")
        time.sleep(time_to_sleep)

if __name__ == '__main__':
    main()
