import pandas as pd
from sqlalchemy import create_engine
from glob import glob
import os


# 연결 문자열 생성
username = 'admin'
password = 'admin'
host = 'localhost'
db = 'mydb'
conn_string = f"postgresql://{username}:{password}@{host}/{db}"


# 데이터베이스 엔진 생성
engine = create_engine(conn_string)


# 데이터 경로 설정
data_path = '/Users/b06/Desktop/yeardream/medi-05/data/pyspark-test'
dirs = {
    "review_settings_keyword": "review_keywords",
    "keywords": "keywords",
    "conveniences": "conveniences",
    "description": "description",
    "data": "data"
}


# 각 폴더 내의 첫 번째 CSV 파일을 찾아 해당 데이터로 테이블을 생성하고 업로드
for dir_name, tbl_name in dirs.items():
    dir_path = f"{data_path}/{dir_name}"
    csv_files = glob(os.path.join(dir_path, '*.csv'))
    if csv_files:
        csv_file = csv_files[0]  # 폴더 내의 첫 번째 CSV 파일을 사용
        try:
            # CSV 파일을 DataFrame으로 로드
            df = pd.read_csv(csv_file, encoding='cp949')
            
            # 데이터프레임을 SQL 테이블로 변환
            df.to_sql(tbl_name, engine, if_exists='replace', index=False)
            
            # 테이블 생성 및 데이터 업로드 완료 메시지 출력
            print(f"table {tbl_name} created and populated successfully with {csv_file}.")
        
        except Exception as e:
            print(f"An error occurred while processing {csv_file}: {e}")
    else:
        print(f"No CSV files found in {dir_path}.")