from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

# 연결 문자열 생성
username = 'admin'
password = 'admin'
host = 'localhost'
db = 'mydb'
conn = f"postgresql://{username}:{password}@{host}/{db}"


# 데이터베이스 엔진 생성
engine = create_engine(conn)


# 데이터베이스에 연결을 시도하고 PostgreSQL 버전 체크
try:
    # connect() 메소드를 사용하여 연결을 시도합니다.
    with engine.connect() as connection:
        # 연결이 성공하면, 서버 버전을 확인하는 쿼리를 실행합니다.
        version = connection.execute("SELECT version();")        
        print(version.fetchone())
        print("Database connection was successful.")
        
except OperationalError as e:
    print(f"Error: {e}")
    print("Database connection failed.")
