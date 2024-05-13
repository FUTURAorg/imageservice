from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

Base = declarative_base()


class FaceRepository:
    
    def __init__(self, db_name):
        self.db_name = db_name

    def create(self):
        raise NotImplementedError
    
    def get_person(self):
        raise NotImplementedError


class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    name = Column(String)
    folder_name = Column(String)

class PostgreSQLRep(FaceRepository):
    
    def __init__(self, db_url):
        super().__init__(db_url)
        self.engine = create_engine(db_url)
        Base.metadata.create_all(self.engine)
        self.session_factory = sessionmaker(bind=self.engine)
        self.Session = scoped_session(self.session_factory)
    
    def create(self, name, folder_name):
        session = self.Session()
        try:
            new_user = User(name=name, folder_name=folder_name)
            session.add(new_user)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
    
    def get_person(self, folder_name):
        session = self.Session()
        try:
            person = session.query(User).filter_by(folder_name=folder_name).first()
            return person.name if person else None
        finally:
            session.close()
            

class SQLiteRep(FaceRepository):
    
    def __init__(self, db_name):
        super().__init__(db_name)
        self.connection = sqlite3.connect(self.db_name, check_same_thread=False)
        self.cursor = self.connection.cursor()
        
        self.prepare_db()
    
    def prepare_db(self):
        self.create_tables()
    
    def create_tables(self):
        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS users (
                                id INTEGER PRIMARY KEY,
                                name TEXT,
                                folder_name TEXT
                            );
                            """)
        self.connection.commit()
    
    def create(self, name, folder_name):
        self.cursor.execute("""
                            INSERT INTO users (name, folder_name) VALUES (?, ?)
                            """, (name, folder_name))
        self.connection.commit()
    
    def get_person(self, folder_name):
        self.cursor.execute("""
                            SELECT name FROM users WHERE folder_name = (?)
                            """, (folder_name,))
        
        res = self.cursor.fetchone()
        if res:
            return str(res[0])