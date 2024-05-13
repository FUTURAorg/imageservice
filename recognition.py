import uuid
import cv2
from deepface import DeepFace
import os

from pathlib import Path

import numpy as np
import logging

import sqlite3

from db import PostgreSQLRep

logger = logging.getLogger('IMAGESERVICE')


class RecognitionBackend:
    
    def recognize(self, image):
        raise NotImplementedError

    def add_face(self, image):
        raise NotImplementedError


class DeepFaceBackend(RecognitionBackend):
    
    def __init__(self, db_folder):
        self.db_folder = db_folder
        self.repository = PostgreSQLRep(os.environ.get('DB_URL'))
        self.max_pictures = 10
        self.name = "deepface"
        
        if not os.path.exists(self.db_folder):
            os.makedirs(db_folder)
    
    def add_face(self, image: np.ndarray, name: str):      
        try:
            face_objs = DeepFace.extract_faces(image, detector_backend="opencv")
        except ValueError as e:
            logger.error(f"Error while extracting face {e}")
            return None

        face, conf = face_objs[0]['face'], face_objs[0]['confidence']
        logger.info(f"Recognized a face with {conf*100}% confidence")
        
        uid = self.__find_uid(image)
        
        if uid and len(os.listdir(os.path.join(self.db_folder, uid))) == self.max_pictures:
            return None
        
        if uid is None:
            uid = str(uuid.uuid4())
            self.repository.create(name, uid)
        
        person_path = os.path.join(self.db_folder, uid)
        
        if not os.path.exists(person_path):
            os.makedirs(person_path)
            
        full_path = os.path.join(person_path, f"{uuid.uuid4()}.jpg")
        
        if face.dtype != np.uint8 or face.max() <= 1:
            face = (face * 255).astype(np.uint8)
            face = face[...,::-1]
        
        cv2.imwrite(full_path, face)
        
        logger.info(f"Added new face for {name} with filename {full_path}")
        
    
    def __find_uid(self, image: np.ndarray):
        try:
            search_res = DeepFace.find(image, db_path=self.db_folder, model_name="Facenet", silent=True)
            if not search_res[0].empty:
                res = search_res[0].iloc[0]['identity']
                uid = res.split(os.sep)[1]         
                return uid
            
        except ValueError as e:
            logger.error(f"Error while face find {e}")
            return None
       
       
    def find(self, image: np.ndarray):
        uid = self.__find_uid(image)
        if uid:    
            res = self.repository.get_person(uid)       
            return res
       
        
if __name__ == "__main__":
    repo = SQLiteRep("test.db")
    repo.create_tables()
    print(repo.get_person("get"))