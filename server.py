import base64
import logging 

from concurrent import futures
import os
import grpc
from recognition import DeepFaceBackend
import cv2
import numpy as np

from futuracommon.protos import imageservice_pb2, imageservice_pb2_grpc
from futuracommon.protos import nlp_pb2, nlp_pb2_grpc
from futuracommon.protos import healthcheck_pb2, healthcheck_pb2_grpc

from futuracommon.SessionManager import RedisSessionManager

logging.basicConfig(level=logging.DEBUG, format='%(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('IMAGESERVICE')

DB_FACES_PATH = 'faces'
REDIS_HOST = os.environ.get("REDIS_HOST", "session_manager")
REDIS_PORT = 6379
REDIS_DB = 0

IMAGE_SERVICE_ADDR = '[::]:50050'
NLP_SERVICE_ADDR = f'{os.environ.get("NLP_SERVICE_HOST", "nlpservice:50050")}'

dfBackend = DeepFaceBackend(DB_FACES_PATH)
sessionManager = RedisSessionManager(REDIS_HOST, REDIS_PORT, REDIS_DB)

class ImageStreamServicer(imageservice_pb2_grpc.ImageStreamServiceServicer):
    def SendImages(self, request_iterator, context):
        # channel = grpc.insecure_channel(NLP_SERVICE_ADDR)
        # stub = nlp_pb2_grpc.NLPServiceStub(channel)
        
        for request in request_iterator:
            nparr = np.frombuffer(request.image_base64, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            sessionManager.save(cliend_id=request.client_id, key="last_face", value=base64.b64encode(nparr.tobytes()))
            
            indentity = dfBackend.find(img)
            logger.info(indentity)
            
            if indentity and sessionManager.get_all(client_id=request.client_id).get('identity') != indentity:
                # stub.NotifySuccess(nlp_pb2.SuccessNotification(client_id=request.client_id))
                sessionManager.save(cliend_id=request.client_id, key="identity", value=indentity)
            
        channel.close()
        
        return imageservice_pb2.StreamSummary(total_images_received=1)
    
    def SaveFace(self, request, context):
        
        last_image = sessionManager.get_all(request.client_id).get('last_face', None)
        if not last_image:
            return imageservice_pb2.SaveAck(ack=True)
        
        nparr = np.frombuffer(base64.b64decode(last_image), np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        dfBackend.add_face(img, request.identity)
        
        return imageservice_pb2.SaveAck(ack=True)
        

class HealthServicer(healthcheck_pb2_grpc.HealthServiceServicer):
    def Check(self, request, context):
        
        return healthcheck_pb2.HealthResponse(status=1, current_backend=dfBackend.name)
 

def serve():
    logger.info("Starting image service")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    imageservice_pb2_grpc.add_ImageStreamServiceServicer_to_server(ImageStreamServicer(), server)
    healthcheck_pb2_grpc.add_HealthServiceServicer_to_server(HealthServicer(), server)
    
    server.add_insecure_port(IMAGE_SERVICE_ADDR)
    server.start()
    logger.info("Listening...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()