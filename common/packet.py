import json
from dataclasses import dataclass, field
import time

@dataclass
class Packet:
    #packet_id: str
    timestamp: str

    def to_json(self):
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, data):
        return cls(**json.loads(data))

@dataclass
class DataPacket(Packet):
    data: dict

    def to_json(self):
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, data):
        return cls(**json.loads(data))

@dataclass
class MoviePacket(DataPacket):
    movie: dict

    def to_json(self):
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, data):
        return cls(**json.loads(data))

# Definiendo FinalPacket
@dataclass  
class FinalPacket(Packet):  
    type: str = "FINAL"

    def to_json(self):
        # Agregar `type` al diccionario antes de convertir a JSON
        #print(f"[to_json] timestamp: {self.timestamp}")
        data = self.__dict__.copy()
        data["header"] = "FINAL"
        return json.dumps(data)

    @classmethod
    def from_json(cls, data):
        # Parsear el JSON y añadir el valor "FINAL" al campo type
        data_dict = json.loads(data)
        data_dict["header"] = "FINAL"
        return cls(**data_dict)
    
@dataclass
class QueryPacket(DataPacket):
    response: str

    def to_json(self):
        return json.dumps(self.__dict__)

    @classmethod
    def from_json(cls, data):
        return cls(**json.loads(data))
def is_final_packet(header):
    if header == "FINAL":
        return True
    return False

def handle_final_packet(method, rabbitmq_instance):
        print(" [!] Final packet received. Stopping consumption and sending FINAL PACKET until no consumers...")
        return rabbitmq_instance.send_final_until_no_consumers(method)
     