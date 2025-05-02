import orjson
from dataclasses import dataclass, field
import time

FINAL = "FINAL"

@dataclass
class Packet:
    timestamp: str
    def to_json(self):
        return orjson.dumps(self.__dict__)

    @classmethod
    def from_json(cls, data):
        return cls(**orjson.loads(data))

@dataclass
class DataPacket(Packet):
    data: dict
    
    def __init__(self, timestamp: str, data: dict, keep_columns: list = None):
        """
        Initialize DataPacket with filtered data based on keep_columns.
        
        Args:
            timestamp (str): ISO timestamp.
            data (dict): Input data dictionary.
            keep_columns (list, optional): List of columns to keep in data. If None, keep all.
        """
        super().__init__(timestamp=timestamp)
        self.data = keep_columns_from(data, keep_columns) if keep_columns else data

    def to_json(self):
        return orjson.dumps(self.__dict__)

    @classmethod
    def from_json(cls, data):
        return cls(**orjson.loads(data))

# Definiendo FinalPacket
@dataclass  
class FinalPacket:  
    type: str = FINAL
    def __init__(self, client_id: int):
        self.client_id = client_id
        self.header = FINAL

    def to_json(self):
        return orjson.dumps(self.__dict__)

    @classmethod
    def from_json(cls, data):
        # Parsear el JSON y añadir el valor "FINAL" al campo type
        data_dict = orjson.loads(data)
        data_dict["header"] = FINAL
        return cls(**data_dict)
    
@dataclass
class QueryPacket(Packet):
    response: str

    def to_json(self):
        return orjson.dumps(self.__dict__)

    @classmethod
    def from_json(cls, data):
        return cls(**orjson.loads(data))
    
def is_final_packet(header):
    if header == FINAL:
        return True
    return False

def is_eof_packet(header):
    return header == "EOF"

def handle_final_packet(method, rabbitmq_instance):
        print(" [!] Final packet received. Stopping consumption and sending FINAL PACKET until no consumers...")
        return rabbitmq_instance.send_final_until_no_consumers(method)


def keep_columns_from(data: dict, columns: list) -> dict:
    """
    Keep only specified keys in a dictionary and return the modified dictionary.
    
    Args:
        data (dict): The input dictionary (e.g., movie data).
        columns (list): List of keys to keep in the dictionary.
    
    Returns:
        dict: A new dictionary with only the specified keys.
    """
    return {k: v for k, v in data.items() if k in columns}
     