import os
import json
import uuid
import logging

class StorageHandler:
    def __init__(self, data_dir='./data'):
        """Inicializa el manejador de almacenamiento con un directorio base."""
        self.data_dir = data_dir
        self.index = {}  # Diccionario para almacenar índices de claves
        if not os.path.exists(data_dir):
            logging.info(f"Creando directorio: {data_dir}")
            os.makedirs(data_dir)
        self._load_index()

    def _load_index(self):
        """Carga el índice de claves desde los archivos de metadatos."""
        for file in os.listdir(self.data_dir):
            if file.startswith('meta_') and file.endswith('.json'):
                group = file[5:-5]  # Extrae el group_key del nombre (meta_<group>.json)
                file_path = os.path.join(self.data_dir, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        self.index[group] = json.load(f)
                except Exception as e:
                    logging.error(f"Error al cargar índice {file}: {e}")
        logging.debug(f"Índice cargado: {self.index}")

    def _save_index(self, group_key):
        """Guarda el índice de un group_key en un archivo JSON."""
        if group_key not in self.index or not self.index[group_key]:
            # Si no hay claves, elimina el archivo de metadatos
            meta_file = os.path.join(self.data_dir, f'meta_{group_key}.json')
            if os.path.exists(meta_file):
                try:
                    os.remove(meta_file)
                except Exception as e:
                    logging.error(f"Error al eliminar {meta_file}: {e}")
            return
        try:
            meta_file = os.path.join(self.data_dir, f'meta_{group_key}.json')
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(self.index[group_key], f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.error(f"Error al guardar índice para {group_key}: {e}")

    def _get_file_id(self, key, group_key='default'):
        """Obtiene o genera un ID único para una clave."""
        if group_key not in self.index:
            self.index[group_key] = {}
        if key not in self.index[group_key]:
            file_id = str(uuid.uuid4())
            self.index[group_key][key] = file_id
            self._save_index(group_key)
        return self.index[group_key][key]

    def store(self, key, value, group_key='default'):
        """Almacena un valor para una clave en un grupo específico."""
        file_id = self._get_file_id(key, group_key)
        file_path = os.path.join(self.data_dir, f'data_{file_id}.json')
        try:
            data = {'value': value}
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
            logging.debug(f"Almacenado {key} en {file_path}")
        except Exception as e:
            logging.error(f"Error al almacenar {key} en {group_key}: {e}")

    def add(self, key, value, group_key='default'):
        """Agrega un valor a una clave existente, concatenándolo como lista."""
        file_id = self._get_file_id(key, group_key)
        file_path = os.path.join(self.data_dir, f'data_{file_id}.json')
        try:
            # Leer datos existentes
            data = {'value': []}
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            # Asegurarse de que 'value' sea una lista
            if not isinstance(data['value'], list):
                data['value'] = [data['value']]
            data['value'].append(value)
            # Guardar datos actualizados
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
            logging.debug(f"Agregado {value} a {key} en {file_path}")
        except Exception as e:
            logging.error(f"Error al agregar {value} a {key} en {group_key}: {e}")

    def retrieve(self, key, group_key='default'):
        """Recupera el valor asociado a una clave."""
        if group_key not in self.index or key not in self.index[group_key]:
            return ''
        file_id = self.index[group_key][key]
        file_path = os.path.join(self.data_dir, f'data_{file_id}.json')
        try:
            if not os.path.exists(file_path):
                return ''
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data['value']
        except Exception as e:
            logging.error(f"Error al recuperar {key} de {group_key}: {e}")
            return ''

    def list_keys(self, prefix='', group_key=None):
        """Lista las claves que coinciden con un prefijo y grupo."""
        result = []
        try:
            groups = [group_key] if group_key else self.index.keys()
            for group in groups:
                for key in self.index.get(group, {}):
                    if key.startswith(prefix):
                        result.append((key, group))
            return result
        except Exception as e:
            logging.error(f"Error al listar claves con prefijo {prefix}: {e}")
            return []

    def remove_keys(self, prefix='', group_key='default'):
        """Elimina claves que coinciden con un prefijo en un grupo."""
        try:
            keys_to_remove = [(k, g) for k, g in self.list_keys(prefix, group_key)]
            for key, group in keys_to_remove:
                file_id = self.index[group][key]
                file_path = os.path.join(self.data_dir, f'data_{file_id}.json')
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    del self.index[group][key]
                    logging.debug(f"Eliminada clave {key} de {group}")
                except Exception as e:
                    logging.error(f"Error al eliminar {key} de {group}: {e}")
            self._save_index(group_key)
        except Exception as e:
            logging.error(f"Error al eliminar claves con prefijo {prefix} en {group_key}: {e}")