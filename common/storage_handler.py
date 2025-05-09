import os
import json
import uuid
import logging

class StorageHandler:
    def __init__(self, data_dir='./data'):
        """Inicializa el manejador de almacenamiento con un directorio base."""
        self.data_dir = data_dir
        self.index = {}  # Diccionario para almacenar índices de claves {key: file_id}
        if not os.path.exists(data_dir):
            logging.info(f"Creando directorio: {data_dir}")
            os.makedirs(data_dir)
        self._load_index()

    def _load_index(self):
        """Carga el índice de claves desde el archivo de metadatos."""
        meta_file = os.path.join(self.data_dir, 'meta.json')
        if os.path.exists(meta_file):
            try:
                with open(meta_file, 'r', encoding='utf-8') as f:
                    self.index = json.load(f)
                logging.debug(f"Índice cargado: {self.index}")
            except Exception as e:
                logging.error(f"Error al cargar índice {meta_file}: {e}")
        else:
            logging.debug("No se encontró archivo de metadatos, inicializando índice vacío")

    def _save_index(self):
        """Guarda el índice en un archivo JSON."""
        meta_file = os.path.join(self.data_dir, 'meta.json')
        if not self.index:
            # Si el índice está vacío, elimina el archivo de metadatos
            if os.path.exists(meta_file):
                try:
                    os.remove(meta_file)
                    logging.debug(f"Archivo de metadatos {meta_file} eliminado")
                except Exception as e:
                    logging.error(f"Error al eliminar {meta_file}: {e}")
            return
        try:
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(self.index, f, ensure_ascii=False, indent=2)
            logging.debug(f"Índice guardado en {meta_file}")
        except Exception as e:
            logging.error(f"Error al guardar índice: {e}")

    def _get_file_id(self, key):
        """Obtiene o genera un ID único para una clave."""
        if key not in self.index:
            file_id = str(uuid.uuid4())
            self.index[key] = file_id
            self._save_index()
        return self.index[key]

    def store(self, key, value):
        """Almacena un valor para una clave."""
        file_id = self._get_file_id(key)
        file_path = os.path.join(self.data_dir, f'data_{file_id}.json')
        try:
            data = {'value': value}
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
            logging.debug(f"Almacenado {key} en {file_path}")
        except Exception as e:
            logging.error(f"Error al almacenar {key}: {e}")

    def add(self, key, value):
        """Agrega un valor a una clave existente, concatenándolo como lista."""
        file_id = self._get_file_id(key)
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
            logging.error(f"Error al agregar {value} a {key}: {e}")

    def retrieve(self, key):
        """Recupera el valor asociado a una clave."""
        if key not in self.index:
            return ''
        file_id = self.index[key]
        file_path = os.path.join(self.data_dir, f'data_{file_id}.json')
        try:
            if not os.path.exists(file_path):
                return ''
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data['value']
        except Exception as e:
            logging.error(f"Error al recuperar {key}: {e}")
            return ''

    def list_keys(self, prefix=''):
        """Lista las claves que coinciden con un prefijo."""
        try:
            return [key for key in self.index if key.startswith(prefix)]
        except Exception as e:
            logging.error(f"Error al listar claves con prefijo {prefix}: {e}")
            return []

    def clean(self, prefix=''):
        """Elimina claves que coinciden con un prefijo."""
        try:
            keys_to_remove = self.list_keys(prefix)
            for key in keys_to_remove:
                file_id = self.index[key]
                file_path = os.path.join(self.data_dir, f'data_{file_id}.json')
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    del self.index[key]
                    logging.debug(f"Eliminada clave {key}")
                except Exception as e:
                    logging.error(f"Error al eliminar {key}: {e}")
            self._save_index()
        except Exception as e:
            logging.error(f"Error al eliminar claves con prefijo {prefix}: {e}")

    def clean_all(self):
        """Elimina todos los datos almacenados en el disco y reinicia el índice."""
        try:
            # Eliminar todos los archivos de datos asociados a las claves
            for key in list(self.index.keys()):
                file_id = self.index[key]
                file_path = os.path.join(self.data_dir, f'data_{file_id}.json')
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    del self.index[key]
                    logging.debug(f"Eliminada clave {key}")
                except Exception as e:
                    logging.error(f"Error al eliminar {key}: {e}")

            # Reiniciar el índice
            self.index = {}
            self._save_index()

            # Eliminar cualquier archivo data_*.json residual
            for file_name in os.listdir(self.data_dir):
                if file_name.startswith('data_') and file_name.endswith('.json'):
                    file_path = os.path.join(self.data_dir, file_name)
                    try:
                        print(f"Borrando archivo residual {file_path}")
                        os.remove(file_path)
                    except Exception as e:
                        logging.error(f"Error al eliminar archivo residual {file_path}: {e}")

            logging.info("Todos los datos en el disco y el índice han sido eliminados.")
        except Exception as e:
            logging.error(f"Error al limpiar todos los datos: {e}")