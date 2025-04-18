#!/usr/bin/env python3

import sys
import yaml

def agregar_join_nodes(compose, num_nodes):
    for i in range(num_nodes):
        join_name = f"join_{i}"
        compose['services'][join_name] = {
            'build': {
                'context': '.',
                'dockerfile': 'join/Dockerfile'
            },
            'container_name': join_name,
            'depends_on': {
                'rabbitmq': {'condition': 'service_healthy'}
            },
            'networks': ['app-network'],
            'environment': [
                'PYTHONUNBUFFERED=1',
                f'NODE_ID={i}',
                'RABBITMQ_QUEUE_1=movie_queue',
                'RABBITMQ_QUEUE_2=secondary_queue',
                'RABBITMQ_OUTPUT_QUEUE=query_queue',
                'RABBITMQ_FINAL_QUEUE=final_queue',
                'RABBITMQ_EXCHANGE_1=movies_router_exchange',
                'RABBITMQ_EXCHANGE_2=movies_router_exchange',
                'RABBITMQ_EXCHANGE_TYPE=direct',
                'RABBITMQ_CONSUMER_TAG=join'
            ]
        }

def agregar_router(compose, num_nodes):
    compose['services']['router'] = {
        'build': {
            'context': '.',
            'dockerfile': 'router/Dockerfile'
        },
        'depends_on': {
            'rabbitmq': {'condition': 'service_healthy'}
        },
        'networks': ['app-network'],
        'environment': [
            'PYTHONUNBUFFERED=1',
            'RABBITMQ_QUEUE=router_queue',
            'RABBITMQ_EXCHANGE=argentina_exchange',
            'RABBITMQ_CONSUMER_TAG=router',
            'RABBITMQ_OUTPUT_EXCHANGE=movies_router_exchange',
            f'NUMBER_OF_NODES={num_nodes}'
        ]
    }

def main(input_file, output_file, num_nodes_str):
    try:
        num_nodes = int(num_nodes_str)
        if num_nodes < 1:
            raise ValueError("Debe haber al menos 1 nodo join.")
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    if not input_file.endswith(('.yml', '.yaml')) or not output_file.endswith(('.yml', '.yaml')):
        print("Error: los archivos deben tener extensión .yml o .yaml")
        sys.exit(1)

    try:
        with open(input_file, 'r') as f:
            compose = yaml.safe_load(f) or {}
    except Exception as e:
        print(f"Error al leer el archivo '{input_file}': {e}")
        sys.exit(1)

    compose.setdefault('services', {})
    compose.setdefault('networks', {'app-network': {'driver': 'bridge'}})

    agregar_join_nodes(compose, num_nodes)
    agregar_router(compose, num_nodes)

    try:
        with open(output_file, 'w') as f:
            yaml.dump(compose, f, default_flow_style=False, sort_keys=False)
        print(f"Archivo Docker Compose actualizado con {num_nodes} nodos join y el router en {output_file}")
    except Exception as e:
        print(f"Error al escribir el archivo '{output_file}': {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f"Uso: {sys.argv[0]} <archivo-entrada> <archivo-salida> <num-nodos-join>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2], sys.argv[3])