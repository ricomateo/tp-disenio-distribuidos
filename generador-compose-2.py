#!/usr/bin/env python3

import sys
import yaml

def agregar_join_nodes(compose, num_join_nodes):
    """Add specified number of join nodes to the Docker Compose services."""
    for i in range(num_join_nodes):
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

def agregar_routers(compose, num_routers, num_join_nodes):
    """Add specified number of router nodes to the Docker Compose services."""
    for i in range(num_routers):
        router_name = f"router_{i}"
        compose['services'][router_name] = {
            'build': {
                'context': '.',
                'dockerfile': 'router/Dockerfile'
            },
            'container_name': router_name,
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
                f'NUMBER_OF_NODES={num_join_nodes}'
            ]
        }

def agregar_calculators(compose, num_calculator_pairs):
    """Add n*2 calculator nodes (two types per pair) to the Docker Compose services."""
    for i in range(num_calculator_pairs * 2):
        # Even indices (0, 2, ...) are calculator type 1, odd indices (1, 3, ...) are type 2
        calc_type = 1 if i % 2 == 0 else 2
        calc_id = i // 2  # Pair index (e.g., 0 for calc_0 and calc_1)
        calc_name = f"calculator_{i}"
        queue = f"queue{i}" if calc_type == 2 else f"queue{i+1}"
        consumer_tag = f"consumer{i}" if calc_type == 1 else f"consumer{i+1}"
        
        compose['services'][calc_name] = {
            'build': {
                'context': '.',
                'dockerfile': 'calculator/Dockerfile'
            },
            'container_name': calc_name,
            'depends_on': {
                'rabbitmq': {'condition': 'service_healthy'}
            },
            'networks': ['app-network'],
            'environment': [
                'PYTHONUNBUFFERED=1',
                f'RABBITMQ_QUEUE={queue}',
                f'NODE_ID={i}',
                'RABBITMQ_EXCHANGE=country_calculator',
                f'RABBITMQ_CONSUMER_TAG={consumer_tag}',
                'RABBITMQ_OUTPUT_QUEUE=deliver_queue',
                'OPERATION=sum_by:production_countries,budget',
                'RABBITMQ_EXCHANGE_TYPE=direct',
                'RABBITMQ_FINAL_QUEUE=final_queue_country'
            ]
        }

def main(file_path, num_join_nodes_str, num_routers_str, num_calculator_pairs_str):
    try:
        num_join_nodes = int(num_join_nodes_str)
        num_routers = int(num_routers_str)
        num_calculator_pairs = int(num_calculator_pairs_str)
        
        if num_join_nodes < 1:
            raise ValueError("Debe haber al menos 1 nodo join.")
        if num_routers < 1:
            raise ValueError("Debe haber al menos 1 router.")
        if num_calculator_pairs < 1:
            raise ValueError("Debe haber al menos 1 par de calculators (2 nodos).")
            
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    if not file_path.endswith(('.yml', '.yaml')):
        print("Error: el archivo debe tener extensión .yml o .yaml")
        sys.exit(1)

    # Base Docker Compose structure
    compose = {
        'services': {
            'rabbitmq': {
                'image': 'rabbitmq:3.13-management',
                'container_name': 'rabbitmq',
                'ports': ['5672:5672', '15672:15672'],
                'healthcheck': {
                    'test': ['CMD', 'rabbitmqctl', 'status'],
                    'interval': '10s',
                    'timeout': '5s',
                    'retries': 5
                },
                'networks': ['app-network']
            },
            'gateway': {
                'build': {
                    'context': '.',
                    'dockerfile': 'gateway/Dockerfile'
                },
                'container_name': 'gateway',
                'depends_on': {
                    'rabbitmq': {'condition': 'service_healthy'}
                },
                'networks': ['app-network'],
                'environment': [
                    'PYTHONUNBUFFERED=1',
                    'GATEWAY_HOST=0.0.0.0',
                    'GATEWAY_PORT=9999',
                    'BATCH_SIZE=100',
                    'RABBITMQ_OUTPUT_QUEUE=csv_queue',
                    'RABBITMQ_INPUT_QUEUE=query_queue'
                ]
            },
            'parser': {
                'build': {
                    'context': '.',
                    'dockerfile': 'parser/Dockerfile'
                },
                'depends_on': {
                    'rabbitmq': {'condition': 'service_healthy'}
                },
                'networks': ['app-network'],
                'environment': [
                    'PYTHONUNBUFFERED=1',
                    'RABBITMQ_QUEUE=csv_queue',
                    'RABBITMQ_OUTPUT_EXCHANGE=movie_exchange',
                    'KEEP_COLUMNS=budget,genres,id,original_language,overview,production_countries,release_date,revenue,title'
                ]
            },
            'unique_country': {
                'build': {
                    'context': '.',
                    'dockerfile': 'filter/Dockerfile'
                },
                'depends_on': {
                    'rabbitmq': {'condition': 'service_healthy'}
                },
                'networks': ['app-network'],
                'environment': [
                    'PYTHONUNBUFFERED=1',
                    'RABBITMQ_QUEUE=movie2_queue',
                    'RABBITMQ_CONSUMER_TAG=argentina_2',
                    'RABBITMQ_OUTPUT_QUEUE=country_router',
                    'RABBITMQ_EXCHANGE=movie_exchange',
                    'MOVIE_FILTERS=production_countries:count(1)'
                ]
            },
            'deliver': {
                'build': {
                    'context': '.',
                    'dockerfile': 'deliver/Dockerfile'
                },
                'container_name': 'deliver',
                'depends_on': {
                    'rabbitmq': {'condition': 'service_healthy'}
                },
                'networks': ['app-network'],
                'environment': [
                    'PYTHONUNBUFFERED=1',
                    'RABBITMQ_QUEUE=deliver_queue',
                    'RABBITMQ_OUTPUT_QUEUE=query_queue',
                    'SORT=total:5',
                    'KEEP_COLUMNS=value,total'
                ]
            },
            'client': {
                'build': {
                    'context': '.',
                    'dockerfile': 'client/Dockerfile'
                },
                'container_name': 'client',
                'depends_on': ['gateway'],
                'networks': ['app-network'],
                'environment': [
                    'PYTHONUNBUFFERED=1',
                    'GATEWAY_HOST=gateway',
                    'GATEWAY_PORT=9999'
                ],
                'deploy': {
                    'restart_policy': {'condition': 'none'}
                }
            }
        },
        'networks': {
            'app-network': {'driver': 'bridge'}
        }
    }

    # Add dynamic nodes
    agregar_join_nodes(compose, num_join_nodes)
    agregar_routers(compose, num_routers, num_join_nodes)
    agregar_calculators(compose, num_calculator_pairs)

    try:
        with open(file_path, 'w') as f:
            yaml.dump(compose, f, default_flow_style=False, sort_keys=False)
        print(f"Archivo Docker Compose actualizado en {file_path} con {num_join_nodes} nodos join, {num_routers} routers, y {num_calculator_pairs*2} calculators.")
    except Exception as e:
        print(f"Error al escribir el archivo '{file_path}': {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(f"Uso: {sys.argv[0]} <archivo> <num-nodos-join> <num-routers> <num-pares-calculators>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])