import json

CLIENTS = 'clients'
FINAL = 'final'
QUERY_1 = 'query_1'
QUERY_2 = 'query_2'
QUERY_3 = 'query_3'
QUERY_4 = 'query_4'
QUERY_5 = 'query_5'
MOVIES_FILE = 'movies_metadata.csv'
RATINGS_FILE = "ratings.csv"
CREDITS_FILE = "credits.csv"
DELIVER = 'deliver'
PARSER = 'parser'
PARSER_MOVIES = 'parser_movies'
PARSER_RATINGS = 'parser_ratings'
PARSER_CREDITS = 'parser_credits'
GATEWAY = 'gateway'
FILTER_2000_ARGENTINA = 'filter_2000_argentina'
FILTER_2000S_SPAIN = 'filter_2000s_spain'
FILTER_UNIQUE_COUNTRY = 'filter_unique_country'
FILTER_BUDGET_REVENUE = 'filter_budget_revenue'
ROUTER_RATINGS = 'router_ratings'
ROUTER_2000_ARGENTINA = "router_2000_argentina"
ROUTER_ACTORS = "router_actors"
ROUTER_COUNTRY = "router_country"
ROUTER_RATINGS_CALCULATED = "router_ratings_calculated"
ROUTER_ACTORS_2000_ARGENTINA = "router_actors_2000_argentina"
ROUTER_POSITIVE_SENTIMENT = "router_positive_sentiment"
ROUTER_NEGATIVE_SENTIMENT = "router_negative_sentiment"
CALCULATOR_BUDGET_COUNTRY = "calculator_budget_country"
CALCULATOR_COUNT_ACTORS = "calculator_count_actors"
CALCULATOR_AVERAGE_RATINGS = "calculator_average_ratings"
CALCULATOR_RATIO_FEELINGS = "calculator_ratio_feelings"
CALCULATOR_RATIO_FEELINGS_POSITIVE = "calculator_ratio_feelings_positive"
CALCULATOR_RATIO_FEELINGS_NEGATIVE = "calculator_ratio_feelings_negative"
JOIN_MOVIES = "join_movies"
JOIN_RATINGS = "join_ratings"
JOIN_ACTORS = "join_actors"
SENTIMENT = 'sentiment'
SENTIMENT_POSITIVE = 'sentiment_positive_queue'
SENTIMENT_NEGATIVE = 'sentiment_negative_queue'
AGGREGATOR_CALCULATOR_RATIO_FEELINGS = 'aggregator_calculator_ratio_feelings'
AGGREGATOR_CALCULATOR_BUDGET_COUNTRY = 'aggregator_calculator_budget_country' 
AGGREGATOR_CALCULATOR_COUNT_ACTORS = 'aggregator_calculator_count_actors' 
CONTROL = 'control'
SLEEP_INTERVAL = '2'
RESTART_INTERVAL = '5'
WORKER_PORT = '9000'
HEALTH_PORT = '10000'

class ConfigGenerator:
    def __init__(self, config_params):
        self.services = {}
        self.config_params = config_params
        self.compose = {
        'networks': {
            'app-network': {'driver': 'bridge'}
        }
    }

    def generate(self) -> dict:
        self._generate_rabbitmq()
        self._generate_input_gateway()
        self._generate_parser()
        self._generate_filters()
        self._generate_routers()
        self._generate_calculators()
        self._generate_sentiment()
        self._generate_joiners()
        self._generate_deliver_1()
        self._generate_deliver_2()
        self._generate_deliver_3()
        self._generate_deliver_4()
        self._generate_deliver_5()
        self._generate_controls()
        self._generate_clients()
        return self.compose
    
    def _generate_rabbitmq(self):
        """Generate RabbitMQ service."""
        config = {
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
        }
        self.compose.setdefault('services', {})['rabbitmq'] = config
        self.services['rabbitmq'] = {'condition': 'service_healthy'}

    def _generate_clients(self):
        """Generate client service with dependencies on all other services."""
        instances = self.config_params.get(CLIENTS)
        gateway_instances = self.config_params.get(GATEWAY)

        gateways = []
        for i in range(gateway_instances):
            gateway_host = f"gateway_{i}"
            if i == 0:
                gateway_host = "gateway"
            gateways.append(gateway_host)


        movies_file = self.config_params["movies_file"]
        ratings_file = self.config_params["ratings_file"]
        credits_file = self.config_params["credits_file"]
        
        depends_on = {
            service_name: condition.copy()
            for service_name, condition in self.services.items()
            if not service_name.startswith('client')
        }

        self.generate_service(
            service_name='client',
            dockerfile='client/Dockerfile',
            environment=[
                f'GATEWAY_HOSTS={json.dumps(gateways)}',
                'GATEWAY_PORT=9999',
                'BATCH_SIZE=1000'
            ],
            networks=['app-network'],
            depends_on=depends_on,
            instances=instances,
            deploy={'restart_policy': {'condition': 'none'}},
            volumes=[
                f"./{movies_file}:/src/movies_metadata.csv",
                f"./{ratings_file}:/src/ratings.csv",
                f"./{credits_file}:/src/credits.csv"
            ]
        )

    def _generate_input_gateway(self):
        """Generate gateway service."""
        instances = self.config_params.get('gateway', 1)
        self.generate_service(
            service_name=GATEWAY,
            dockerfile='gateway/Dockerfile',
            environment=[
                'GATEWAY_HOST=0.0.0.0',
                'GATEWAY_PORT=9999',
                f'RABBITMQ_INPUT_QUEUE={DELIVER}',
                f'RABBITMQ_EXCHANGE={DELIVER}',
                f'RABBITMQ_OUTPUT_EXCHANGE={GATEWAY}'
            ],
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=instances
        )
        
    def _generate_parser(self):
        instances = self.config_params.get(PARSER_MOVIES)
        self.generate_service(
            service_name=f"{PARSER_MOVIES}",
            dockerfile='parser/Dockerfile',
            environment=[
            f'RABBITMQ_QUEUE={GATEWAY}',
            f'RABBITMQ_EXCHANGE={GATEWAY}', 
            f'RABBITMQ_OUTPUT_EXCHANGE={PARSER}',
            'KEEP_COLUMNS=budget,genres,id,original_language,overview,production_countries,release_date,revenue,title',
            f'HEALTH_SERVER_PORT={HEALTH_PORT}',
            f'HEALTH_SERVER_IP=0.0.0.0',
            f'WORKER_PORT={WORKER_PORT}',
            f'FILENAME={MOVIES_FILE}'
            ],
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=instances
        )
        
        instances = self.config_params.get(PARSER_RATINGS)
        self.generate_service(
            service_name=f"{PARSER_RATINGS}",
            dockerfile='parser/Dockerfile',
            environment=[
            f'RABBITMQ_QUEUE={GATEWAY}',
            f'RABBITMQ_EXCHANGE={GATEWAY}', 
            f'RABBITMQ_OUTPUT_EXCHANGE={PARSER}',
            'KEEP_COLUMNS=userId,movieId,rating',    
            f'FILENAME={RATINGS_FILE}',
            f'HEALTH_SERVER_PORT={HEALTH_PORT}',
            f'HEALTH_SERVER_IP=0.0.0.0',
            f'WORKER_PORT={WORKER_PORT}',
            'REPLACE=movieId:id'
            ],
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=instances
        )
        
        instances = self.config_params.get(PARSER_CREDITS)
        self.generate_service(
            service_name=f"{PARSER_CREDITS}",
            dockerfile='parser/Dockerfile',
            environment=[
            f'RABBITMQ_QUEUE={GATEWAY}',
            f'RABBITMQ_EXCHANGE={GATEWAY}', 
            f'RABBITMQ_OUTPUT_EXCHANGE={PARSER}',
            'KEEP_COLUMNS=cast,id',
            f'HEALTH_SERVER_PORT={HEALTH_PORT}',
            f'HEALTH_SERVER_IP=0.0.0.0',
            f'WORKER_PORT={WORKER_PORT}',
            f'FILENAME={CREDITS_FILE}'
            ],
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=instances
        )

    def generate_service(self,
                          service_name: str,
                          dockerfile: str,
                          environment: list[str],
                          networks: list[str] = None,
                          depends_on: list[str] = [],
                          instances: int = 1,
                          deploy: dict = None,
                          start_node_id: int = None,
                          cluster_size: int = None,
                          volumes: list[str] = None):
        
        if environment is None:
            environment = []
        if networks is None:
            networks = ['app-network']
        if depends_on is None:
            depends_on = {}
       


        for instance_id in range(instances):
            # Generate instance-specific service name
            instances_new = start_node_id + instances if start_node_id is not None else instances
            node_id = start_node_id + instance_id if start_node_id is not None else instance_id
            instance_suffix = '' if node_id == 0 else f'_{node_id}'
            service_name_instance = f"{service_name}{instance_suffix}"
            
            condition = {'condition': 'service_started'}
            self.services[service_name_instance] = condition
            # Initialize environment with mandatory variables
            current_environment = ['PYTHONUNBUFFERED=1']
            current_environment.extend(environment)
            current_environment.append(f'NODE_ID={node_id}')
            current_environment.append(f'CLUSTER_SIZE={cluster_size if cluster_size is not None else instances}')

            

            # Build service configuration
            config = {
                'networks': networks.copy()
            }

            # Set build or image
            if dockerfile:
                config['build'] = {
                    'context': '.',
                    'dockerfile': dockerfile
                }
            else:
                raise ValueError("Either 'dockerfile' or 'image' must be provided")

            # Add container_name
            config['container_name'] = service_name_instance

            # Add environment if non-empty
            if current_environment:
                config['environment'] = current_environment

            # Add volume in case the service is the client
            if service_name == 'client':
                config['volumes'] = ['./output:/app/output']

            # Add depends_on if non-empty
            if depends_on:
                config['depends_on'] = depends_on.copy()

            # Add deploy if provided
            if deploy:
                config['deploy'] = deploy.copy()

            if volumes:
                config['volumes'] = config.get('volumes', []) + volumes.copy()

            # Add service to compose
            self.compose.setdefault('services', {})[service_name_instance] = config

    def _generate_filter(self, service_name, environment, instances):
        updated_environment = environment.copy() if environment else []
       
        updated_environment.extend([
            f'HEALTH_SERVER_PORT={HEALTH_PORT}',
            f'HEALTH_SERVER_IP=0.0.0.0',
            f'WORKER_PORT={WORKER_PORT}'
        ])
        
        self.generate_service(
            service_name=service_name,
            dockerfile='filter/Dockerfile',
            environment=updated_environment,
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=instances
        )
        
    def _generate_router(self, service_name, environment, instances):
        updated_environment = environment.copy() if environment else []
       
        updated_environment.extend([
            f'HEALTH_SERVER_PORT={HEALTH_PORT}',
            f'HEALTH_SERVER_IP=0.0.0.0',
            f'WORKER_PORT={WORKER_PORT}'
        ])
        
        self.generate_service(
            service_name=service_name,
            dockerfile='router/Dockerfile',
            environment=environment,
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=instances
        )
        
    def _generate_join(self, service_name, environment, instances):
        
        self.generate_service(
            service_name=service_name,
            dockerfile='join/Dockerfile',
            environment=environment,
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=instances
        )
            
    def _generate_calculator(self, service_name, environment, instances, start_node_id=None, cluster_size=None):
        
        self.generate_service(
            service_name=service_name,
            dockerfile='calculator/Dockerfile',
            environment=environment,
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=instances,
            start_node_id=start_node_id,
            cluster_size=cluster_size
        )
        
    def _generate_aggregator(self, service_name, environment, instances):
        
        self.generate_service(
            service_name=service_name,
            dockerfile='aggregator/Dockerfile',
            environment=environment,
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=instances,
        )
        
    def _generate_filters(self):
        instances = self.config_params[FILTER_2000_ARGENTINA]
        self._generate_filter(
            service_name=FILTER_2000_ARGENTINA,
            environment=[
                F'RABBITMQ_QUEUE={PARSER}{FILTER_2000_ARGENTINA}',
                f'RABBITMQ_CONSUMER_TAG={FILTER_2000_ARGENTINA}',
                f'RABBITMQ_OUTPUT_QUEUE={FILTER_2000_ARGENTINA}',
                f'RABBITMQ_EXCHANGE={PARSER}',
                f'RABBITMQ_ROUTING_KEY={MOVIES_FILE}',
                f'RABBITMQ_OUTPUT_EXCHANGE={FILTER_2000_ARGENTINA}',
                f'RABBITMQ_FINAL_QUEUE={FILTER_2000_ARGENTINA}{FINAL}',
                f'KEEP_COLUMNS=production_countries,release_date,title,genres,id',
                'FILTERS=production_countries:in(Argentina);release_date:more_date(1999)'
            ],
            instances=instances
            )
        
        instances = self.config_params[FILTER_2000S_SPAIN]
        self._generate_filter(
            service_name=FILTER_2000S_SPAIN,
            environment=[
                f'RABBITMQ_QUEUE={FILTER_2000_ARGENTINA}{FILTER_2000S_SPAIN}',
                f'RABBITMQ_CONSUMER_TAG={FILTER_2000S_SPAIN}',
                f'RABBITMQ_OUTPUT_QUEUE={FILTER_2000S_SPAIN}',
                f'RABBITMQ_FINAL_QUEUE={FILTER_2000S_SPAIN}{FINAL}',
                f'RABBITMQ_EXCHANGE={FILTER_2000_ARGENTINA}',
                f'KEEP_COLUMNS=title,genres,id',
                'FILTERS=production_countries:in(Spain);release_date:less_date(2010)'
            ],
            instances=instances
            )
        
        instances = self.config_params[FILTER_UNIQUE_COUNTRY]
        self._generate_filter(
            service_name=FILTER_UNIQUE_COUNTRY,
            environment=[
                f'RABBITMQ_QUEUE={PARSER}{FILTER_UNIQUE_COUNTRY}',
                f'RABBITMQ_CONSUMER_TAG={FILTER_UNIQUE_COUNTRY}',
                f'RABBITMQ_OUTPUT_QUEUE={FILTER_UNIQUE_COUNTRY}',
                f'RABBITMQ_EXCHANGE={PARSER}',
                f'RABBITMQ_ROUTING_KEY={MOVIES_FILE}',
                f'RABBITMQ_FINAL_QUEUE={FILTER_UNIQUE_COUNTRY}{FINAL}',
                f'KEEP_COLUMNS=production_countries,budget,id',
                'FILTERS=production_countries:count(1)'
            ],
            instances=instances
            )
        
        instances = self.config_params[FILTER_BUDGET_REVENUE]
        self._generate_filter(
            service_name=FILTER_BUDGET_REVENUE,
            environment=[
                f'RABBITMQ_QUEUE={PARSER}{FILTER_BUDGET_REVENUE}',
                f'RABBITMQ_CONSUMER_TAG={FILTER_BUDGET_REVENUE}',
                f'RABBITMQ_OUTPUT_QUEUE={FILTER_BUDGET_REVENUE}',
                f'RABBITMQ_EXCHANGE={PARSER}',
                f'RABBITMQ_ROUTING_KEY={MOVIES_FILE}',
                f'RABBITMQ_FINAL_QUEUE={FILTER_BUDGET_REVENUE}{FINAL}',
                f'KEEP_COLUMNS=overview,budget,revenue,id',
                'FILTERS=budget:more(0);revenue:more(0)'
            ],
            instances=instances
            )
      
 
    def _generate_routers(self):
        instances = self.config_params[ROUTER_COUNTRY]
        self._generate_router(
            service_name=ROUTER_COUNTRY,
            environment=[
                F'RABBITMQ_QUEUE={FILTER_UNIQUE_COUNTRY}',
                f'RABBITMQ_CONSUMER_TAG={ROUTER_COUNTRY}',
                f'RABBITMQ_OUTPUT_EXCHANGE={ROUTER_COUNTRY}',
                f'ROUTER_BY=id',
                f'NUMBER_OF_NODES={self.config_params[CALCULATOR_BUDGET_COUNTRY]}'
            ],
            instances=instances
            )
        
        instances = self.config_params[ROUTER_2000_ARGENTINA]
        self._generate_router(
            service_name=ROUTER_2000_ARGENTINA,
            environment=[
                f'RABBITMQ_QUEUE={FILTER_2000_ARGENTINA}{ROUTER_2000_ARGENTINA}',
                f'RABBITMQ_CONSUMER_TAG={ROUTER_2000_ARGENTINA}',
                f'RABBITMQ_OUTPUT_EXCHANGE={ROUTER_2000_ARGENTINA}',
                f'RABBITMQ_EXCHANGE={FILTER_2000_ARGENTINA}',
                f'ROUTER_BY=id',
                f'NUMBER_OF_NODES={self.config_params[JOIN_MOVIES]}'
            ],
            instances=instances
            )
        
        instances = self.config_params[ROUTER_ACTORS]
        self._generate_router(
            service_name=ROUTER_ACTORS,
            environment=[
                f'RABBITMQ_QUEUE={PARSER}{ROUTER_ACTORS}',
                f'RABBITMQ_CONSUMER_TAG={ROUTER_ACTORS}',
                f'RABBITMQ_OUTPUT_EXCHANGE={ROUTER_ACTORS}',
                f'RABBITMQ_EXCHANGE={PARSER}',
                f'RABBITMQ_ROUTING_KEY={CREDITS_FILE}',
                f'ROUTER_BY=id',
                f'NUMBER_OF_NODES={self.config_params[JOIN_MOVIES]}'
            ],
            instances=instances
            )
        
        instances = self.config_params[ROUTER_RATINGS]
        self._generate_router(
            service_name=ROUTER_RATINGS,
            environment=[
                f'RABBITMQ_QUEUE={PARSER}{ROUTER_RATINGS}',
                f'RABBITMQ_CONSUMER_TAG={ROUTER_RATINGS}',
                f'RABBITMQ_OUTPUT_EXCHANGE={ROUTER_RATINGS}',
                f'RABBITMQ_EXCHANGE={PARSER}',
                f'RABBITMQ_ROUTING_KEY={RATINGS_FILE}',
                f'ROUTER_BY=id',
                f'NUMBER_OF_NODES={self.config_params[JOIN_MOVIES]}'
            ],
            instances=instances
            )
        
        instances = self.config_params[ROUTER_RATINGS_CALCULATED]
        self._generate_router(
            service_name=ROUTER_RATINGS_CALCULATED,
            environment=[
                f'RABBITMQ_QUEUE={JOIN_RATINGS}',
                f'RABBITMQ_CONSUMER_TAG={ROUTER_RATINGS_CALCULATED}',
                f'RABBITMQ_OUTPUT_EXCHANGE={ROUTER_RATINGS_CALCULATED}',
                f'ROUTER_BY=id',
                f'NUMBER_OF_NODES={self.config_params[CALCULATOR_AVERAGE_RATINGS]}'
            ],
            instances=instances
            )
        
        instances = self.config_params[ROUTER_ACTORS_2000_ARGENTINA]
        self._generate_router(
            service_name=ROUTER_ACTORS_2000_ARGENTINA,
            environment=[
                f'RABBITMQ_QUEUE={JOIN_ACTORS}',
                f'RABBITMQ_CONSUMER_TAG={ROUTER_ACTORS_2000_ARGENTINA}',
                f'RABBITMQ_OUTPUT_EXCHANGE={ROUTER_ACTORS_2000_ARGENTINA}',
                f'ROUTER_BY=id',
                f'NUMBER_OF_NODES={self.config_params[CALCULATOR_COUNT_ACTORS]}'
            ],
            instances=instances
            )
        
        instances = self.config_params[ROUTER_POSITIVE_SENTIMENT]
        self._generate_router(
            service_name=ROUTER_POSITIVE_SENTIMENT,
            environment=[
                f'RABBITMQ_QUEUE={SENTIMENT_POSITIVE}',
                f'RABBITMQ_CONSUMER_TAG={ROUTER_POSITIVE_SENTIMENT}',
                f'RABBITMQ_OUTPUT_EXCHANGE={ROUTER_POSITIVE_SENTIMENT}',
                f'ROUTER_BY=id',
                f'NUMBER_OF_NODES={self.config_params[CALCULATOR_RATIO_FEELINGS]}'
            ],
            instances=instances
            )
        
        instances = self.config_params[ROUTER_NEGATIVE_SENTIMENT]
        self._generate_router(
            service_name=ROUTER_NEGATIVE_SENTIMENT,
            environment=[
                f'RABBITMQ_QUEUE={SENTIMENT_NEGATIVE}',
                f'RABBITMQ_CONSUMER_TAG={ROUTER_NEGATIVE_SENTIMENT}',
                f'RABBITMQ_OUTPUT_EXCHANGE={ROUTER_NEGATIVE_SENTIMENT}',
                f'ROUTER_BY=id',
                f'NUMBER_OF_NODES={self.config_params[CALCULATOR_RATIO_FEELINGS]}'
            ],
            instances=instances
            )
        
       
        
    def _generate_calculators(self):
        instances = self.config_params[CALCULATOR_BUDGET_COUNTRY]
        self._generate_calculator(
            service_name=CALCULATOR_BUDGET_COUNTRY,
            environment=[
                F'RABBITMQ_QUEUE={ROUTER_COUNTRY}{CALCULATOR_BUDGET_COUNTRY}',
                f'RABBITMQ_CONSUMER_TAG={CALCULATOR_BUDGET_COUNTRY}',
                f'RABBITMQ_EXCHANGE={ROUTER_COUNTRY}',
                f'RABBITMQ_OUTPUT_QUEUE={CALCULATOR_BUDGET_COUNTRY}',
                f'RABBITMQ_FINAL_QUEUE={CALCULATOR_BUDGET_COUNTRY}{FINAL}',
                f'OPERATION=sum_by:production_countries,budget'
            ],
            instances=instances
            )
        
        instances = self.config_params[CALCULATOR_AVERAGE_RATINGS]
        self._generate_calculator(
            service_name=CALCULATOR_AVERAGE_RATINGS,
            environment=[
                F'RABBITMQ_QUEUE={ROUTER_RATINGS_CALCULATED}{CALCULATOR_AVERAGE_RATINGS}',
                f'RABBITMQ_CONSUMER_TAG={CALCULATOR_AVERAGE_RATINGS}',
                f'RABBITMQ_EXCHANGE={ROUTER_RATINGS_CALCULATED}',
                f'RABBITMQ_OUTPUT_QUEUE={CALCULATOR_AVERAGE_RATINGS}',
                f'RABBITMQ_FINAL_QUEUE={CALCULATOR_AVERAGE_RATINGS}{FINAL}',
                f'OPERATION=average_by:id,rating'
            ],
            instances=instances
            )
        
        instances = self.config_params[CALCULATOR_COUNT_ACTORS]
        self._generate_calculator(
            service_name=CALCULATOR_COUNT_ACTORS,
            environment=[
                F'RABBITMQ_QUEUE={ROUTER_ACTORS_2000_ARGENTINA}{CALCULATOR_COUNT_ACTORS}',
                f'RABBITMQ_CONSUMER_TAG={CALCULATOR_COUNT_ACTORS}',
                f'RABBITMQ_EXCHANGE={ROUTER_ACTORS_2000_ARGENTINA}',
                f'RABBITMQ_OUTPUT_QUEUE={CALCULATOR_COUNT_ACTORS}',
                f'RABBITMQ_FINAL_QUEUE={CALCULATOR_COUNT_ACTORS}{FINAL}',
                f'OPERATION=count_by:cast'
            ],
            instances=instances
            )
        
        instances = self.config_params[CALCULATOR_RATIO_FEELINGS]
        self._generate_calculator(
            service_name=CALCULATOR_RATIO_FEELINGS_POSITIVE,
            environment=[
                F'RABBITMQ_QUEUE={ROUTER_POSITIVE_SENTIMENT}{CALCULATOR_RATIO_FEELINGS}',
                f'RABBITMQ_CONSUMER_TAG={CALCULATOR_RATIO_FEELINGS}',
                f'RABBITMQ_EXCHANGE={ROUTER_POSITIVE_SENTIMENT}',
                f'RABBITMQ_OUTPUT_QUEUE={CALCULATOR_RATIO_FEELINGS}',
                f'RABBITMQ_FINAL_QUEUE={CALCULATOR_RATIO_FEELINGS}{FINAL}',
                f'OPERATION=ratio_by:revenue,budget'
            ],
            instances=instances,
            cluster_size=instances*2
            )
        
        self._generate_calculator(
            service_name=CALCULATOR_RATIO_FEELINGS_NEGATIVE,
            environment=[
                F'RABBITMQ_QUEUE={ROUTER_NEGATIVE_SENTIMENT}{CALCULATOR_RATIO_FEELINGS}',
                f'RABBITMQ_CONSUMER_TAG={CALCULATOR_RATIO_FEELINGS}',
                f'RABBITMQ_EXCHANGE={ROUTER_NEGATIVE_SENTIMENT}',
                f'RABBITMQ_OUTPUT_QUEUE={CALCULATOR_RATIO_FEELINGS}',
                f'RABBITMQ_FINAL_QUEUE={CALCULATOR_RATIO_FEELINGS}{FINAL}',
                f'OPERATION=ratio_by:revenue,budget',
                f'NODE_ID_DUPLICATE=true'
            ],
            instances=instances,
            cluster_size=instances*2
            )
        
    def _generate_joiners(self):
        instances = self.config_params[JOIN_MOVIES]
        self._generate_join(
            service_name=JOIN_ACTORS,
            environment=[
                F'RABBITMQ_QUEUE_1={ROUTER_2000_ARGENTINA}{JOIN_ACTORS}',
                f'RABBITMQ_EXCHANGE_1={ROUTER_2000_ARGENTINA}',
                F'RABBITMQ_QUEUE_2={ROUTER_ACTORS}{JOIN_ACTORS}',
                f'RABBITMQ_EXCHANGE_2={ROUTER_ACTORS}',
                f'RABBITMQ_CONSUMER_TAG={JOIN_ACTORS}',
                f'RABBITMQ_OUTPUT_QUEUE={JOIN_ACTORS}',
                f'KEEP_COLUMNS=title,id,cast',
                f'JOIN_BY=id',
                f'RABBITMQ_FINAL_QUEUE={JOIN_ACTORS}{FINAL}'
            ],
            instances=instances
            )
        
        self._generate_join(
            service_name=JOIN_RATINGS,
            environment=[
                F'RABBITMQ_QUEUE_1={ROUTER_2000_ARGENTINA}{JOIN_RATINGS}',
                f'RABBITMQ_EXCHANGE_1={ROUTER_2000_ARGENTINA}',
                F'RABBITMQ_QUEUE_2={ROUTER_RATINGS}{JOIN_RATINGS}',
                f'RABBITMQ_EXCHANGE_2={ROUTER_RATINGS}',
                f'RABBITMQ_CONSUMER_TAG={JOIN_RATINGS}',
                f'RABBITMQ_OUTPUT_QUEUE={JOIN_RATINGS}',
                f'KEEP_COLUMNS=title,id,rating',
                f'JOIN_BY=id',
                f'RABBITMQ_FINAL_QUEUE={JOIN_RATINGS}{FINAL}'
            ],
            instances=instances
            )
            
        
    def _generate_deliver_1(self):
        self.generate_service(
            service_name=QUERY_1,
            dockerfile='deliver/Dockerfile',
            environment=[
                F'RABBITMQ_QUEUE={FILTER_2000S_SPAIN}',
                f'RABBITMQ_CONSUMER_TAG={QUERY_1}',
                f'RABBITMQ_OUTPUT_EXCHANGE={DELIVER}',
                f'RABBITMQ_FINAL_QUEUE={DELIVER}{FINAL}',
                f'QUERY_NUMBER=1',
                f'KEEP_COLUMNS=title,genres'
            ],
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=1
        )
        
    def _generate_deliver_2(self):
        self._generate_aggregator(
            service_name=AGGREGATOR_CALCULATOR_BUDGET_COUNTRY,
            environment=[
                F'RABBITMQ_QUEUE={CALCULATOR_BUDGET_COUNTRY}',
                f'RABBITMQ_CONSUMER_TAG={AGGREGATOR_CALCULATOR_BUDGET_COUNTRY}',
                f'RABBITMQ_OUTPUT_QUEUE={AGGREGATOR_CALCULATOR_BUDGET_COUNTRY}',
                'operation=total_invested'
            ],
            instances=1
            )
        
        self.generate_service(
            service_name=QUERY_2,
            dockerfile='deliver/Dockerfile',
            environment=[
                F'RABBITMQ_QUEUE={AGGREGATOR_CALCULATOR_BUDGET_COUNTRY}',
                f'RABBITMQ_CONSUMER_TAG={QUERY_2}',
                f'RABBITMQ_OUTPUT_EXCHANGE={DELIVER}',
                f'RABBITMQ_FINAL_QUEUE={DELIVER}{FINAL}',
                f'QUERY_NUMBER=2',
                f'SORT=total:5',
                f'KEEP_COLUMNS=value,total'
            ],
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=1,
        )
    
    def _generate_deliver_3(self):
        self.generate_service(
            service_name=QUERY_3,
            dockerfile='deliver/Dockerfile',
            environment=[
                F'RABBITMQ_QUEUE={CALCULATOR_AVERAGE_RATINGS}',
                f'RABBITMQ_CONSUMER_TAG={QUERY_3}',
                f'RABBITMQ_OUTPUT_EXCHANGE={DELIVER}',
                f'RABBITMQ_FINAL_QUEUE={DELIVER}{FINAL}',
                f'QUERY_NUMBER=3',
                f'SORT=average:1,average:-1',
                f'KEEP_COLUMNS=id,title,average'
            ],
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=1
        )
        
    def _generate_deliver_4(self):
        self._generate_aggregator(
            service_name=AGGREGATOR_CALCULATOR_COUNT_ACTORS,
            environment=[
                F'RABBITMQ_QUEUE={CALCULATOR_COUNT_ACTORS}',
                f'RABBITMQ_CONSUMER_TAG={AGGREGATOR_CALCULATOR_COUNT_ACTORS}',
                f'RABBITMQ_OUTPUT_QUEUE={AGGREGATOR_CALCULATOR_COUNT_ACTORS}',
                'operation=count'
            ],
            instances=1
            )
          
        self.generate_service(
            service_name=QUERY_4,
            dockerfile='deliver/Dockerfile',
            environment=[
                F'RABBITMQ_QUEUE={AGGREGATOR_CALCULATOR_COUNT_ACTORS}',
                f'RABBITMQ_CONSUMER_TAG={QUERY_4}',
                f'RABBITMQ_OUTPUT_EXCHANGE={DELIVER}',
                f'RABBITMQ_FINAL_QUEUE={DELIVER}{FINAL}',
                f'QUERY_NUMBER=4',
                f'SORT=count:10',
                f'KEEP_COLUMNS=value,count'
            ],
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=1
        )
        
    def _generate_deliver_5(self):
        self._generate_aggregator(
            service_name=AGGREGATOR_CALCULATOR_RATIO_FEELINGS,
            environment=[
                F'RABBITMQ_QUEUE={CALCULATOR_RATIO_FEELINGS}',
                f'RABBITMQ_CONSUMER_TAG={AGGREGATOR_CALCULATOR_RATIO_FEELINGS}',
                f'RABBITMQ_OUTPUT_QUEUE={AGGREGATOR_CALCULATOR_RATIO_FEELINGS}',
                'operation=average'
            ],
            instances=1
            )
        
        self.generate_service(
            service_name=QUERY_5,
            dockerfile='deliver/Dockerfile',
            environment=[
                F'RABBITMQ_QUEUE={AGGREGATOR_CALCULATOR_RATIO_FEELINGS}',
                f'RABBITMQ_CONSUMER_TAG={QUERY_5}',
                f'RABBITMQ_OUTPUT_EXCHANGE={DELIVER}',
                f'RABBITMQ_FINAL_QUEUE={DELIVER}{FINAL}',
                f'QUERY_NUMBER=5',
                f'SORT=ratio:2',
                f'KEEP_COLUMNS=feeling,ratio,count'
            ],
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=1,
            cluster_size=5
        )

    def _generate_sentiment(self):
        instances = self.config_params[SENTIMENT]
        self.generate_service(
            service_name=SENTIMENT,
            dockerfile='sentiment/Dockerfile',
            environment=[
                f'RABBITMQ_QUEUE={FILTER_BUDGET_REVENUE}',
                f'RABBITMQ_CONSUMER_TAG={SENTIMENT}',
                f'RABBITMQ_OUTPUT_QUEUE_POSITIVE={SENTIMENT_POSITIVE}',
                f'RABBITMQ_OUTPUT_QUEUE_NEGATIVE={SENTIMENT_NEGATIVE}',
                f'HEALTH_SERVER_PORT={HEALTH_PORT}',
                f'HEALTH_SERVER_IP=0.0.0.0',
                f'WORKER_PORT={WORKER_PORT}'
            ],
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=instances
        )
        
    def _generate_control_stateless(self, environment, node_worker, start_node_id=None):
        depends_on = {
            service_name: condition.copy()
            for service_name, condition in self.services.items()
            if not service_name.startswith('control')
        }
        
          
        instances = self.config_params.get(node_worker, 1) 
        
        
        included_containers_list = [
                f'{node_worker}' if i == 0 else f'{node_worker}_{i}'
                for i in range(instances)
        ]

        included_containers_str = ','.join(included_containers_list)

        control_environment = environment + [
            f'INCLUDED_CONTAINERS={included_containers_str}'
        ]

        self.generate_service(
            service_name=CONTROL,
            dockerfile='control/Dockerfile',
            environment=control_environment,
            networks=['app-network'],
            depends_on=depends_on,
            instances=1,
            start_node_id=start_node_id,
            volumes=["/var/run/docker.sock:/var/run/docker.sock"]
        )


    def _generate_controls_stateless(self):
        worker_config = [
            PARSER_CREDITS, PARSER_MOVIES, PARSER_RATINGS,
            FILTER_2000_ARGENTINA, FILTER_2000S_SPAIN, FILTER_UNIQUE_COUNTRY, FILTER_BUDGET_REVENUE,
            ROUTER_RATINGS, ROUTER_2000_ARGENTINA, ROUTER_ACTORS, ROUTER_COUNTRY,
            ROUTER_RATINGS_CALCULATED, ROUTER_ACTORS_2000_ARGENTINA, ROUTER_NEGATIVE_SENTIMENT, ROUTER_POSITIVE_SENTIMENT,
            SENTIMENT
        ]
        
        router_types = {
            ROUTER_RATINGS, ROUTER_2000_ARGENTINA, ROUTER_ACTORS, ROUTER_COUNTRY,
            ROUTER_RATINGS_CALCULATED, ROUTER_ACTORS_2000_ARGENTINA, ROUTER_NEGATIVE_SENTIMENT, ROUTER_POSITIVE_SENTIMENT, SENTIMENT
        }
        
        all_worker_types_in_order = worker_config
        total_controls = len(all_worker_types_in_order) + 1

        for i, worker_type in enumerate(all_worker_types_in_order):
            
            current_node_id = i
            
            next_node_id = (i + 1) % total_controls
            
            environment = [
                f'NODE_NAME={current_node_id}',
                f'NEXT_NODE={next_node_id}', 
                f'HEALTH_SERVER_PORT={HEALTH_PORT}', 
                f'HEALTH_SERVER_IP=0.0.0.0',
                f'WORKER_PORT={WORKER_PORT}', 
                f'SLEEP_INTERVAL={SLEEP_INTERVAL}',
                f'RESTART_INTERVAL={RESTART_INTERVAL}',
                f'ONLY_HEALTHCHECK=0',
            ]
            
            if worker_type in router_types:
                environment.append('ROUTER=true')
            
            self._generate_control_stateless(
                environment=environment,
                node_worker=worker_type, 
                start_node_id=current_node_id
            )
            
        return len(all_worker_types_in_order)
            


    def _generate_controls_gateway(self, total_stateful_nodes):
        included_containers_list = []
        instances = self.config_params.get(GATEWAY)
        included_containers_list.extend(
                    [f'{GATEWAY}' if i == 0 else f'{GATEWAY}_{i}' for i in range(instances)]
        )
        
        included_containers_str = ','.join(included_containers_list)

        current_node_id = total_stateful_nodes 
        next_node_id = 0  

        # Define environment for the single stateful control node
        environment = [
            f'NODE_NAME={current_node_id}',
            f'NEXT_NODE={next_node_id}',
            f'HEALTH_SERVER_PORT={HEALTH_PORT}',
            f'HEALTH_SERVER_IP=0.0.0.0',
            f'SLEEP_INTERVAL={SLEEP_INTERVAL}',
            f'RESTART_INTERVAL={RESTART_INTERVAL}',
            f'ONLY_HEALTHCHECK=1',
            f'LEADER_ELECTION=1',
            f'INCLUDED_CONTAINERS={included_containers_str}'
        ]

        # Generate the single stateful control node
        self._generate_control_stateful(
            environment=environment,
            start_node_id=current_node_id
        )


    def _generate_controls(self):
        total_stateless_nodes = self._generate_controls_stateless()
        # Define stateful worker configuration
        worker_config = [
            JOIN_ACTORS, JOIN_RATINGS, CALCULATOR_AVERAGE_RATINGS, 
            CALCULATOR_BUDGET_COUNTRY, CALCULATOR_COUNT_ACTORS, 
            CALCULATOR_RATIO_FEELINGS,
            AGGREGATOR_CALCULATOR_BUDGET_COUNTRY, AGGREGATOR_CALCULATOR_COUNT_ACTORS, 
            AGGREGATOR_CALCULATOR_RATIO_FEELINGS, DELIVER
        ]
        
        included_containers_list = []
        for worker_type in worker_config:
            if worker_type == DELIVER:
                included_containers_list.extend([QUERY_1, QUERY_2, QUERY_3, QUERY_4, QUERY_5])
            elif worker_type == CALCULATOR_RATIO_FEELINGS:
                instances = self.config_params.get(worker_type)
                included_containers_list.extend(
                    [f'{CALCULATOR_RATIO_FEELINGS_POSITIVE}' if i == 0 else f'{CALCULATOR_RATIO_FEELINGS_POSITIVE}_{i}' for i in range(instances)]
                )
                included_containers_list.extend(
                    [f'{CALCULATOR_RATIO_FEELINGS_NEGATIVE}' if i == 0 else f'{CALCULATOR_RATIO_FEELINGS_NEGATIVE}_{i}' for i in range(instances)]
                )
            else:
                instances = self.config_params.get(JOIN_MOVIES, 1) if worker_type in [JOIN_ACTORS, JOIN_RATINGS] else self.config_params.get(worker_type, 1)
                included_containers_list.extend(
                    [f'{worker_type}' if i == 0 else f'{worker_type}_{i}' for i in range(instances)]
                )

        included_containers_str = ','.join(included_containers_list)
        
        total_stateful_nodes = total_stateless_nodes + 1

        current_node_id = total_stateless_nodes  # Stateful node follows stateless nodes
        next_node_id = total_stateful_nodes  # Connects back to the first stateless node

        # Define environment for the single stateful control node
        environment = [
            f'NODE_NAME={current_node_id}',
            f'NEXT_NODE={next_node_id}',
            f'HEALTH_SERVER_PORT={HEALTH_PORT}',
            f'HEALTH_SERVER_IP=0.0.0.0',
            f'SLEEP_INTERVAL={SLEEP_INTERVAL}',
            f'RESTART_INTERVAL={RESTART_INTERVAL}',
            f'ONLY_HEALTHCHECK=1',
            f'INCLUDED_CONTAINERS={included_containers_str}'
        ]

        # Generate the single stateful control node
        self._generate_control_stateful(
            environment=environment,
            start_node_id=current_node_id
        )
        
        self._generate_controls_gateway(total_stateful_nodes)
        
    def _generate_control_stateful(self, environment, start_node_id=None):
        depends_on = {
            service_name: condition.copy()
            for service_name, condition in self.services.items()
            if not service_name.startswith('control')
        }

        control_environment = environment  

        self.generate_service(
            service_name=CONTROL,
            dockerfile='control/Dockerfile',
            environment=control_environment,
            networks=['app-network'],
            depends_on=depends_on,
            instances=1,
            start_node_id=start_node_id,
            volumes=["/var/run/docker.sock:/var/run/docker.sock"]
        )
