
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
CALCULATOR_BUDGET_COUNTRY = "calculator_budget_country"
CALCULATOR_COUNT_ACTORS = "calculator_count_actors"
CALCULATOR_AVERAGE_RATINGS = "calculator_average_ratings"
CALCULATOR_RATIO_FEELINGS = "calculator_ratio_feelings"
JOIN_MOVIES = "join_movies"
JOIN_RATINGS = "join_ratings"
JOIN_ACTORS = "join_actors"
SENTIMENT = 'sentiment'
SENTIMENT_POSITIVE = 'sentiment_positive_queue'
SENTIMENT_NEGATIVE = 'sentiment_negative_queue'
AGGREGATOR_CALCULATOR_RATIO_FEELINGS = 'aggregator_calculator_ratio_feelings'
AGGREGATOR_CALCULATOR_BUDGET_COUNTRY = 'aggregator_calculator_budget_country' 

class ConfigGenerator:
    def __init__(self, config_params):
        self.config_params = config_params
        self.compose = {
        'networks': {
            'app-network': {'driver': 'bridge'}
        }
    }

    def generate(self) -> dict:
        self._generate_rabbitmq()
        self._generate_client()
        self._generate_input_gateway()
        self._generate_parser()
        self._generate_filters()
        self._generate_routers()
        self._generate_calculators()
        #self._generate_sentiment()
        self._generate_joiners()
        self._generate_aggregators()
        self._generate_deliver_1()
        self._generate_deliver_2()
        self._generate_deliver_3()
        self._generate_deliver_4()
        #self._generate_deliver_5()
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

    def _generate_client(self):
        """Generate client service."""
        #instances = self.config_params.get('CLIENT', 1)
        self.generate_service(
            service_name='client',
            dockerfile='client/Dockerfile',
            environment=[
                'GATEWAY_HOST=gateway',
                'GATEWAY_PORT=9999'
            ],
            networks=['app-network'],
            depends_on={
                'gateway': {'condition': 'service_started'}
            },
            instances=1,
            deploy={'restart_policy': {'condition': 'none'}}
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
                'BATCH_SIZE=100',
                f'RABBITMQ_OUTPUT_QUEUE={GATEWAY}',
                f'RABBITMQ_INPUT_QUEUE={DELIVER}'
            ],
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=instances
        )
        
    def _generate_parser(self):
        instances = self.config_params.get(PARSER, 1)
        self.generate_service(
            service_name=PARSER,
            dockerfile='parser/Dockerfile',
            environment=[
            f'RABBITMQ_QUEUE={GATEWAY}', 
            f'RABBITMQ_OUTPUT_EXCHANGE={PARSER}',
            'KEEP_MOVIES_COLUMNS=budget,genres,id,original_language,overview,production_countries,release_date,revenue,title',
            'KEEP_RATINGS_COLUMNS=userId,movieId,rating',
            'KEEP_CREDITS_COLUMNS=cast,id',
            'REPLACE=movieId:id'
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
                          start_node_id: int = None):
        
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
            instance_suffix = '' if instances_new == 1 else f'_{node_id}'
            service_name_instance = f"{service_name}{instance_suffix}"
            # Initialize environment with mandatory variables
            current_environment = ['PYTHONUNBUFFERED=1']
            current_environment.extend(environment)
            current_environment.append(f'NODE_ID={node_id}')
            current_environment.append(f'CLUSTER_SIZE={instances}')

            

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

            

            # Add depends_on if non-empty
            if depends_on:
                config['depends_on'] = depends_on.copy()

            # Add deploy if provided
            if deploy:
                config['deploy'] = deploy.copy()

            # Add service to compose
            self.compose.setdefault('services', {})[service_name_instance] = config

    def _generate_filter(self, service_name, environment, instances):
        
        self.generate_service(
            service_name=service_name,
            dockerfile='filter/Dockerfile',
            environment=environment,
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=instances
        )
        
    def _generate_router(self, service_name, environment, instances):
        
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
            
    def _generate_calculator(self, service_name, environment, instances, start_node_id=None):
        
        self.generate_service(
            service_name=service_name,
            dockerfile='calculator/Dockerfile',
            environment=environment,
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=instances,
            start_node_id=start_node_id
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
                F'RABBITMQ_QUEUE={FILTER_2000_ARGENTINA}',
                f'RABBITMQ_CONSUMER_TAG={FILTER_2000_ARGENTINA}',
                f'RABBITMQ_OUTPUT_QUEUE={FILTER_2000_ARGENTINA}',
                f'RABBITMQ_EXCHANGE={PARSER}',
                f'RABBITMQ_ROUTING_KEY={MOVIES_FILE}',
                f'RABBITMQ_OUTPUT_EXCHANGE={FILTER_2000_ARGENTINA}',
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
                f'KEEP_COLUMNS=overview,budget,revenue',
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
                f'NUMBER_OF_NODES={self.config_params[CALCULATOR_AVERAGE_RATINGS]}'
            ],
            instances=instances
            )
        
        instances = self.config_params[ROUTER_RATINGS_CALCULATED]
        self._generate_router(
            service_name=ROUTER_RATINGS_CALCULATED,
            environment=[
                f'RABBITMQ_QUEUE={CALCULATOR_AVERAGE_RATINGS}',
                f'RABBITMQ_CONSUMER_TAG={ROUTER_RATINGS_CALCULATED}',
                f'RABBITMQ_OUTPUT_EXCHANGE={ROUTER_RATINGS_CALCULATED}',
                f'NUMBER_OF_NODES={self.config_params[JOIN_MOVIES]}'
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
                f'NUMBER_OF_NODES={self.config_params[CALCULATOR_COUNT_ACTORS]}'
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
                F'RABBITMQ_QUEUE={ROUTER_RATINGS}{CALCULATOR_AVERAGE_RATINGS}',
                f'RABBITMQ_CONSUMER_TAG={CALCULATOR_AVERAGE_RATINGS}',
                f'RABBITMQ_EXCHANGE={ROUTER_RATINGS}',
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
            service_name=CALCULATOR_RATIO_FEELINGS,
            environment=[
                F'RABBITMQ_QUEUE={SENTIMENT_POSITIVE}',
                f'RABBITMQ_CONSUMER_TAG={CALCULATOR_RATIO_FEELINGS}',
                f'RABBITMQ_OUTPUT_QUEUE={CALCULATOR_RATIO_FEELINGS}',
                f'RABBITMQ_FINAL_QUEUE={CALCULATOR_RATIO_FEELINGS}{FINAL}',
                f'OPERATION=ratio_by:revenue,budget'
            ],
            instances=instances
            )
        
        self._generate_calculator(
            service_name=CALCULATOR_RATIO_FEELINGS,
            environment=[
                F'RABBITMQ_QUEUE={SENTIMENT_NEGATIVE}',
                f'RABBITMQ_CONSUMER_TAG={CALCULATOR_RATIO_FEELINGS}',
                f'RABBITMQ_OUTPUT_QUEUE={CALCULATOR_RATIO_FEELINGS}',
                f'RABBITMQ_FINAL_QUEUE={CALCULATOR_RATIO_FEELINGS}{FINAL}',
                f'OPERATION=ratio_by:revenue,budget'
            ],
            instances=instances,
            start_node_id=instances
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
                F'RABBITMQ_QUEUE_2={ROUTER_RATINGS_CALCULATED}{JOIN_RATINGS}',
                f'RABBITMQ_EXCHANGE_2={ROUTER_RATINGS_CALCULATED}',
                f'RABBITMQ_CONSUMER_TAG={JOIN_RATINGS}',
                f'RABBITMQ_OUTPUT_QUEUE={JOIN_RATINGS}',
                f'KEEP_COLUMNS=title,id,average',
                f'JOIN_BY=id',
                f'RABBITMQ_FINAL_QUEUE={JOIN_RATINGS}{FINAL}'
            ],
            instances=instances
            )
        
    def _generate_aggregators(self):
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
    
        
        
            
        
    def _generate_deliver_1(self):
        self.generate_service(
            service_name=QUERY_1,
            dockerfile='deliver/Dockerfile',
            environment=[
                F'RABBITMQ_QUEUE={FILTER_2000S_SPAIN}',
                f'RABBITMQ_CONSUMER_TAG={QUERY_1}',
                f'RABBITMQ_OUTPUT_QUEUE={DELIVER}',
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
        self.generate_service(
            service_name=QUERY_2,
            dockerfile='deliver/Dockerfile',
            environment=[
                F'RABBITMQ_QUEUE={AGGREGATOR_CALCULATOR_BUDGET_COUNTRY}',
                f'RABBITMQ_CONSUMER_TAG={QUERY_2}',
                f'RABBITMQ_OUTPUT_QUEUE={DELIVER}',
                f'RABBITMQ_FINAL_QUEUE={DELIVER}{FINAL}',
                f'QUERY_NUMBER=2',
                f'SORT=total:5',
                f'KEEP_COLUMNS=value,total'
            ],
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=1
        )
    
    def _generate_deliver_3(self):
        self.generate_service(
            service_name=QUERY_3,
            dockerfile='deliver/Dockerfile',
            environment=[
                F'RABBITMQ_QUEUE={JOIN_RATINGS}',
                f'RABBITMQ_CONSUMER_TAG={QUERY_3}',
                f'RABBITMQ_OUTPUT_QUEUE={DELIVER}',
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
        self.generate_service(
            service_name=QUERY_4,
            dockerfile='deliver/Dockerfile',
            environment=[
                F'RABBITMQ_QUEUE={CALCULATOR_COUNT_ACTORS}',
                f'RABBITMQ_CONSUMER_TAG={QUERY_4}',
                f'RABBITMQ_OUTPUT_QUEUE={DELIVER}',
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
        self.generate_service(
            service_name=QUERY_5,
            dockerfile='deliver/Dockerfile',
            environment=[
                F'RABBITMQ_QUEUE={AGGREGATOR_CALCULATOR_RATIO_FEELINGS}',
                f'RABBITMQ_CONSUMER_TAG={QUERY_5}',
                f'RABBITMQ_OUTPUT_QUEUE={DELIVER}',
                f'RABBITMQ_FINAL_QUEUE={DELIVER}{FINAL}',
                f'QUERY_NUMBER=5',
                f'SORT=ratio:2',
                f'KEEP_COLUMNS=feeling,ratio,count'
            ],
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=1
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
                f'RABBITMQ_OUTPUT_QUEUE_NEGATIVE={SENTIMENT_NEGATIVE}'
            ],
            networks=['app-network'],
            depends_on={
                'rabbitmq': {'condition': 'service_healthy'}
            },
            instances=instances
        )