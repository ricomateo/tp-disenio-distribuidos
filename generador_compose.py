import configparser

import yaml
from generator.config_generator import ConfigGenerator


def initialize_config(config_file="config.ini"):
    config = configparser.ConfigParser()
    config.read(config_file)
    

    if not config.has_section("NODES"):
        config.read("docker-compose-generator/config.ini")
    
    if not config.has_section("NODES"):
        raise ValueError("Config file not found")

    config_params = {}
    
    container_keys = [
        "PARSER",
        "FILTER_2000_ARGENTINA",
        "FILTER_2000s_SPAIN",
        "FILTER_UNIQUE_COUNTRY",
        "FILTER_BUDGET_REVENUE",
        "ROUTER_RATINGS",
        "ROUTER_2000_ARGENTINA",
        "ROUTER_ACTORS",
        "ROUTER_COUNTRY",
        "ROUTER_RATINGS_CALCULATED",
        "ROUTER_ACTORS_2000_ARGENTINA",
        "CALCULATOR_BUDGET_COUNTRY",
        "CALCULATOR_COUNT_ACTORS",
        "CALCULATOR_AVERAGE_RATINGS",
        "CALCULATOR_RATIO_FEELINGS",
        "JOIN_MOVIES",
        "SENTIMENT"
    ]
    
    
    for key in container_keys:
        config_params[key.lower()] = int(config["NODES"].get(key, 0))

    
    return config_params


def main():
    config_params = initialize_config()
    generator = ConfigGenerator(config_params)
    docker_compose_config = generator.generate()
    with open("docker-compose-gen.yaml", "w") as f:
        yaml.dump(docker_compose_config, f,
                  sort_keys=False, default_flow_style=False)

    print("Generated docker-compose-gen.yaml")


if __name__ == "__main__":
    main()
