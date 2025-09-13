import random
import subprocess
import time
import yaml


def get_services_to_kill() -> list[str]:
    """
    Returns the services that can be killed (does not include rabbitmq, gateway and clients)
    """
    docker_compose_file = "docker-compose-gen.yaml"
    with open(docker_compose_file, "r", encoding="utf-8") as f:
        data = f.read()
    compose = yaml.safe_load(data)
    services = list(compose["services"].keys())

    unkillable_services = ["rabbitmq", "control"]

    for service in services:
        if service.startswith("client") or service.startswith("gateway"):
            unkillable_services.append(service)

    for unkillable_service in unkillable_services:
        services.remove(unkillable_service)

    print(f"services = {services}")
    return services

def kill_containers(containers_to_kill: list[str]):
    """
    Kills all the containers in the given list
    """
    for container_to_kill in containers_to_kill:
        docker_kill_command = f"docker kill {container_to_kill}".split(" ")
        try:
            subprocess.run(docker_kill_command, check=True)
        except Exception as e:
            print(f"Failed to kill container {container_to_kill}. Error: {e}")


def main():
    containers_to_kill = get_services_to_kill()
    kill_containers(containers_to_kill)


if __name__ == "__main__":
    main()
