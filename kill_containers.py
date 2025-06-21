import random
import subprocess
import time
import yaml


def get_services_to_kill() -> list[str]:
    """
    Returns the services that can be killed (does notn include rabbitmq, gateway and join nodes)
    """
    docker_compose_file = "docker-compose-gen.yaml"
    with open(docker_compose_file, "r", encoding="utf-8") as f:
        data = f.read()
    compose = yaml.safe_load(data)
    services = list(compose["services"].keys())

    services.remove("rabbitmq")
    services.remove("gateway")
    # Loop in reverse so that elements can be removed on the run
    for i, service in reversed(list(enumerate(services))):
        # Remove clients and join
        if service.startswith("client") or service.startswith("join"):
            services.pop(i)
    print(f"services = {services}")
    return services


def kill_containers_randomly(services: list[str]):
    """
    Loops killing the containers given by the 'service' list randomly
    """
    services_len = len(services)
    while True:
        random_index = random.randint(0, services_len - 1)
        container_to_kill = services[random_index]
        # Reduce the probability of killing a sentiment node to 10%
        if container_to_kill.startswith("sentiment"):
            if random.randint(1, 100) > 10:
                continue
        docker_kill_command = f"docker kill {container_to_kill}".split(" ")
        try:
            subprocess.run(docker_kill_command, check=True)
        except Exception as e:
            print(f"Failed to kill container {container_to_kill}. Error: {e}")
        time.sleep(0.2)


def main():
    containers_to_kill = get_services_to_kill()
    kill_containers_randomly(containers_to_kill)


if __name__ == "__main__":
    main()
