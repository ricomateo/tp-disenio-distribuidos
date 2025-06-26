## Bajas al azar

1. Asegurarse de que no hay containers corriendo con:

```bash
make clear
```

2. Iniciar el script que mata containers con

```bash
python3 kill_containers.py
```

3. En otra terminal, levantar el sistema (junto con las pruebas automatizadas) con:

```bash
make test_against_notebook
```

## Duplicación de mensajes

1. Moverse a la rama que duplica mensajes con

```bash
git checkout demo-duplicate-packets
```

2. Asegurarse de que no hay containers corriendo con:

```bash
make clear
```

3. Levantar el sistema (junto con las pruebas automatizadas) con:

```bash
make test_against_notebook
```

En los logs se puede apreciar la duplicación de mensajes mediante el log `[INFO] Sending duplicate packet`

## Caída del clientes / gateway - Elección de líder

1. Moverse a la rama `gateway-crash` con:

```bash
git checkout gateway-crash
```

2. Asegurarse de que no hay containers corriendo con:

```bash
make clear
```

3. Levantar el sistema (junto con las pruebas automatizadas) con:

```bash
make test_against_notebook
```

4. Esperar un minuto y matar al gateway líder con

```bash
sleep 60
docker kill gateway_3
```

5. En los logs del `gateway_3` se puede ver como al reiniciarse se encarga de enviar el paquete `DELETE` para limpiar la información del cliente caído.

```bash
docker logs gateway_3
```

6. Reconectar el cliente con (ahora se le va a asignar el ID 1)

```bash
docker restart client
```

7. Luego de que se procesan los mensajes del cliente 0 (el que se cayó), en los logs de los distintos nodos se puede apreciar que reciben el paquete `DELETE` y borran los datos del cliente 0.

```bash
docker logs calculator_count_actors
```

8. Mientras se realiza el procesamiento de la consulta, se pueden ver los logs de la elección de lider en los nodos gateway con:

```bash
docker logs gateway_2 | grep LEADER_ELECTION
```

```bash
docker logs gateway_1 | grep LEADER_ELECTION
```

```bash
docker logs gateway | grep LEADER_ELECTION
```

## Catástrofe

1. Moverse a la rama `demo-catastrofe` con:

```bash
git checkout demo-catastrofe
```

2. Asegurarse de que no hay containers corriendo con:

```bash
make clear
```

3. Levantar el sistema (junto con las pruebas automatizadas) con:

```bash
make test_against_notebook
```

4. En otra terminal, mostrar el estado de los containers con `docker stats`

```bash
docker stats
```

5. En otra terminal, esperar un minuto y luego matar todos los containers (a excepción de rabbit, los gateways y los clientes)

```bash
sleep
python3 catastrofe.py
```

Esperar a que los clientes terminen
