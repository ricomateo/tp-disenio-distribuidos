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

6. Luego de que se procesan los paquetes de datos, en los logs de los distintos nodos se puede apreciar que reciben el paquete `DELETE` y borran los datos del cliente.

```bash
docker logs calculator_count_actors
```

7. Reconectar el cliente con

```bash
docker restart client
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
