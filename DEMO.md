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
