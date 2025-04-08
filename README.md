# Documentación de arquitectura

## Tabla de contenidos

1. [Vista física](#vista-física)
2. [Vista de desarrollo](#vista-de-desarrollo)
3. [Vista de procesos](#vista-de-procesos)
4. [Vista lógica](#vista-lógica)
5. [Tareas a realizar](#tareas-a-realizar)

## Vista física

### Planteamiento

A partir de un análisis conjunto de todas las consultas (queries) que debíamos resolver, diseñamos un bosquejo que nos permitió visualizar el flujo de datos desde el ingreso del cliente hasta la obtención del resultado final. Identificamos funciones que se repetían en múltiples consultas —como el filtrado por año (> 2000), por país, o el enrutamiento por ID— y decidimos desacoplarlas como servicios independientes para reutilizarlas. Esto nos permitió no solo reducir el procesamiento redundante, sino también mejorar la eficiencia general del sistema al compartir resultados intermedios entre varias ramas de ejecución. A su vez, estructuramos la lógica de forma modular, de manera que cada función pudiera escalarse fácilmente y ser reutilizada en distintos contextos, manteniendo coherencia y simplicidad en el diseño.

![image bosquejo](img/vista_fisica/bosquejo.png)

### Diagrama de despliegue

El **diagrama de despliegue** muestra cómo están distribuidos los diferentes componentes del sistema sobre nodos físicos o virtuales. El sistema está diseñado con una arquitectura **modular, distribuida y escalable**, basada en **microservicios**.

![image despliegue](img/vista_fisica/diagrama_despliegue.png)

#### Flujo General

1. **Cliente**:

   - Se conecta mediante un **socket TCP** al sistema.
   - Envía archivos `.csv` con información de **libros**, **ratings** y **actores**.

2. **Gateway**:

   - Es el punto de entrada al sistema.
   - **Un gateway por cada cliente** en un escenario **multicliente**.
   - Recibe los archivos, los interpreta y los envía al Middleware.

3. **Middleware (RabbitMQ)**:

   - Funciona como **broker de mensajes**.
   - Encargado de distribuir los datos a los distintos componentes del sistema de forma asincrónica.

4. **Componentes del Sistema** (todos son **servicios desacoplados**):
   - **Parser (1 × N nodos)**: Procesa y transforma los datos de los `.csv`.
   - **Router (6 × N nodos)**: Enruta datos según claves específicas.
   - **Filter (5 × N nodos)**: Aplica filtros para mantener solo los paquetes que nos interesan.
   - **Calculator (3 × N nodos)**: Realiza cálculos como sumatorias o conteos.
   - **Joiner (2 × N nodos)**: Une datasets por claves compartidas (ej. ID película).
   - **Sensor (1 × N nodos)**: Analiza sentimientos u otras características del texto utilizando herramientas de procesamiento de lenguaje natural.
   - **Averager (2 nodos)**: Calcula promedios de los sentimientos.
   - **Aggregator / Deliver (5 nodos)**: Junta los resultados finales para entregárselos al cliente.

#### Escalabilidad

- Los componentes con multiplicadores de **N** son **escalables horizontalmente**.
- **`N` representa la cantidad máxima de instancias que el sistema puede levantar** para cada tipo de servicio, según la carga o necesidad.
- Permite paralelizar el procesamiento y mejorar el rendimiento en contextos de alta concurrencia.

### Diagrama de robustez

Este diagrama muestra el **comportamiento interno del sistema**, dividido por responsabilidades y relaciones entre componentes.

En este diagrama indicamos que hay más de una instancia de una entidad utilizando un asterisco (\*).

![image robustez](img/vista_fisica/diagrama_robustez.png)

#### Componentes Funcionales

**Escalables (\*)**:

- **Parser**: Parsea los CSV en datos procesables para los otros nodos.
- **Filter**: Filtra datos por condiciones descartando las que no cumplen.
- **Router**: Direcciona el flujo de datos según distintas claves.
- **Calculator**: Realiza sumatorias de valores, conteos de elementos entre otras cosas.
- **Joiner**: Relaciona diferentes fuentes de datos con una clave común (ID película, etc.).
- **Sensor**: Aplica análisis de sentimientos sobre texto.

**No-Escalables**:

- **Averager**: Aplica promedios a todo un flujo de información. No escala ya que para hacer un promedio tenes que crear una columna de conteo para despues juntar todos los promedios parciales, entonces preferimos que se haga el promedio total directo separando el promedio positivo del negativo en 2 nodos.
- **Aggregator**: Selecciona y ordena los datos finales ademas de agrupar resultados parciales y componer una respuesta completa si es necesario.

### Conclusiones

- La arquitectura está pensada para ser **altamente escalable y distribuida**.
- Se pueden levantar múltiples instancias de cada componente según la necesidad (**N escalable**).

## Vista de desarrollo

### Diagrama de componentes

### Diagrama de paquetes

## Vista de procesos

### Diagrama de secuencia

### Diagramas de actividades

#### Consulta 1

![image actividades consulta 1](img/vista_procesos/actividad_1.png)

#### Consulta 2

![image actividades consulta 2](img/vista_procesos/actividad_2.png)

#### Consulta 3

![image actividades consulta 3](img/vista_procesos/actividad_3.png)

#### Consulta 4

Para resolver esta consulta planteamos una especie de "sharding" de registros, para poder realizar el JOIN de películas y actores de forma distribuida. Esto sucede en los enrutadores de actores y películas. Cada componente se encarga de "mandar" cada registro a su cola correspondiente.

Este "sharding" nos asegura que los actores y películas que comparten `id_pelicula` (la joining key) van a terminar guardados en la misma cola (en realidad son colas distintas pero tienen el mismo id).

Para implementar el sharding de registros en los enrutadores de películas y de actores, se podría hashear el `id_pelicula` de los registros, y calcular el módulo `hash(id_pelicula) % n` (siendo `n` la cantidad de nodos) para determinar el id de la cola en la cual hay que guardar los registros.

![image actividades consulta 4](img/vista_procesos/actividad_4.png)

#### Consulta 5

![image actividades consulta 5](img/vista_procesos/actividad_5.png)

## Vista lógica

### Diagrama de clases

### Diagrama de estados

## Tareas a realizar
