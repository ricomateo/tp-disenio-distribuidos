# Documentación de arquitectura

## Tabla de contenidos

1. [Vista lógica](#vista-lógica)
2. [Vista de desarrollo](#vista-de-desarrollo)
3. [Vista de procesos](#vista-de-procesos)
4. [Vista física](#vista-física)
5. [Tareas a realizar](#tareas-a-realizar)

## Vista lógica

### Diagrama de clases

### Diagrama de estados


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

## Vista física

### Diagrama de despliegue

### Diagrama de robustez

En este diagrama indicamos que hay más de una instancia de una entidad utilizando un asterisco (*).

![image robustez](img/vista_fisica/diagrama_robustez.png)

## Tareas a realizar
