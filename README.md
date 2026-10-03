- Planificador Dieciochero

Tarea 1 de Sistemas Operativos. El programa implementa un planificador de actividades modeladas como un DAG usando procesos Unix, pipes y señales.

Cada actividad se ejecuta como un proceso hijo y el programa respeta un limite maximo de concurrencia K. Una actividad solo puede comenzar cuando todas sus dependencias hayan terminado correctamente.

- Compilacion

gcc -Wall -Wextra -std=c17 -lpthread -o planificador planificador.c

- Ejecucion

./planificador plan.txt K

plan.txt corresponde al archivo con las actividades.
K corresponde a la cantidad maxima de procesos que pueden ejecutarse al mismo tiempo.

Ejemplo:

./planificador plan_10.txt 3

- Formato del archivo plan.txt

Cada linea debe tener este formato:

ID_Actividad : Nombre_Actividad : tiempo_ms : Dependencia1, Dependencia2

Los identificadores se almacenan como texto, por lo que pueden ser numericos o alfanumericos.

Si el tiempo viene vacio, el programa asigna automaticamente una duracion aleatoria entre 100 y 5000 milisegundos.

- Estructura del programa

Primero se encuentran las librerias, constantes, estructuras y declaraciones de funciones.
Despues se encuentra main, que coordina el flujo general del programa.
Finalmente se encuentran las implementaciones de las funciones.

El flujo principal es:

validar argumentos
leer K
instalar manejo de SIGINT
cargar plan.txt
construir y validar el DAG
ejecutar el planificador
mostrar resumen
liberar memoria

- Estructuras principales

- Actividad

Representa una actividad del plan.
Guarda el ID, nombre, tiempo, dependencias, sucesores, cantidad de dependencias pendientes, estado y PID cuando se encuentra en ejecucion.

Los estados posibles son:

EST_PENDIENTE
EST_LISTA
EST_EJECUTANDO
EST_TERMINADA
EST_FALLIDA
EST_ABORTADA

- Plan

Guarda el arreglo dinamico de actividades, la cantidad de actividades y la capacidad reservada.

- EntradaId

Permite relacionar cada ID con la posicion de su actividad dentro del arreglo principal.

- HijoActivo

Guarda la informacion de un proceso que se encuentra ejecutandose, incluyendo su PID, la actividad asociada y el descriptor de lectura de su pipe.

- Lectura y parseo del archivo

El programa lee el archivo linea por linea.
Cada linea se separa en ID, nombre, tiempo y dependencias.

Las actividades se almacenan en un arreglo dinamico que aumenta su capacidad usando realloc cuando es necesario.

Las dependencias tambien se guardan dinamicamente porque cada actividad puede tener una cantidad distinta.

Funciones principales de esta parte:

cargar_plan
parsear_linea
parsear_tiempo
parsear_dependencias
agregar_dependencia_id
agregar_actividad_plan

- Construccion del DAG

Despues de cargar todas las actividades se construyen las relaciones entre ellas.

Se validan IDs duplicados, dependencias inexistentes, dependencias hacia la misma actividad, dependencias repetidas y ciclos.

Tambien se guardan los sucesores de cada actividad para poder actualizar rapidamente las actividades dependientes cuando una termina o falla.

Para validar que no existan ciclos se utiliza el algoritmo de Kahn.

Funcion principal:

construir_y_validar_dag

- Creacion de procesos

Cada actividad lista puede ejecutarse si existe un cupo disponible segun el limite K.

La creacion de procesos se realiza usando fork.

El hijo espera el tiempo correspondiente a la actividad, envia un mensaje por pipe y termina.

El padre guarda el PID y sigue administrando el resto de las actividades.

Funcion principal:

lanzar_actividad

- Control de concurrencia

El programa nunca mantiene mas de K actividades ejecutandose al mismo tiempo.

Cuando no existe espacio para crear otro proceso, el padre utiliza waitpid para esperar que termine alguno de los hijos.

De esta forma no se utiliza busy waiting.

El proceso padre mantiene el estado global del planificador para evitar modificaciones simultaneas sobre las mismas estructuras.

Funcion principal:

ejecutar_plan

- Pipes

Antes de crear cada proceso hijo se crea un pipe asociado a esa actividad.

Cuando el hijo termina correctamente envia un mensaje de finalizacion al padre.

El padre recibe ese mensaje y actualiza las dependencias pendientes de las actividades sucesoras.

Si una actividad queda con cero dependencias pendientes pasa al estado EST_LISTA.

Funciones principales:

escribir_mensaje_hijo
leer_mensaje_pipe
procesar_exito

- Manejo de errores

Si una actividad falla, se marca como EST_FALLIDA.

Las actividades que dependan directa o indirectamente de ella se marcan como EST_ABORTADA.

Las ramas independientes del DAG siguen ejecutandose normalmente.

Una actividad puede considerarse fallida si termina con un codigo distinto de cero, termina por una señal, existe un problema con su pipe o no se puede crear correctamente el proceso.

Funciones principales:

marcar_fallo_y_propagar
abortar_descendientes

- Manejo de Ctrl+C

El programa utiliza SIGINT para detectar Ctrl+C.

Cuando se recibe la señal, el planificador cancela las actividades activas, marca como abortadas las actividades que no terminaron, recolecta los hijos y libera los recursos antes de terminar.

Funciones principales:

manejar_sigint
instalar_sigint
cancelar_todo
cerrar_y_recolectar_hijos

- Salida del programa

Durante la ejecucion se muestran los inicios y terminos de las actividades, los mensajes enviados por pipes y los posibles fallos o abortos.

Al terminar se muestra un resumen con la cantidad total de actividades, terminadas, fallidas y abortadas.

- Pruebas realizadas

Se probaron archivos con 10, 100 y 10000 actividades.

El archivo de 10000 actividades se utiliza como prueba de carga para comprobar que el programa puede manejar el maximo solicitado por la tarea.

- Decisiones de diseño

- Un proceso por actividad

Cada actividad se representa mediante un proceso hijo mientras se encuentra ejecutandose.

- El padre administra el estado global

Los procesos hijos solo simulan el trabajo y envian el mensaje de termino. El proceso padre actualiza el DAG y los estados.

- Arreglos dinamicos

Las actividades, dependencias y sucesores utilizan memoria dinamica para no limitar artificialmente la cantidad de elementos.

- Sucesores almacenados

Cada actividad guarda tambien las actividades que dependen de ella. Esto permite actualizar solo la parte necesaria del grafo cuando una actividad termina o falla.

- Espera bloqueante

Se utiliza waitpid para esperar procesos y evitar busy waiting.

- Pipes por actividad

Cada hijo utiliza su propio pipe para comunicar su finalizacion al proceso padre.


