#define _POSIX_C_SOURCE 200809L

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define MENSAJE_MAX 256

typedef enum {
    EST_PENDIENTE = 0,
    EST_LISTA,
    EST_EJECUTANDO,
    EST_TERMINADA,
    EST_FALLIDA,
    EST_ABORTADA
} EstadoActividad;

typedef struct {
    char *id;
    char *nombre;
    int tiempo_ms;

    char **dependencias_id;
    size_t num_dependencias;
    size_t cap_dependencias;

    size_t *sucesores;
    size_t num_sucesores;
    size_t cap_sucesores;

    size_t dependencias_pendientes;

    EstadoActividad estado;
    pid_t pid;
} Actividad;

typedef struct {
    Actividad *items;
    size_t cantidad;
    size_t capacidad;
} Plan;

typedef struct {
    const char *id;
    size_t indice;
} EntradaId;

typedef struct {
    pid_t pid;
    size_t indice_actividad;
    int fd_lectura;
} HijoActivo;

static volatile sig_atomic_t g_sigint_recibido = 0;

//Declaración de Funciones
static void manejar_sigint(int signo);

static int instalar_sigint(void);

static char *duplicar_cadena(const char *texto);

static char *trim(char *texto);

static void inicializar_actividad(Actividad *a);

static void liberar_actividad(Actividad *a);

static void liberar_plan(Plan *plan);

static int agregar_dependencia_id(Actividad *a, const char *id);

static int agregar_sucesor(Actividad *a, size_t sucesor);

static int agregar_actividad_plan(Plan *plan, Actividad *nueva);

static int parsear_tiempo(const char *texto, int *resultado);

static int parsear_dependencias(char *campo, Actividad *a);

static int parsear_linea(char *linea, Actividad *a);

static int cargar_plan(const char *ruta, Plan *plan);

static int comparar_entrada_id(const void *a, const void *b);

static ssize_t buscar_indice_id(const EntradaId *indice, size_t cantidad, const char *id);

static int construir_y_validar_dag(Plan *plan);

static int dormir_ms(int milisegundos);

static int escribir_mensaje_hijo(int fd, const char *id);

static ssize_t leer_mensaje_pipe(int fd, char *buffer, size_t tam);

static int encontrar_hijo_activo(const HijoActivo *activos, size_t cantidad, pid_t pid);

static void abortar_descendientes(Plan *plan, size_t origen);

static void marcar_fallo_y_propagar(Plan *plan, size_t idx, const char *motivo);

static int lanzar_actividad(Plan *plan, size_t idx, HijoActivo *activos, size_t *cantidad_activos);

static void procesar_exito(Plan *plan, size_t idx, const char *mensaje, size_t *cola_listas, size_t *fin_cola);

static void cancelar_todo(Plan *plan, HijoActivo *activos, size_t cantidad_activos);

static void cerrar_y_recolectar_hijos(HijoActivo *activos, size_t cantidad_activos);

static int ejecutar_plan(Plan *plan, size_t k);

static int leer_k(const char *texto, size_t *k);

static void imprimir_resumen(const Plan *plan);

//Main
int main(int argc, char *argv[])
{
    if (argc != 3) {
        fprintf(stderr, "Uso: %s plan.txt K\n", argv[0]);
        return EXIT_FAILURE;
    }

    size_t k = 0;
    if (leer_k(argv[2], &k) != 0) {
        fprintf(stderr, "Error: K debe ser un entero positivo\n");
        return EXIT_FAILURE;
    }

    srand((unsigned int)(time(NULL) ^ (unsigned int)getpid()));

    if (instalar_sigint() == -1) {
        perror("sigaction");
        return EXIT_FAILURE;
    }

    Plan plan = {0};

    if (cargar_plan(argv[1], &plan) != 0) {
        liberar_plan(&plan);
        return EXIT_FAILURE;
    }

    if (construir_y_validar_dag(&plan) != 0) {
        liberar_plan(&plan);
        return EXIT_FAILURE;
    }

    printf("Plan cargado: %zu actividades | K=%zu\n", plan.cantidad, k);

    int resultado = ejecutar_plan(&plan, k);
    imprimir_resumen(&plan);

    bool hubo_fallos = false;
    for (size_t i = 0; i < plan.cantidad; i++) {
        if (plan.items[i].estado == EST_FALLIDA ||
            plan.items[i].estado == EST_ABORTADA) {
            hubo_fallos = true;
            break;
        }
    }

    liberar_plan(&plan);

    if (resultado == 130) {
        return 130;
    }

    if (resultado != 0 || hubo_fallos) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

//Implementación de funciones
static void manejar_sigint(int signo)
{
    (void)signo;
    g_sigint_recibido = 1;
}

static int instalar_sigint(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = manejar_sigint;
    sigemptyset(&sa.sa_mask);

    return sigaction(SIGINT, &sa, NULL);
}

static char *duplicar_cadena(const char *texto)
{
    size_t n = strlen(texto) + 1;
    char *copia = malloc(n);
    if (copia == NULL) {
        return NULL;
    }
    memcpy(copia, texto, n);
    return copia;
}

static char *trim(char *texto)
{
    while (isspace((unsigned char)*texto)) {
        texto++;
    }

    if (*texto == '\0') {
        return texto;
    }

    char *fin = texto + strlen(texto) - 1;
    while (fin > texto && isspace((unsigned char)*fin)) {
        fin--;
    }
    fin[1] = '\0';

    return texto;
}

static void inicializar_actividad(Actividad *a)
{
    memset(a, 0, sizeof(*a));
    a->estado = EST_PENDIENTE;
    a->pid = -1;
}

static void liberar_actividad(Actividad *a)
{
    free(a->id);
    free(a->nombre);

    for (size_t i = 0; i < a->num_dependencias; i++) {
        free(a->dependencias_id[i]);
    }
    free(a->dependencias_id);
    free(a->sucesores);

    inicializar_actividad(a);
}

static void liberar_plan(Plan *plan)
{
    if (plan == NULL) {
        return;
    }

    for (size_t i = 0; i < plan->cantidad; i++) {
        liberar_actividad(&plan->items[i]);
    }
    free(plan->items);

    plan->items = NULL;
    plan->cantidad = 0;
    plan->capacidad = 0;
}

static int agregar_dependencia_id(Actividad *a, const char *id)
{
    if (a->num_dependencias == a->cap_dependencias) {
        size_t nueva_cap = (a->cap_dependencias == 0) ? 4 : a->cap_dependencias * 2;
        char **tmp = realloc(a->dependencias_id, nueva_cap * sizeof(*a->dependencias_id));
        if (tmp == NULL) {
            return -1;
        }
        a->dependencias_id = tmp;
        a->cap_dependencias = nueva_cap;
    }

    char *copia = duplicar_cadena(id);
    if (copia == NULL) {
        return -1;
    }

    a->dependencias_id[a->num_dependencias++] = copia;
    return 0;
}

static int agregar_sucesor(Actividad *a, size_t sucesor)
{
    if (a->num_sucesores == a->cap_sucesores) {
        size_t nueva_cap = (a->cap_sucesores == 0) ? 4 : a->cap_sucesores * 2;
        size_t *tmp = realloc(a->sucesores, nueva_cap * sizeof(*a->sucesores));
        if (tmp == NULL) {
            return -1;
        }
        a->sucesores = tmp;
        a->cap_sucesores = nueva_cap;
    }

    a->sucesores[a->num_sucesores++] = sucesor;
    return 0;
}

static int agregar_actividad_plan(Plan *plan, Actividad *nueva)
{
    if (plan->cantidad == plan->capacidad) {
        size_t nueva_cap = (plan->capacidad == 0) ? 16 : plan->capacidad * 2;
        Actividad *tmp = realloc(plan->items, nueva_cap * sizeof(*plan->items));
        if (tmp == NULL) {
            return -1;
        }
        plan->items = tmp;
        plan->capacidad = nueva_cap;
    }

    plan->items[plan->cantidad++] = *nueva;
    inicializar_actividad(nueva);
    return 0;
}

static int parsear_tiempo(const char *texto, int *resultado)
{
    if (*texto == '\0') {
        *resultado = 100 + rand() % 4901; 
        return 0;
    }

    errno = 0;
    char *fin = NULL;
    long valor = strtol(texto, &fin, 10);

    if (errno != 0 || fin == texto) {
        return -1;
    }

    while (isspace((unsigned char)*fin)) {
        fin++;
    }

    if (*fin != '\0' || valor < 0 || valor > INT_MAX) {
        return -1;
    }

    *resultado = (int)valor;
    return 0;
}

static int parsear_dependencias(char *campo, Actividad *a)
{
    campo = trim(campo);
    if (*campo == '\0') {
        return 0;
    }

    if (*campo == '[') {
        campo++;
        campo = trim(campo);

        size_t len = strlen(campo);
        if (len == 0) {
            return -1;
        }

        char *fin = campo + len - 1;
        while (fin >= campo && isspace((unsigned char)*fin)) {
            fin--;
        }
        if (fin < campo || *fin != ']') {
            return -1;
        }
        *fin = '\0';
        campo = trim(campo);

        if (*campo == '\0') {
            return 0;
        }
    }

    char *guardar = NULL;
    for (char *tok = strtok_r(campo, ",", &guardar);
         tok != NULL;
         tok = strtok_r(NULL, ",", &guardar)) {

        tok = trim(tok);
        if (*tok == '\0') {
            return -1;
        }

        if (agregar_dependencia_id(a, tok) != 0) {
            return -1;
        }
    }

    return 0;
}

static int parsear_linea(char *linea, Actividad *a)
{
    char *sep1 = strchr(linea, ':');
    if (sep1 == NULL) {
        return -1;
    }

    char *sep2 = strchr(sep1 + 1, ':');
    if (sep2 == NULL) {
        return -1;
    }

    char *sep3 = strchr(sep2 + 1, ':');
    if (sep3 == NULL) {
        return -1;
    }

    *sep1 = '\0';
    *sep2 = '\0';
    *sep3 = '\0';

    char *id = trim(linea);
    char *nombre = trim(sep1 + 1);
    char *tiempo = trim(sep2 + 1);
    char *dependencias = trim(sep3 + 1);

    if (*id == '\0' || *nombre == '\0') {
        return -1;
    }

    a->id = duplicar_cadena(id);
    a->nombre = duplicar_cadena(nombre);
    if (a->id == NULL || a->nombre == NULL) {
        return -1;
    }

    if (parsear_tiempo(tiempo, &a->tiempo_ms) != 0) {
        return -1;
    }

    if (parsear_dependencias(dependencias, a) != 0) {
        return -1;
    }

    a->dependencias_pendientes = a->num_dependencias;
    return 0;
}

static int cargar_plan(const char *ruta, Plan *plan)
{
    FILE *f = fopen(ruta, "r");
    if (f == NULL) {
        perror("fopen");
        return -1;
    }

    char *linea = NULL;
    size_t capacidad_linea = 0;
    ssize_t leidos;
    size_t numero_linea = 0;

    while ((leidos = getline(&linea, &capacidad_linea, f)) != -1) {
        (void)leidos;
        numero_linea++;

        linea[strcspn(linea, "\r\n")] = '\0';
        char *contenido = trim(linea);

        if (*contenido == '\0') {
            continue;
        }

        Actividad a;
        inicializar_actividad(&a);

        if (parsear_linea(contenido, &a) != 0) {
            fprintf(stderr, "Error de formato en la linea %zu\n", numero_linea);
            liberar_actividad(&a);
            free(linea);
            fclose(f);
            return -1;
        }

        if (agregar_actividad_plan(plan, &a) != 0) {
            fprintf(stderr, "Error: memoria insuficiente al cargar el plan\n");
            liberar_actividad(&a);
            free(linea);
            fclose(f);
            return -1;
        }
    }

    if (ferror(f)) {
        perror("getline");
        free(linea);
        fclose(f);
        return -1;
    }

    free(linea);
    fclose(f);

    if (plan->cantidad == 0) {
        fprintf(stderr, "Error: el plan no contiene actividades\n");
        return -1;
    }

    return 0;
}

static int comparar_entrada_id(const void *a, const void *b)
{
    const EntradaId *ea = a;
    const EntradaId *eb = b;
    return strcmp(ea->id, eb->id);
}

static ssize_t buscar_indice_id(const EntradaId *indice, size_t cantidad, const char *id)
{
    EntradaId clave = { .id = id, .indice = 0 };
    const EntradaId *encontrada = bsearch(&clave, indice, cantidad, sizeof(*indice), comparar_entrada_id);
    if (encontrada == NULL) {
        return -1;
    }
    return (ssize_t)encontrada->indice;
}

static int construir_y_validar_dag(Plan *plan)
{
    const size_t n = plan->cantidad;

    EntradaId *indice = malloc(n * sizeof(*indice));
    if (indice == NULL) {
        return -1;
    }

    for (size_t i = 0; i < n; i++) {
        indice[i].id = plan->items[i].id;
        indice[i].indice = i;
    }

    qsort(indice, n, sizeof(*indice), comparar_entrada_id);

    for (size_t i = 1; i < n; i++) {
        if (strcmp(indice[i - 1].id, indice[i].id) == 0) {
            fprintf(stderr, "Error: ID duplicado '%s'\n", indice[i].id);
            free(indice);
            return -1;
        }
    }

    size_t *marca = calloc(n, sizeof(*marca));
    if (marca == NULL) {
        free(indice);
        return -1;
    }

    for (size_t hijo = 0; hijo < n; hijo++) {
        Actividad *a = &plan->items[hijo];
        const size_t generacion = hijo + 1;

        for (size_t j = 0; j < a->num_dependencias; j++) {
            const char *dep_id = a->dependencias_id[j];
            ssize_t padre_ss = buscar_indice_id(indice, n, dep_id);

            if (padre_ss < 0) {
                fprintf(stderr, "Error: actividad '%s' depende del ID inexistente '%s'\n", a->id, dep_id);
                free(marca);
                free(indice);
                return -1;
            }

            size_t padre = (size_t)padre_ss;
            if (padre == hijo) {
                fprintf(stderr, "Error: actividad '%s' depende de si misma\n", a->id);
                free(marca);
                free(indice);
                return -1;
            }

            if (marca[padre] == generacion) {
                fprintf(stderr, "Error: dependencia duplicada '%s' en actividad '%s'\n", dep_id, a->id);
                free(marca);
                free(indice);
                return -1;
            }
            marca[padre] = generacion;

            if (agregar_sucesor(&plan->items[padre], hijo) != 0) {
                free(marca);
                free(indice);
                return -1;
            }
        }
    }

    free(marca);
    free(indice);

    size_t *indegree = malloc(n * sizeof(*indegree));
    size_t *cola = malloc(n * sizeof(*cola));
    if (indegree == NULL || cola == NULL) {
        free(indegree);
        free(cola);
        return -1;
    }

    size_t inicio = 0;
    size_t fin = 0;

    for (size_t i = 0; i < n; i++) {
        indegree[i] = plan->items[i].num_dependencias;
        if (indegree[i] == 0) {
            cola[fin++] = i;
        }
    }

    size_t visitadas = 0;
    while (inicio < fin) {
        size_t actual = cola[inicio++];
        visitadas++;

        const Actividad *a = &plan->items[actual];
        for (size_t j = 0; j < a->num_sucesores; j++) {
            size_t s = a->sucesores[j];
            if (--indegree[s] == 0) {
                cola[fin++] = s;
            }
        }
    }

    free(indegree);
    free(cola);

    if (visitadas != n) {
        fprintf(stderr, "Error: el archivo contiene un ciclo; no es un DAG\n");
        return -1;
    }

    return 0;
}

static int dormir_ms(int milisegundos)
{
    struct timespec restante;
    restante.tv_sec = milisegundos / 1000;
    restante.tv_nsec = (long)(milisegundos % 1000) * 1000000L;

    while (nanosleep(&restante, &restante) == -1) {
        if (errno == EINTR) {
            continue;
        }
        return -1;
    }

    return 0;
}

static int escribir_mensaje_hijo(int fd, const char *id)
{
    char mensaje[MENSAJE_MAX];
    int largo = snprintf(mensaje, sizeof(mensaje), "FIN:%s", id);
    if (largo < 0) {
        return -1;
    }

    size_t a_escribir = (size_t)largo;
    if (a_escribir >= sizeof(mensaje)) {
        a_escribir = sizeof(mensaje) - 1;
    }

    size_t total = 0;
    while (total < a_escribir) {
        ssize_t n = write(fd, mensaje + total, a_escribir - total);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        total += (size_t)n;
    }

    return 0;
}

static ssize_t leer_mensaje_pipe(int fd, char *buffer, size_t tam)
{
    if (tam == 0) {
        return -1;
    }

    size_t total = 0;
    while (total + 1 < tam) {
        ssize_t n = read(fd, buffer + total, tam - 1 - total);
        if (n == 0) {
            break;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        total += (size_t)n;
    }

    buffer[total] = '\0';
    return (ssize_t)total;
}

static int encontrar_hijo_activo(const HijoActivo *activos, size_t cantidad, pid_t pid)
{
    for (size_t i = 0; i < cantidad; i++) {
        if (activos[i].pid == pid) {
            return (int)i;
        }
    }
    return -1;
}

static void abortar_descendientes(Plan *plan, size_t origen)
{
    const size_t n = plan->cantidad;
    size_t *pila = malloc(n * sizeof(*pila));
    unsigned char *encolada = calloc(n, sizeof(*encolada));

    if (pila == NULL || encolada == NULL) {
        fprintf(stderr, "Advertencia: no se pudo reservar memoria para propagar aborto\n");
        free(pila);
        free(encolada);
        return;
    }

    size_t tope = 0;
    const Actividad *base = &plan->items[origen];
    for (size_t i = 0; i < base->num_sucesores; i++) {
        size_t s = base->sucesores[i];
        if (!encolada[s]) {
            encolada[s] = 1;
            pila[tope++] = s;
        }
    }

    while (tope > 0) {
        size_t idx = pila[--tope];
        Actividad *a = &plan->items[idx];

        if (a->estado == EST_TERMINADA ||
            a->estado == EST_FALLIDA ||
            a->estado == EST_ABORTADA) {
            continue;
        }

        if (a->estado == EST_EJECUTANDO && a->pid > 0) {
            kill(a->pid, SIGTERM);
        }

        a->estado = EST_ABORTADA;
        printf("[ABORTADA] %s (depende de una actividad fallida)\n", a->id);

        for (size_t j = 0; j < a->num_sucesores; j++) {
            size_t s = a->sucesores[j];
            if (!encolada[s]) {
                encolada[s] = 1;
                pila[tope++] = s;
            }
        }
    }

    free(encolada);
    free(pila);
}

static void marcar_fallo_y_propagar(Plan *plan, size_t idx, const char *motivo)
{
    Actividad *a = &plan->items[idx];
    a->estado = EST_FALLIDA;
    a->pid = -1;

    fprintf(stderr, "[FALLIDA] %s: %s\n", a->id, motivo);
    abortar_descendientes(plan, idx);
}

static int lanzar_actividad(Plan *plan, size_t idx, HijoActivo *activos, size_t *cantidad_activos)
{
    int fd[2];
    if (pipe(fd) == -1) {
        marcar_fallo_y_propagar(plan, idx, "no se pudo crear el pipe");
        return -1;
    }

    fflush(NULL);

    pid_t pid = fork();
    if (pid == -1) {
        close(fd[0]);
        close(fd[1]);
        marcar_fallo_y_propagar(plan, idx, "fork() fallo");
        return -1;
    }

    if (pid == 0) {
        struct sigaction sa_default;
        memset(&sa_default, 0, sizeof(sa_default));
        sa_default.sa_handler = SIG_DFL;
        sigemptyset(&sa_default.sa_mask);
        sigaction(SIGINT, &sa_default, NULL);
        sigaction(SIGTERM, &sa_default, NULL);

        close(fd[0]);

        for (size_t i = 0; i < *cantidad_activos; i++) {
            close(activos[i].fd_lectura);
        }

        const Actividad *a = &plan->items[idx];

        if (dormir_ms(a->tiempo_ms) != 0) {
            close(fd[1]);
            _exit(2);
        }

        if (escribir_mensaje_hijo(fd[1], a->id) != 0) {
            close(fd[1]);
            _exit(3);
        }

        close(fd[1]);
        _exit(0);
    }

    close(fd[1]);

    Actividad *a = &plan->items[idx];
    a->estado = EST_EJECUTANDO;
    a->pid = pid;

    activos[*cantidad_activos].pid = pid;
    activos[*cantidad_activos].indice_actividad = idx;
    activos[*cantidad_activos].fd_lectura = fd[0];
    (*cantidad_activos)++;

    printf("[INICIO] %s (%s) pid=%ld tiempo=%dms\n", a->id, a->nombre, (long)pid, a->tiempo_ms);

    return 0;
}

static void procesar_exito(Plan *plan, size_t idx, const char *mensaje, size_t *cola_listas, size_t *fin_cola)
{
    Actividad *a = &plan->items[idx];
    a->estado = EST_TERMINADA;
    a->pid = -1;

    printf("[FIN] %s mensaje_pipe='%s'\n", a->id, mensaje);

    for (size_t j = 0; j < a->num_sucesores; j++) {
        size_t s = a->sucesores[j];
        Actividad *sucesor = &plan->items[s];

        if (sucesor->estado == EST_ABORTADA ||
            sucesor->estado == EST_FALLIDA ||
            sucesor->estado == EST_TERMINADA) {
            continue;
        }

        if (sucesor->dependencias_pendientes > 0) {
            sucesor->dependencias_pendientes--;
        }

        printf("[PIPE] %s -> %s : %s\n",
               a->id,
               sucesor->id,
               mensaje);

        if (sucesor->dependencias_pendientes == 0 &&
            sucesor->estado == EST_PENDIENTE) {
            sucesor->estado = EST_LISTA;
            cola_listas[(*fin_cola)++] = s;
        }
    }
}

static void cancelar_todo(Plan *plan, HijoActivo *activos, size_t cantidad_activos)
{
    for (size_t i = 0; i < cantidad_activos; i++) {
        if (activos[i].pid > 0) {
            kill(activos[i].pid, SIGTERM);
        }
    }

    for (size_t i = 0; i < plan->cantidad; i++) {
        Actividad *a = &plan->items[i];
        if (a->estado != EST_TERMINADA &&
            a->estado != EST_FALLIDA &&
            a->estado != EST_ABORTADA) {
            a->estado = EST_ABORTADA;
        }
    }
}

static void cerrar_y_recolectar_hijos(HijoActivo *activos, size_t cantidad_activos)
{
    for (size_t i = 0; i < cantidad_activos; i++) {
        if (activos[i].fd_lectura >= 0) {
            close(activos[i].fd_lectura);
            activos[i].fd_lectura = -1;
        }
    }

    for (;;) {
        pid_t r = waitpid(-1, NULL, 0);
        if (r > 0) {
            continue;
        }
        if (r < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
}

static int ejecutar_plan(Plan *plan, size_t k)
{
    const size_t n = plan->cantidad;
    const size_t limite = (k < n) ? k : n;

    size_t *cola_listas = malloc(n * sizeof(*cola_listas));
    HijoActivo *activos = calloc(limite, sizeof(*activos));

    if (cola_listas == NULL || activos == NULL) {
        free(cola_listas);
        free(activos);
        fprintf(stderr, "Error: memoria insuficiente para el scheduler\n");
        return -1;
    }

    size_t inicio_cola = 0;
    size_t fin_cola = 0;
    size_t cantidad_activos = 0;

    for (size_t i = 0; i < n; i++) {
        plan->items[i].dependencias_pendientes = plan->items[i].num_dependencias;
        plan->items[i].estado = EST_PENDIENTE;
        plan->items[i].pid = -1;

        if (plan->items[i].dependencias_pendientes == 0) {
            plan->items[i].estado = EST_LISTA;
            cola_listas[fin_cola++] = i;
        }
    }

    while (1) {
        if (g_sigint_recibido) {
            fprintf(stderr, "\n[SIGINT] Inspeccion de la Seremi: abortando todo...\n");
            cancelar_todo(plan, activos, cantidad_activos);
            cerrar_y_recolectar_hijos(activos, cantidad_activos);
            free(cola_listas);
            free(activos);
            return 130;
        }

        while (cantidad_activos < limite) {
            while (inicio_cola < fin_cola &&
                   plan->items[cola_listas[inicio_cola]].estado != EST_LISTA) {
                inicio_cola++;
            }

            if (inicio_cola >= fin_cola) {
                break;
            }

            size_t idx = cola_listas[inicio_cola++];
            (void)lanzar_actividad(plan, idx, activos, &cantidad_activos);

            if (g_sigint_recibido) {
                break;
            }
        }

        if (g_sigint_recibido) {
            continue;
        }

        if (cantidad_activos == 0) {
            break;
        }

        int status = 0;
        pid_t pid = waitpid(-1, &status, 0);

        if (pid < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("waitpid");
            cancelar_todo(plan, activos, cantidad_activos);
            cerrar_y_recolectar_hijos(activos, cantidad_activos);
            free(cola_listas);
            free(activos);
            return -1;
        }

        int pos = encontrar_hijo_activo(activos, cantidad_activos, pid);
        if (pos < 0) {
            continue;
        }

        HijoActivo terminado = activos[(size_t)pos];
        size_t idx = terminado.indice_actividad;

        char mensaje[MENSAJE_MAX];
        ssize_t largo = leer_mensaje_pipe(terminado.fd_lectura, mensaje, sizeof(mensaje));
        close(terminado.fd_lectura);

        activos[(size_t)pos] = activos[cantidad_activos - 1];
        cantidad_activos--;

        bool exito = WIFEXITED(status) && WEXITSTATUS(status) == 0 && largo > 0;

        if (exito) {
            procesar_exito(plan, idx, mensaje, cola_listas, &fin_cola);
        } else {
            char motivo[128];

            if (WIFSIGNALED(status)) {
                snprintf(motivo, sizeof(motivo), "termino por senal %d", WTERMSIG(status));
            } else if (WIFEXITED(status)) {
                snprintf(motivo, sizeof(motivo), "exit status %d", WEXITSTATUS(status));
            } else if (largo <= 0) {
                snprintf(motivo, sizeof(motivo), "no entrego mensaje por pipe");
            } else {
                snprintf(motivo, sizeof(motivo), "fallo desconocido");
            }

            marcar_fallo_y_propagar(plan, idx, motivo);
        }
    }

    free(cola_listas);
    free(activos);

    return 0;
}

static int leer_k(const char *texto, size_t *k)
{
    if (texto == NULL || *texto == '\0' || *texto == '-') {
        return -1;
    }

    errno = 0;
    char *fin = NULL;
    unsigned long valor = strtoul(texto, &fin, 10);

    if (errno != 0 || fin == texto || *fin != '\0' || valor == 0) {
        return -1;
    }

    *k = (size_t)valor;
    return 0;
}

static void imprimir_resumen(const Plan *plan)
{
    size_t terminadas = 0;
    size_t fallidas = 0;
    size_t abortadas = 0;
    size_t otras = 0;

    for (size_t i = 0; i < plan->cantidad; i++) {
        switch (plan->items[i].estado) {
            case EST_TERMINADA:
                terminadas++;
                break;
            case EST_FALLIDA:
                fallidas++;
                break;
            case EST_ABORTADA:
                abortadas++;
                break;
            default:
                otras++;
                break;
        }
    }

    printf("\n=== RESUMEN ===\n");
    printf("Total:      %zu\n", plan->cantidad);
    printf("Terminadas: %zu\n", terminadas);
    printf("Fallidas:   %zu\n", fallidas);
    printf("Abortadas:  %zu\n", abortadas);
    printf("Otras:      %zu\n", otras);
}

