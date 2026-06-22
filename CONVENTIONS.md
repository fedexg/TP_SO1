# Convenciones de código
## C
1. El código (variables, funciones...) se escribe en inglés. Ser preciso con los nombres.
2. Los comentarios se escriben en español.
3. Los nombres de variables se escriben con snake_case.
4. Los nombres de estructuras se escriben con PascalCase (salvo especificado otra cosa).
5. Los statements de bloque (funciones, if, while, ...) tienen la llave en la misma linea, separada por un espacio.
6. 4 espacios como indentación.

### Ejemplo
1.
```c
int cantidad_de_hilos = 4; // No.
int num_threads = 4;       // Sí.
```

2.
```c
// No:

// This function calculates how hot it is.
void calor(int x);

// Sí:

// Esta función calcula cuánto calor hace.
void hot(int x);
```

3.
```c
int numWorkersRunning = 0;   // No.
int num_workers_running = 0; // Sí.
```

4.
```c
struct worker_struct; // No.
struct WorkerStruct;  // Sí.
```

5.
```c
// No:
int func (int x){
    if(x == 1){
        return 0;
    }
    return x+1;
}

// Sí:
int func(int x) {
    if(x == 1) {
        return 0;
    }

    return x+1;
}
```

6.
```c
// No:
int x = 0;
while (x < 0) {
  ++x;
  if (x >= 10)
    printf("Hola!\n");
}

// Sí:
int x = 0;
while (x < 0) {
    ++x;
    if (x >= 10)
        printf("Hola!\n");
}
```
