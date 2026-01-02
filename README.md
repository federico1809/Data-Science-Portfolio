# Henry Data Science Projects and Notes

**Repositorio:** `henry-ds-projects-and-notes`  
**Autor:** Federico Ceballos Torres  
**Cohorte:** Dsf03

## Descripción

Repositorio con el material del curso Henry Data Science organizado por módulos: notebooks, ejercicios, proyectos integradores y apuntes. Pensado como copia de trabajo y portafolio personal.

## Estructura

* **m01-Fundamentos** — Notebooks de estadística y probabilidad; apuntes teóricos.
* **m02-Bases-de-datos** — Scripts SQL, ejemplos de arquitectura y ejercicios.
* **m03-BI-Visualizacion** — Dashboards, notebooks de visualización y recursos.
* **m04-Machine-Learning** — Modelos, experimentos, notebooks y entregables.
* **m05-Cloud-DS-Production** — Notas sobre despliegue, pipelines y producción.
* **otros** — Programas, Proyectos Cerrados y recursos adicionales.

Cada carpeta debe incluir un `README.md` propio con instrucciones, dependencias y cómo reproducir los ejercicios.

## Uso

### Clonar
```bash
git clone https://github.com/tu-usuario/henry-ds-projects-and-notes.git
```

### Flujo básico
```bash
cd henry-ds-projects-and-notes
git checkout -b feature/nombre-feature
# trabajar, commitear
git add .
git commit -m "Descripción clara del cambio"
git push -u origin feature/nombre-feature
```

## Buenas prácticas

* Trabajar en ramas por tarea (`feature/`, `fix/`, `experiment/`).
* Hacer `git pull --rebase origin main` antes de pushear a `main`.
* Documentar cada notebook con objetivo, dependencias y pasos para reproducir.

## Datos y archivos grandes

### Reglas

* No subir datos sensibles ni credenciales.
* No subir archivos >100 MB directamente; usar Git LFS o almacenamiento externo (Drive, S3) y documentar cómo obtenerlos.
* Incluir instrucciones para descargar datasets externos en el README de cada módulo.

### Ejemplo mínimo de .gitignore
```gitignore
# Python
__pycache__/
*.pyc
.env
venv/
.env/

# Jupyter
.ipynb_checkpoints/

# Datos y modelos pesados
data/
models/

# IDEs
.vscode/
.idea/
```

## Contribuciones

* **Commits claros:** verbo en presente y descripción breve.
* **Documentación:** cada notebook debe tener una celda inicial con objetivo y dependencias.
* Añadir `CONTRIBUTING.md` si colaboran varias personas.
* Abrir Pull Requests para revisiones.

## Licencia y contacto

* **Contacto:** Federico Ceballos Torres — Cohorte Dsf03