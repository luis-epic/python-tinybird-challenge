# python-tinybird-challenge

Motor analítico en tiempo real en **Python puro** para procesamiento de eventos de e-commerce. Diseñado con principios OLAP orientados a integraciones con **Tinybird**, sin dependencias externas.

---

## 🎯 Objetivos Cubiertos

- [x] **PARTE 1 – Procesamiento**: Validación estricta de eventos con detección de duplicados O(N)
- [x] **PARTE 2 – Métricas**: Revenue total, conversión, usuarios únicos, top productos
- [x] **PARTE 3 – API**: Función `get_metrics()` con filtros (país, fechas) sin frameworks
- [x] **PARTE 4 – Diseño**: Arquitectura Tinybird, pipes, deduplicación, escalabilidad

---

## 🛠️ Stack Tecnológico

| Componente | Detalle |
|-----------|--------|
| **Lenguaje** | Python 3.11+ |
| **Dependencias** | ❌ Ninguna (stdlib puro) |
| **Frameworks** | ❌ No se usan (sin Django, FastAPI, Pandas) |
| **Tipo** | Script sincrónico + Mock data |

---

## 📁 Estructura del Proyecto

```
python-tinybird-challenge/
├── main.py              # Script principal con todas las partes
├── README.md            # Esta documentación
└── [Opcional] Dockerfile
```

---

## 🚀 Cómo ejecutar

### Opción 1: Ejecución Local
```bash
python main.py
```

**Salida esperada:**
```json
{
  "procesamiento": {
    "total_events": 6,
    "valid_events": 4,
    "invalid_events": 1,
    "duplicated_events": 1
  },
  "metricas": {
    "total_revenue": 150.5,
    "purchases": 1,
    "unique_users": 2,
    "conversion_rate": 0.5,
    "top_products": ["p_10"]
  }
}
```

---

## 📖 Guía de Uso

### PARTE 1 & 2: Procesamiento y Métricas

```python
from main import process_events

events = [
    {
        "event_id": "evt_001",
        "user_id": "u_123",
        "event_type": "purchase",
        "product_id": "p_10",
        "timestamp": "2026-05-03T10:20:00Z",
        "price": 150.5,
        "country": "CR"
    }
]

resultado = process_events(events)
print(resultado)
```

**Retorna:**
- `procesamiento`: Conteos de eventos (total, válidos, inválidos, duplicados)
- `metricas`: Revenue, purchases, unique_users, conversion_rate, top_products

---

### PARTE 3: API con Filtros

```python
from main import get_metrics

# Sin filtros
metrics = get_metrics(events)

# Filtrar por país
metrics_cr = get_metrics(events, country="CR")

# Filtrar por rango de fechas
metrics_range = get_metrics(
    events,
    country="CR",
    from_date="2026-05-01T00:00:00Z",
    to_date="2026-05-31T23:59:59Z"
)

print(metrics_range)
```

---

### PARTE 1: Validaciones Implementadas

| Validación | Campo | Regla |
|-----------|-------|-------|
| Requerido | `event_id` | No nulo, única en dataset |
| Requerido | `user_id` | No nulo |
| Válido | `event_type` | `product_view`, `add_to_cart`, `purchase` |
| Válido | `timestamp` | Formato ISO 8601 (ej: `2026-05-03T10:15:00Z`) |
| Condicional | `price` | Si `event_type == "purchase"` → `price > 0` |
| Deduplicación | `event_id` | Rechaza IDs duplicados |

**Ejemplo de evento inválido:**
```python
{
    "event_id": None,           # ❌ Falla: event_id requerido
    "user_id": "u_456",
    "event_type": "purchase",
    "price": -50,               # ❌ Falla: precio negativo
    "timestamp": "invalid"      # ❌ Falla: timestamp inválido
}
```

---

### PARTE 2: Métricas Calculadas

| Métrica | Descripción | Fórmula |
|---------|-----------|---------|
| `total_revenue` | Suma de precios en compras | `SUM(price WHERE event_type == "purchase")` |
| `purchases` | Cantidad de compras | `COUNT(event_type == "purchase")` |
| `unique_users` | Usuarios únicos en dataset | `DISTINCT(user_id)` |
| `conversion_rate` | Porcentaje de conversión | `users_purchased / users_viewed` |
| `top_products` | Productos más vendidos | `RANK BY COUNT(product_id)` |

---

### PARTE 4: Diseño de Arquitectura Tinybird

Ver [comentarios en `main.py`](main.py#L167) para:

1. **Modelado en Tinybird**
   - Tabla: `events` (PK: `event_id`)
   - Deduplication built-in en ingesta
   - Particionamiento por `(country, timestamp)`

2. **Pipes Recomendados**
   - `events_validated` → Filtrado de inválidos
   - `events_metrics` → Cálculo de métricas globales
   - `top_products_pipe` → Ranking dinámico
   - `metrics_by_country` → Segmentación geográfica

3. **Manejo de Duplicados**
   - **Opción A (Recomendada):** Deduplicación en ingesta con `DEDUPLICATION_PERIOD = 24h`
   - **Opción B:** Query-time deduplicación con `ROW_NUMBER()`
   - **Opción C:** Validación previa (enfoque actual)

4. **Escalabilidad**
   - **Volumen:** Particionamiento + MergeTree engine + TTL
   - **Queries:** Caching Redis (5 min), materializations
   - **Ingesta:** Batch insert (1000 evt), Kafka si > 10K evt/s
   - **Monitoreo:** Alertas en invalid_events > 5%

---

## ✅ Checklist de Implementación

- [x] Procesamiento O(N) en una sola pasada
- [x] Validación de `event_id`, `user_id`, `event_type`, `timestamp`
- [x] Validación condicional para `purchase.price > 0`
- [x] Detección de duplicados por `event_id`
- [x] Cálculo de 5 métricas distintas
- [x] Función `get_metrics()` parametrizada (país, fechas)
- [x] Sin dependencias externas (stdlib puro)
- [x] Documentación de arquitectura Tinybird
- [x] Mock data para testing

---

## 📊 Ejemplo Completo

```bash
$ python main.py

--- 📊 Procesando todos los eventos (Sin filtros) ---
{
  "procesamiento": {
    "total_events": 6,
    "valid_events": 4,
    "invalid_events": 1,
    "duplicated_events": 1
  },
  "metricas": {
    "total_revenue": 150.5,
    "purchases": 1,
    "unique_users": 2,
    "conversion_rate": 0.5,
    "top_products": ["p_10"]
  }
}

--- 🇨🇷 Procesando eventos filtrados por País (CR) ---
{
  "procesamiento": {
    "total_events": 4,
    "valid_events": 3,
    "invalid_events": 1,
    "duplicated_events": 0
  },
  "metricas": {
    "total_revenue": 150.5,
    "purchases": 1,
    "unique_users": 1,
    "conversion_rate": 1.0,
    "top_products": ["p_10"]
  }
}
```

---

## 📚 Funciones Clave

### `process_events(events: List[Dict]) → Dict`
Procesa eventos y retorna estadísticas de validación + métricas.

**Entrada:**
```python
[
    {"event_id": "evt_001", "user_id": "u_123", "event_type": "product_view", 
     "timestamp": "2026-05-03T10:15:00Z", "price": 0, "country": "CR", "product_id": "p_10"}
]
```

**Salida:**
```python
{
    "procesamiento": {"total_events": 1, "valid_events": 1, "invalid_events": 0, "duplicated_events": 0},
    "metricas": {"total_revenue": 0.0, "purchases": 0, "unique_users": 1, "conversion_rate": 0.0, "top_products": []}
}
```

---

### `get_metrics(events, country=None, from_date=None, to_date=None) → Dict`
API controladora que filtra antes de procesar.

**Parámetros:**
- `events`: List de eventos a procesar
- `country` (opt): Código de país (ej: "CR", "MX")
- `from_date` (opt): Fecha inicio ISO 8601
- `to_date` (opt): Fecha fin ISO 8601

**Ejemplo:**
```python
get_metrics(mock_events, country="CR", from_date="2026-05-01T00:00:00Z")
```

---

## 🔗 Next Steps (Implementación Real)

1. **Conectar con Tinybird API**
   ```python
   import requests
   
   def push_to_tinybird(events, api_token):
       response = requests.post(
           "https://api.tinybird.co/v0/events?name=events",
           headers={"Authorization": f"Bearer {api_token}"},
           json={"events": [e for e in events if is_valid(e)]}
       )
   ```

2. **Crear tabla y pipes en Tinybird Console**

3. **Exponer API con FastAPI/Uvicorn** (si se requiere servidor)

---

## 📝 Notas Importantes

- El script usa **stdlib puro** (sin pandas, numpy, requests)
- Los timestamps deben estar en **ISO 8601** con zona horaria
- La deduplicación usa `set()` → O(1) lookup
- Las métricas se calculan en **una sola pasada** → O(N)
- Mock data incluido en `if __name__ == "__main__"` para testing

---

## 📧 Contacto
luismartinez.developer@gmail.com
+584126335861
Proyecto de desafío técnico: **python-tinybird-challenge**
