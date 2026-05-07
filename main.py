import json
from typing import List, Dict, Any, Optional
from datetime import datetime

def parse_date(date_str: str) -> Optional[datetime]:
    """Utilidad para validar y parsear el timestamp ISO 8601."""
    try:
        # Reemplazamos 'Z' por '+00:00' para compatibilidad con fromisoformat
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None

def process_events(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Procesa la lista de eventos en una sola pasada O(N).
    Maneja validaciones estrictas (Parte 1) y cálculo de métricas (Parte 2).
    """
    # Contadores de estado
    total_events = 0
    valid_events = 0
    invalid_events = 0
    duplicated_events = 0
    
    # Control de duplicados O(1)
    seen_event_ids = set()
    
    # Agrupadores de métricas
    total_revenue = 0.0
    purchases = 0
    unique_users = set()
    viewing_users = set()
    purchasing_users = set()
    product_sales: Dict[str, int] = {}
    
    valid_event_types = {'product_view', 'add_to_cart', 'purchase'}

    for event in events:
        total_events += 1
        
        event_id = event.get("event_id")
        user_id = event.get("user_id")
        event_type = event.get("event_type")
        timestamp_str = event.get("timestamp")
        price = event.get("price", 0)
        product_id = event.get("product_id")
        
        # 1. Validación de duplicados
        if not event_id or event_id in seen_event_ids:
            if event_id:
                duplicated_events += 1
            else:
                invalid_events += 1 # Falla validación: event_id es requerido
            continue
            
        seen_event_ids.add(event_id)
        
        # 2. Validaciones estrictas
        parsed_date = parse_date(timestamp_str) if timestamp_str else None
        
        is_valid = (
            user_id is not None and
            event_type in valid_event_types and
            parsed_date is not None
        )
        
        # Validación de compras (price > 0)
        if event_type == "purchase" and (not isinstance(price, (int, float)) or price <= 0):
            is_valid = False
            
        if not is_valid:
            invalid_events += 1
            continue
            
        # 3. Procesamiento de métricas (Solo eventos válidos)
        valid_events += 1
        unique_users.add(user_id)
        
        if event_type == "product_view":
            viewing_users.add(user_id)
            
        elif event_type == "purchase":
            purchases += 1
            total_revenue += price  # ya validado como int/float
            purchasing_users.add(user_id)
            
            if product_id:
                product_sales[product_id] = product_sales.get(product_id, 0) + 1

    # 4. Consolidación de métricas finales
    conversion_rate = 0.0
    if len(viewing_users) > 0:
        conversion_rate = len(purchasing_users) / len(viewing_users)
        
    # Extraer los IDs de los productos más vendidos
    top_products = sorted(product_sales.items(), key=lambda item: item[1], reverse=True)

    return {
        "procesamiento": {
            "total_events": total_events,
            "valid_events": valid_events,
            "invalid_events": invalid_events,
            "duplicated_events": duplicated_events
        },
        "metricas": {
            "total_revenue": round(total_revenue, 2),
            "purchases": purchases,
            "unique_users": len(unique_users),
            "conversion_rate": round(conversion_rate, 4),
            "top_products": [prod[0] for prod in top_products]
        }
    }

def get_metrics(events: List[Dict[str, Any]], country: Optional[str] = None, from_date: Optional[str] = None, to_date: Optional[str] = None) -> Dict[str, Any]:
    """
    API controlador (Parte 3). Filtra los eventos antes de inyectarlos al procesador.
    """
    filtered_events = []
    
    start_dt = parse_date(from_date) if from_date else None
    end_dt = parse_date(to_date) if to_date else None

    for event in events:
        # Filtro de país
        if country and event.get("country") != country:
            continue
            
        # Filtro de rango de fechas
        if start_dt or end_dt:
            event_dt = parse_date(event.get("timestamp", ""))
            if not event_dt:
                continue 
                
            if start_dt and event_dt < start_dt:
                continue
            if end_dt and event_dt > end_dt:
                continue
                
        filtered_events.append(event)
        
    return process_events(filtered_events)

# ==========================================
# BLOQUE DE EJECUCIÓN Y PRUEBAS LOCALES
# ==========================================
if __name__ == "__main__":
    # Mock de datos cubriendo casos de uso (Válidos, inválidos y duplicados)
    mock_events = [
        {"event_id": "evt_001", "user_id": "u_123", "event_type": "product_view", "product_id": "p_10", "timestamp": "2026-05-03T10:15:00Z", "price": 0, "country": "CR"},
        {"event_id": "evt_002", "user_id": "u_123", "event_type": "add_to_cart", "product_id": "p_10", "timestamp": "2026-05-03T10:16:00Z", "price": 0, "country": "CR"},
        {"event_id": "evt_003", "user_id": "u_123", "event_type": "purchase", "product_id": "p_10", "timestamp": "2026-05-03T10:20:00Z", "price": 150.5, "country": "CR"},
        {"event_id": "evt_004", "user_id": "u_456", "event_type": "product_view", "product_id": "p_20", "timestamp": "2026-05-03T11:00:00Z", "price": 0, "country": "MX"},
        {"event_id": "evt_001", "user_id": "u_123", "event_type": "product_view", "timestamp": "2026-05-03T10:15:00Z"}, # Intencionalmente duplicado
        {"event_id": "evt_005", "user_id": None, "event_type": "purchase", "product_id": "p_30", "timestamp": "2026-05-03T12:00:00Z", "price": -50, "country": "CR"} # Inválido (sin user_id y precio negativo)
    ]

    print("--- 📊 Procesando todos los eventos (Sin filtros) ---")
    resultados_totales = get_metrics(mock_events)
    print(json.dumps(resultados_totales, indent=2))

    print("\n--- 🇨🇷 Procesando eventos filtrados por País (CR) ---")
    resultados_cr = get_metrics(mock_events, country="CR")
    print(json.dumps(resultados_cr, indent=2))


# ==========================================
# PARTE 4 – DISEÑO (Arquitectura Tinybird)
# ==========================================
"""
PREGUNTA 1: ¿Cómo modelar en Tinybird?
├── Table: events (Source Data Model)
│   ├── event_id: String (Primary Key)
│   ├── user_id: String
│   ├── event_type: Enum('product_view', 'add_to_cart', 'purchase')
│   ├── product_id: String (Nullable)
│   ├── price: Float
│   ├── country: String
│   ├── timestamp: DateTime
│   └── _tb_inserted_at: DateTime (deduplication timestamp)

PREGUNTA 2: ¿Qué pipes crear?
1. events_validated
   └─ Filtra eventos inválidos, detecta duplicados
   
2. events_metrics
   └─ Calcula: revenue, purchases, users únicos, conversión
   
3. top_products_pipe
   └─ Ranking de productos más vendidos
   
4. metrics_by_country
   └─ Métricas segmentadas por país con filtros de fecha

PREGUNTA 3: ¿Cómo evitar duplicados a escala?
✓ OPCIÓN A: Deduplicación en ingesta (Recomendado)
  • Usar event_id como Primary Key
  • Configurar DEDUPLICATION_PERIOD = 24h
  • Tinybird automáticamente rechaza event_id duplicados
  
✓ OPCIÓN B: Deduplicación en READ (si ya existen duplicados)
  • En query: SELECT DISTINCT ON (event_id) * FROM events
  • Mantener solo evento más reciente: ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _tb_inserted_at DESC)
  
✓ OPCIÓN C: Validación previa (enfoque actual del script)
  • Procesar en lotes con set() de event_ids seen
  • Rechazar antes de insertar en table

PREGUNTA 4: ¿Cómo escalar?
┌─ VOLUMEN (millones de eventos/día)
│  ├─ Particionar tabla por (country, timestamp)
│  ├─ Usar MergeTree engine con ORDER BY event_id
│  └─ TTL para limpiar datos antiguos
│
├─ QUERYS EN TIEMPO REAL
│  ├─ Cachear top_products en Redis (5 min)
│  ├─ Usar materializations para conversión_rate
│  └─ Índices en (user_id, event_type) para filtrados
│
├─ INGESTA
│  ├─ Batch insert (1000 eventos por request)
│  ├─ Cola con Kafka/RabbitMQ si > 10K evt/s
│  └─ Circuit breaker si tabla está saturada
│
└─ MONITOREO
   ├─ Alertas si invalid_events > 5%
   ├─ Revisar duplicados diarios
   └─ Latencia de queries < 2s

ARQUITECTURA RECOMENDADA:
┌──────────────┐
│   E-COMMERCE │
│   (datos)    │
└──────┬───────┘
       │ POST /events
       ▼
┌──────────────────────┐
│  Validación (script) │◄── Event ID validation
│  - Parseo datos      │    - User ID check
│  - Duplicados (set)  │    - Tipo válido
│  - Price validation  │    - Timestamp ISO8601
└──────┬───────────────┘
       │ Eventos válidos
       ▼
┌──────────────────┐
│  TINYBIRD TABLE  │
│  (events)        │    ← Deduplication built-in
│  PK: event_id    │
└──────┬───────────┘
       │ PIPES
       ├─► events_validated
       ├─► events_metrics
       ├─► top_products_pipe
       └─► metrics_by_country
       │
       ▼ (Queries/API)
┌──────────────────┐
│  RESPUESTA JSON  │
│  - Procesamiento │
│  - Métricas      │
│  - Top products  │
└──────────────────┘
"""