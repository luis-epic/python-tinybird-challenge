"""Test cases para validar lógica edge-case."""
from main import process_events, get_metrics

# Test 1: Usuario que compra sin "ver" primero
print("TEST 1: Compra sin product_view previo")
events1 = [
    {"event_id": "evt_1", "user_id": "u_1", "event_type": "purchase", 
     "product_id": "p_1", "timestamp": "2026-05-03T10:00:00Z", "price": 100.0, "country": "CR"}
]
result1 = process_events(events1)
print(f"  conversion_rate: {result1['metricas']['conversion_rate']} (esperado: 0.0)")
print(f"  purchases: {result1['metricas']['purchases']} (esperado: 1)")
print()

# Test 2: Precio cero en purchase (debe ser inválido)
print("TEST 2: Purchase con precio 0 (debe rechazarse)")
events2 = [
    {"event_id": "evt_2", "user_id": "u_2", "event_type": "purchase",
     "product_id": "p_2", "timestamp": "2026-05-03T10:00:00Z", "price": 0, "country": "CR"}
]
result2 = process_events(events2)
print(f"  valid_events: {result2['procesamiento']['valid_events']} (esperado: 0)")
print(f"  invalid_events: {result2['procesamiento']['invalid_events']} (esperado: 1)")
print()

# Test 3: Multiple purchases mismo usuario
print("TEST 3: Múltiples compras del mismo usuario")
events3 = [
    {"event_id": "evt_3a", "user_id": "u_3", "event_type": "purchase",
     "product_id": "p_3", "timestamp": "2026-05-03T10:00:00Z", "price": 50.0, "country": "CR"},
    {"event_id": "evt_3b", "user_id": "u_3", "event_type": "purchase",
     "product_id": "p_3", "timestamp": "2026-05-03T11:00:00Z", "price": 75.0, "country": "CR"}
]
result3 = process_events(events3)
print(f"  unique_users: {result3['metricas']['unique_users']} (esperado: 1)")
print(f"  purchases: {result3['metricas']['purchases']} (esperado: 2)")
print(f"  total_revenue: {result3['metricas']['total_revenue']} (esperado: 125.0)")
print()

# Test 4: add_to_cart no afecta métricas
print("TEST 4: add_to_cart no cuenta en viewing/purchasing")
events4 = [
    {"event_id": "evt_4a", "user_id": "u_4", "event_type": "add_to_cart",
     "product_id": "p_4", "timestamp": "2026-05-03T10:00:00Z", "price": 0, "country": "CR"},
    {"event_id": "evt_4b", "user_id": "u_4", "event_type": "purchase",
     "product_id": "p_4", "timestamp": "2026-05-03T11:00:00Z", "price": 100.0, "country": "CR"}
]
result4 = process_events(events4)
print(f"  conversion_rate: {result4['metricas']['conversion_rate']} (esperado: 0.0, no hay viewers)")
print()

# Test 5: Timestamp inválido
print("TEST 5: Timestamp inválido")
events5 = [
    {"event_id": "evt_5", "user_id": "u_5", "event_type": "product_view",
     "product_id": "p_5", "timestamp": "invalid", "price": 0, "country": "CR"}
]
result5 = process_events(events5)
print(f"  invalid_events: {result5['procesamiento']['invalid_events']} (esperado: 1)")
print()

# Test 6: Filtro por país excluye eventos sin país
print("TEST 6: Evento sin país se filtra al buscar por país")
events6 = [
    {"event_id": "evt_6a", "user_id": "u_6", "event_type": "product_view",
     "product_id": "p_6", "timestamp": "2026-05-03T10:00:00Z", "price": 0, "country": "CR"},
    {"event_id": "evt_6b", "user_id": "u_6", "event_type": "product_view",
     "product_id": "p_6", "timestamp": "2026-05-03T11:00:00Z", "price": 0}  # sin country
]
result6 = get_metrics(events6, country="CR")
print(f"  total_events: {result6['procesamiento']['total_events']} (esperado: 1, solo CR)")
print()

print("✅ TODOS LOS TESTS COMPLETADOS")
