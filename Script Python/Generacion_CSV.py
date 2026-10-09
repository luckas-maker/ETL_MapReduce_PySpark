import csv
import random

def generar_y_validar():
    print("--- INICIANDO GENERACIÓN DE DATOS ---")
    
    nombres_masculinos = ["ALEJANDRO", "CARLOS", "DANIEL", "EDUARDO", "FERNANDO", "GABRIEL"]
    nombres_femeninos = ["ANA", "BEATRIZ", "CARMEN", "DIANA"]
    
    apellidos = ["PEREZ", "GOMEZ", "LOPEZ", "MARTINEZ", "SANCHEZ", "GONZALEZ", "RAMIREZ", "TORRES", "FLORES", "RIVERA"]
    random.shuffle(apellidos)
    
    clientes = []
    for i in range(10):
        idC = f"C{i+1:02d}"
        if i < 6:
            nombre = f"{nombres_masculinos[i]} {apellidos[i]}"
            sexo = "Masculino"
        else:
            nombre = f"{nombres_femeninos[i-6]} {apellidos[i]}"
            sexo = "Femenino"
            
        edad = random.randint(18, 60)
        nacionalidad = random.choice(["Mexico", "Estados Unidos Americanos"])
        
        clientes.append({
            "idC": idC, "nombre": nombre, "edad": edad, 
            "sexo": sexo, "nacionalidad": nacionalidad
        })

    tipos = ["Electronico", "Alimenticio", "Limpieza", "Ropa", "Muebles"]
    catalogo_base = {
        "Electronico": [("TV 4K", 3000, 4500), ("Radio", 200, 350), ("Laptop", 5000, 8000), ("Tablet", 2000, 3500)],
        "Alimenticio": [("Cereal", 20, 45), ("Galletas", 15, 30), ("Leche", 18, 28), ("Queso", 40, 65)],
        "Limpieza": [("Jabon", 10, 18), ("Cloro", 12, 20), ("Escoba", 30, 55), ("Detergente", 40, 70)],
        "Ropa": [("Camisa", 150, 350), ("Pantalon", 200, 450), ("Chamarra", 300, 650), ("Tenis", 400, 800)],
        "Muebles": [("Silla", 150, 280), ("Mesa", 500, 900), ("Sofa", 2000, 3800), ("Cama", 1500, 2900)]
    }

    productos = []
    p_idx = 1
    for tipo in tipos:
        for prod in catalogo_base[tipo]:
            productos.append({
                "idP": f"P{p_idx:02d}",
                "nombre": prod[0],
                "tipo": tipo,
                "precio_compra": prod[1],
                "precio_venta": prod[2]
            })
            p_idx += 1

    meses_distribucion = {mes: 650 for mes in range(1, 13)}
    
    facturas_restantes = 10000 - 7800
    for _ in range(facturas_restantes):
        meses_distribucion[random.randint(1, 12)] += 1
        
    dias_por_mes = {1:31, 2:28, 3:31, 4:30, 5:31, 6:30, 7:31, 8:31, 9:30, 10:31, 11:30, 12:31}
    
    facturas_general = []
    f_idx = 1
    
    for mes, cantidad in meses_distribucion.items():
        for _ in range(cantidad):
            id_factura = f"FACT{f_idx:05d}"
            id_c = random.choice(clientes)["idC"]
            dia = random.randint(1, dias_por_mes[mes])
            fecha = f"{dia:02d}/{mes:02d}/2025"
            
            facturas_general.append({
                "idFactura": id_factura,
                "idC": id_c,
                "fecha": fecha
            })
            f_idx += 1

    random.shuffle(facturas_general)

    facturas_detalle = []
    for factura in facturas_general:
        productos_factura = random.sample(productos, 4)
        cantidades_usadas = set()
        
        for p in productos_factura:
            cantidad = random.randint(6, 600)
            while cantidad in cantidades_usadas:
                amount = random.randint(6, 600)
            cantidades_usadas.add(cantidad)
            
            facturas_detalle.append({
                "idFactura": factura["idFactura"],
                "idP": p["idP"],
                "cantidad": cantidad,
                "precio_unitario_venta": p["precio_venta"]
            })

    print("--- DATOS GENERADOS EN MEMORIA. INICIANDO VALIDACIONES ---")
    
    errores = []

    if len(clientes) != 10: errores.append("Clientes: No hay exactamente 10 registros.")
    if len(set([c["nombre"].split()[0] for c in clientes])) != 10: errores.append("Clientes: Hay nombres repetidos.")
    if len(set([c["nombre"].split()[1] for c in clientes])) != 10: errores.append("Clientes: Hay apellidos repetidos.")
    if sum(1 for c in clientes if c["sexo"] == "Masculino") != 6: errores.append("Clientes: No hay exactamente 6 hombres.")
    if sum(1 for c in clientes if c["sexo"] == "Femenino") != 4: errores.append("Clientes: No hay exactamente 4 mujeres.")
    if not all(18 <= c["edad"] <= 60 for c in clientes): errores.append("Clientes: Hay edades fuera del rango 18-60.")

    if len(productos) != 20: errores.append("Productos: No hay exactamente 20 registros.")
    tipos_count = {}
    for p in productos:
        tipos_count[p["tipo"]] = tipos_count.get(p["tipo"], 0) + 1
        if p["precio_compra"] >= p["precio_venta"]: errores.append(f"Productos: {p['idP']} el precio de compra no es menor al de venta.")

    if len(tipos_count) != 5 or not all(count == 4 for count in tipos_count.values()): 
        errores.append("Productos: No se cumple la regla de 5 tipos con 4 productos cada uno.")

    if len(facturas_general) != 10000: errores.append("FacturaGeneral: No hay exactamente 10,000 registros.")
    meses_check = {m: 0 for m in range(1, 13)}
    ids_clientes_validos = {c["idC"] for c in clientes}
    for f in facturas_general:
        if f["idC"] not in ids_clientes_validos: errores.append(f"FacturaGeneral: El {f['idC']} no existe en el catálogo.")
        mes = int(f["fecha"].split("/")[1])
        meses_check[mes] += 1
    if any(count < 650 for count in meses_check.values()): errores.append("FacturaGeneral: Hay meses con menos de 650 facturas.")

    if len(facturas_detalle) != 40000: errores.append("FacturasDetalle: No hay exactamente 40,000 registros.")
    df_agrupadas = {}
    for d in facturas_detalle:
        df_agrupadas.setdefault(d["idFactura"], []).append(d)
        
    productos_dict = {p["idP"]: p["precio_venta"] for p in productos}
    
    for idF, items in df_agrupadas.items():
        if len(items) != 4: errores.append(f"FacturasDetalle: {idF} no tiene exactamente 4 productos.")
        ids_p_factura = set()
        for item in items:
            if item["idP"] in ids_p_factura: errores.append(f"FacturasDetalle: Producto repetido en la factura {idF}.")
            ids_p_factura.add(item["idP"])
            
            if not (6 <= item["cantidad"] <= 600): errores.append(f"FacturasDetalle: Cantidad fuera de rango en {idF}.")
            
            if item["precio_unitario_venta"] != productos_dict.get(item["idP"], -1):
                errores.append(f"FacturasDetalle: Inconsistencia de precio en {idF} para el producto {item['idP']}.")

    if errores:
        print("\n[!] ERROR DE VALIDACIÓN. NO SE CREARÁN LOS ARCHIVOS CSV.")
        for error in errores:
            print(f" - {error}")
    else:
        print("\n[OK] TODAS LAS VALIDACIONES FUERON EXITOSAS. COHERENCIA AL 100%.")
        print("Guardando archivos CSV...")
        
        with open("Clientes.csv", "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["idC", "nombre", "edad", "sexo", "nacionalidad"])
            writer.writeheader()
            writer.writerows(clientes)
            
        with open("Productos.csv", "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["idP", "nombre", "tipo", "precio_compra", "precio_venta"])
            writer.writeheader()
            writer.writerows(productos)
            
        with open("FacturaGeneral.csv", "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["idFactura", "idC", "fecha"])
            writer.writeheader()
            writer.writerows(facturas_general)
            
        with open("FacturasDetalle.csv", "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["idFactura", "idP", "cantidad", "precio_unitario_venta"])
            writer.writeheader()
            writer.writerows(facturas_detalle)
            
        print("ARCHIVOS CSV CREADOS CORRECTAMENTE:\n- Clientes.csv\n- Productos.csv\n- FacturaGeneral.csv\n- FacturasDetalle.csv")

if __name__ == "__main__":
    generar_y_validar()

            
