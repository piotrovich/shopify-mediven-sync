import pandas as pd, glob, os
f = max(glob.glob('reportes/diagnostico_sync_*.xlsx'), key=os.path.getmtime)
df = pd.read_excel(f, sheet_name='CAMBIOS_PRECIO')
df = df[(df['Precio_Anterior'] > 0) & (df['Precio_Nuevo'] > 0)].copy()
df['delta_pct'] = (df['Precio_Nuevo'] - df['Precio_Anterior']) / df['Precio_Anterior'] * 100

subidas = df[df['delta_pct'] > 0]
bajadas = df[df['delta_pct'] < 0]
print(f"Total con ambos precios: {len(df)}")
print(f"  Subidas: {len(subidas)} | Bajadas: {len(bajadas)}")
print(f"  Mayor SUBIDA: +{df['delta_pct'].max():.0f}%")
print(f"  Mayor BAJADA: {df['delta_pct'].min():.0f}%")

extremos = df[(df['delta_pct'] <= -85) | (df['delta_pct'] >= 200)]
print(f"\nCambios que el breaker DEBERIA retener (<=-85% o >=+200%): {len(extremos)}")
for _, r in extremos.iterrows():
    print(f"    {r['SKU']}: {r['Precio_Anterior']:,.0f} -> {r['Precio_Nuevo']:,.0f} ({r['delta_pct']:.0f}%)")

sondas = df[df['Descripcion'].str.contains('SONDA', case=False, na=False)]
print(f"\nSondas en la lista: {len(sondas)}")
for _, r in sondas.iterrows():
    print(f"    {r['Descripcion'][:40]}: {r['Precio_Anterior']:,.0f} -> {r['Precio_Nuevo']:,.0f} ({r['delta_pct']:.0f}%)")

print("\nTop 8 BAJADAS:")
for _, r in bajadas.nsmallest(8, 'delta_pct').iterrows():
    print(f"    {r['Descripcion'][:38]:<40} {r['Precio_Anterior']:>8,.0f} -> {r['Precio_Nuevo']:>8,.0f} ({r['delta_pct']:.0f}%)")
print("\nTop 8 SUBIDAS:")
for _, r in subidas.nlargest(8, 'delta_pct').iterrows():
    print(f"    {r['Descripcion'][:38]:<40} {r['Precio_Anterior']:>8,.0f} -> {r['Precio_Nuevo']:>8,.0f} (+{r['delta_pct']:.0f}%)")
