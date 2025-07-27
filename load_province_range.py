from pathlib import Path

import geopandas as gpd

# 1) 读取全国省级边界
gdf = gpd.read_file("province-epsg-4490/省.shp")  # 文件名以实际为准
assert gdf.crs.to_epsg() == 4490, "坐标系不是 CGCS-2000!"

# 2) 按字段 'NAME' 或 'PROVINCE' 切片
out_dir = Path("output_shp")
out_dir.mkdir(exist_ok=True)

for prov, sub in gdf.groupby("NAME"):  # 'NAME' = 省级行政区中文名
    shp_path = out_dir / f"{prov}.shp"
    sub.to_file(shp_path, driver="ESRI Shapefile", encoding="utf-8")
    print("✔ 导出", shp_path)
