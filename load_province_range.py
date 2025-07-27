import os
from pathlib import Path

import geopandas as gpd

# 1) 读取全国省级边界
gdf = gpd.read_file("province-epsg-4490/province.shp")  # 文件名以实际为准
if gdf.crs.to_epsg() is None:
    gdf = gdf.set_crs(epsg=4490, allow_override=True)
else:
    assert gdf.crs.to_epsg() == 4490, "坐标系不是 CGCS-2000!"

# 2) 按字段 'NAME' 或 'PROVINCE' 切片
out_dir = Path("output_shp")

for prov, sub in gdf.groupby("省"):  # 'NAME' = 省级行政区中文名
    shp_path = out_dir / prov
    os.makedirs(shp_path, exist_ok=True)  # 确保目录存在
    province_shp_path = shp_path / f"{prov}.shp"
    sub.to_file(province_shp_path, driver="ESRI Shapefile", encoding="utf-8")
    print("✔ 导出", shp_path)
