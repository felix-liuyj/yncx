from pathlib import Path

import geopandas as gpd
from shapely.geometry import box


def generate_tiles_for_province(shp_path: Path, tile_size_deg: float = 0.005):
    province_name = shp_path.parent.name
    print(f"⏳ 处理省份: {province_name}")

    # 读取 shapefile，并转换为经纬度坐标系
    gdf = gpd.read_file(shp_path)
    gdf = gdf.to_crs(epsg=4490)

    bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
    minx, miny, maxx, maxy = bounds
    print(f"边界范围: {bounds}")

    # 构建 tile 网格
    tiles = []
    x = minx
    while x < maxx:
        y = miny
        while y < maxy:
            tile = box(x, y, x + tile_size_deg, y + tile_size_deg)
            tiles.append(tile)
            y += tile_size_deg
        x += tile_size_deg
        print('生成 tile:', len(tiles), end='\r')

    print(f"总计生成 {len(tiles)} 个 tile")

    grid = gpd.GeoDataFrame(geometry=tiles, crs="EPSG:4490")

    print('计算与省份边界的交集...')
    # 与省份边界相交
    intersected = gpd.overlay(grid, gdf, how="intersection")
    intersected["tile_id"] = range(1, len(intersected) + 1)

    print(f"交集 tile 数量: {len(intersected)}")
    # 输出路径：省目录下
    output_path = shp_path.parent.parent.parent / f"{province_name}.geojson"
    intersected.to_file(output_path, driver="GeoJSON", encoding="utf-8")
    print(f"✅ {province_name} 已生成 tile 数量: {len(intersected)}\n")


def main():
    base_dir = Path("province-epsg-4490")
    tile_size = 0.05  # 每个 tile 是 0.5 x 0.5 度

    try:
        generate_tiles_for_province(base_dir / '河南省' / '河南省.shp', tile_size_deg=tile_size)
    except Exception as e:
        print(f"❌ 错误处理 'province-epsg-4490/河南省/'：{e}")

    # for province_dir in base_dir.iterdir():
    #     if not province_dir.is_dir():
    #         continue
    #
    #     shp_files = list(province_dir.glob("*.shp"))
    #     if not shp_files:
    #         print(f"⚠️ 跳过 {province_dir.name}：未找到 shp 文件")
    #         continue
    #     try:
    #         generate_tiles_for_province(shp_files[0], tile_size_deg=tile_size)
    #     except Exception as e:
    #         print(f"❌ 错误处理 {province_dir.name}：{e}")


if __name__ == "__main__":
    main()
