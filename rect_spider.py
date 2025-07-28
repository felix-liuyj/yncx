"""
永久基本农田数据查询与下载工具

此模块提供了基于矩形区域查询并下载永久基本农田数据的功能。
支持将数据保存为GeoJSON格式以及转换为Shapefile格式。
"""
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Any

import fiona
import geopandas as gpd
from fiona.crs import from_epsg
from gmssl import sm2
from httpx import HTTPError, AsyncClient, Proxy, Timeout, Limits
from shapely.geometry import Polygon
from shapely.geometry import mapping
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

__all__ = (
    'PermanentBasicFarmlandSpider',
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# 查询永久基本农田数据的类
class PermanentBasicFarmlandSpider(AsyncClient):
    """
    永久基本农田数据查询类
    """
    SM2_PRIVATE_KEY = "3c444bddc35163466de81734157430c459d77624333d1daa61f5c22abd890dd0"
    # 隧道域名:端口号
    PROXY_TUNNEL = "l630.kdltps.com:15818"
    # 用户名和密码方式
    PROXY_USERNAME = "t15361501806886"
    PROXY_PASSWORD = "ckg7zjk8"
    # 批处理大小
    BATCH_SIZE = 100

    def __init__(self, geo_output: str, shp_output: str, n: int):
        """
        初始化查询类
        :param geo_output:  GeoJSON输出文件路径
        :param shp_output:  Shapefile输出文件路径
        :param n:  期望返回的结果数量
        """
        # noinspection HttpUrlsUsage
        super().__init__(
            timeout=Timeout(10.0, connect=5.0),
            limits=Limits(max_keepalive_connections=5, max_connections=10),
            proxy=Proxy(url=f"http://{self.PROXY_USERNAME}:{self.PROXY_PASSWORD}@{self.PROXY_TUNNEL}")
        )
        self.x1 = 0.0
        self.y1 = 0.0
        self.x2 = 0.0
        self.y2 = 0.0
        self.geo_output = geo_output
        self.shp_output = shp_output
        self.n = n
        self.feature_list: list[dict[str, Any]] = []
        self.feature: dict[str, Any] = []

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(HTTPError),
        reraise=True
    )
    async def get_features(self) -> str:
        """
        根据矩形范围查询永久基本农田数据
        Returns:
            加密的GeoJSON数据字符串
        Raises:
            HTTPError: 当HTTP请求失败时抛出
        """
        # HTTP请求配置




        form_data = {
            'queryMode': 'SpatialQuery',
            'queryParameters': {
                'prjCoordSys': {'epsgCode': 4490},
                'expectCount': self.n,
                'queryParams': [{'name': "pro31@yndk", 'attributeFilter': "1=1", 'fields': ['yjjbntmj', 'yjjbnttbbh']}],
                'startRecord': 0,
            },
            'geometry': {
                'id': None,
                'style': None,
                'parts': [5],
                'points': [
                    {'CLASS_NAME': 'SuperMap.Geometry.Point', 'id': "SuperMap.Geometry_1", 'bounds': None, 'SRID': None,
                     'x': self.x1, 'y': self.y1, 'tag': None, 'type': "Point", 'geometryType': "Point"},
                    {'CLASS_NAME': 'SuperMap.Geometry.Point', 'id': "SuperMap.Geometry_2", 'bounds': None, 'SRID': None,
                     'x': self.x2, 'y': self.y1, 'tag': None, 'type': "Point", 'geometryType': "Point"},
                    {'CLASS_NAME': 'SuperMap.Geometry.Point', 'id': "SuperMap.Geometry_3", 'bounds': None, 'SRID': None,
                     'x': self.x2, 'y': self.y2, 'tag': None, 'type': "Point", 'geometryType': "Point"},
                    {'CLASS_NAME': 'SuperMap.Geometry.Point', 'id': "SuperMap.Geometry_4", 'bounds': None, 'SRID': None,
                     'x': self.x1, 'y': self.y2, 'tag': None, 'type': "Point", 'geometryType': "Point"},
                    {'CLASS_NAME': 'SuperMap.Geometry.Point', 'id': "SuperMap.Geometry_5", 'bounds': None, 'SRID': None,
                     'x': self.x1, 'y': self.y1, 'tag': None, 'type': "Point", 'geometryType': "Point"}
                ],
                'type': "REGION",
                'prjCoordSys': {'epsgCode': 4490}
            },
            'spatialQueryMode': "INTERSECT"
        }

        # 使用httpx.Client发送POST请求
        response = await self.post(
            "https://yncx.mnr.gov.cn/dist-app-yn/map/queryResults.json",
            headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        },
            params={
            "returnContent": "true",
            'token': 'Whe67hpdoYaBsTRmdzFkEfWcUyHkFuwQOuKgXDHEOv2deNvj0VbufUWA2w0297kDBDa5T_1V6__VvI1lHY7_fMwl'
        },
            json=form_data
        )
        return response.raise_for_status().json().get('data', '')

    async def fetch_features_geojson(self) -> bytes:
        """
        :return:          解密后原始字节串
        """
        try:
            cipher_hex = await self.get_features()
            logger.info(f"已获取到加密的 GeoJSON 数据, 解密中...")
            if cipher_hex.startswith("04"):  # JS 里先做的那一步
                cipher_hex = cipher_hex[2:]
            content = sm2.CryptSM2(public_key="", private_key=self.SM2_PRIVATE_KEY, mode=1)
            content = json.loads(content.decrypt(bytes.fromhex(cipher_hex)).decode("utf-8"))
            record_sets = content.get('recordsets', [])
            for recordset in record_sets:
                if not (feature_list := recordset.get('features')):
                    continue
                self.feature_list.extend(feature_list)
            logger.info(f"解密完成，共获取到 {len(self.feature_list)} 个地块数据")
        except KeyboardInterrupt:
            logger.info("用户中断操作")
        except HTTPError as e:
            logger.error(f"HTTP请求错误: {str(e)}")
        except Exception as e:
            logger.error(f"处理过程中出现错误: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())

    @staticmethod
    def create_polygon(feature: dict[str, Any]) -> Polygon:
        """
        处理GeoJSON几何数据为Shapely Polygon对象
        Args:
            feature: GeoJSON特征字典
        Returns:
            Shapely Polygon对象
        """
        parts = feature.get('geometry', {}).get('parts')
        points = feature.get('geometry', {}).get('points')
        start_index = 0
        exterior_ring = []
        interior_rings = []

        for part in parts:
            end_index = start_index + part
            ring = [(points[i].get('x'), points[i].get('y')) for i in range(start_index, end_index)]

            if not exterior_ring:  # 如果外部边界尚未设置，则这是外部边界
                exterior_ring = ring
            else:  # 否则，这是一个内部边界
                interior_rings.append(ring)
            start_index = end_index

        # 如果没有内部边界，Polygon 只需要外部边界
        if not interior_rings:
            polygon = Polygon(exterior_ring)
        else:
            # 创建Polygon对象，第一个参数是外部边界，第二个参数是一个内部边界的列表
            polygon = Polygon(exterior_ring, holes=interior_rings)

        return polygon

    def process_feature(self, feature: dict[str, Any]) -> Dict[str, Any]:
        """
        处理单个GeoJSON特征，转换为Fiona可写入的格式
        Args:
            feature: GeoJSON特征字典
        Returns:
            格式化后的特征字典
        """
        return {
            'properties': {'ID': feature.get('ID')},
            'geometry': mapping(self.create_polygon(feature))
        }

    def download_geojson(self, file_path: str, batch_size: int = 100) -> None:
        """
        下载GeoJSON数据到文件，支持批量处理和并行计算
        Args:
            file_path: 目标文件路径
            batch_size: 批处理大小
        Returns:
            None
        """
        if not self.geo_output:
            return
        logger.info(f"下载数据到 {file_path}...")
        # 确保目标目录存在
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        # 检查文件是否存在
        file_exists = os.path.exists(file_path)

        # 使用线程池并行处理几何数据
        with ThreadPoolExecutor() as executor:
            if not file_exists:
                # 文件不存在，创建新文件
                schema = {'geometry': 'Polygon', 'properties': {'ID': 'int'}}
                with fiona.open(file_path, 'w', driver='GeoJSON', crs='EPSG:4490', schema=schema) as f:
                    # 批量并行处理特征
                    for i in range(0, len(self.feature_list), batch_size):
                        batch = self.feature_list[i:i + batch_size]
                        processed_features = list(executor.map(self.process_feature, batch))
                        for feature in processed_features:
                            f.write(feature)
            else:
                # 文件已存在，增量更新
                # 读取现有ID以避免重复
                with fiona.open(file_path, 'r') as f:
                    existing_ids = {feature.get('properties', {}).get('ID') for feature in f}

                # 过滤出新特征
                new_features = [f for f in self.feature_list if f.get('ID') not in existing_ids]

                if new_features:
                    with fiona.open(file_path, 'a') as f:
                        # 批量并行处理特征
                        for i in range(0, len(new_features), batch_size):
                            batch = new_features[i:i + batch_size]
                            processed_features = list(executor.map(self.process_feature, batch))
                            for feature in processed_features:
                                f.write(feature)

    def convert_geojson_to_shapefile(self) -> None:

        """
        如果指定了 Shapefile 输出路径，则将 GeoJSON 格式转换为 Shapefile 格式
        Returns:
            None
        """
        logger.info(f"转换为Shapefile格式: {self.shp_output}")
        if not self.geo_output or not self.shp_output:
            return
        with fiona.open(self.geo_output, 'r') as source:
            with fiona.open(
                    self.shp_output, 'w',
                    driver='ESRI Shapefile',
                    crs=from_epsg(4490),  # 设置坐标系，根据实际情况修改
                    schema=source.schema
            ) as sink:
                for feature in source:
                    sink.write(feature)


async def main():
    # 设置日志级别
    logging.getLogger().setLevel(logging.INFO)
    # 1. 读取 tile 文件，确保坐标系是 4490（Î经纬度）
    # crs_aea = CRS.from_proj4(
    #     "+proj=aea +lat_1=25 +lat_2=47 +lat_0=0 "
    #     "+lon_0=105 +x_0=0 +y_0=0"
    # )
    tiles = gpd.read_file("河南省.geojson")
    bounds_list = [(row.get("tile_id", idx + 1), *row.geometry.bounds) for idx, row in tiles.iterrows()]
    # tiles = gpd.read_file("province-geojson/河南省-0_1.geojson").to_crs(crs_aea)
    geo_output, shp_output = '', ''
    async with PermanentBasicFarmlandSpider(geo_output, shp_output, 1000) as spider:
        for tile_id, x1, y1, x2, y2 in bounds_list:
            print(f"开始查询区域: ({x1}, {y1}) - ({x2}, {y2}) 的永久基本农田数据...")
            await spider.fetch_features_geojson()
            if tile_id >= 10:
                break
        spider.download_geojson(geo_output)
        spider.convert_geojson_to_shapefile()
        logger.info("处理完成")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
