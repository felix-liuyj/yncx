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
    PROXY_TUNNEL = "z307.kdltpspro.com:15818"

    # 用户名和密码方式
    PROXY_USERNAME = "t15351811402121"
    PROXY_PASSWORD = "izsuxm6r"

    # 批处理大小
    BATCH_SIZE = 100

    def __init__(self, x1: float, y1: float, x2: float, y2: float, n: int):
        """
        初始化查询参数

        Args:
            x1: 左上角经度
            y1: 左上角纬度
            x2: 右下角经度
            y2: 右下角纬度
            n: 期望返回的结果数量
        """
        # noinspection HttpUrlsUsage
        super().__init__(
            timeout=Timeout(10.0, connect=5.0),
            limits=Limits(max_keepalive_connections=5, max_connections=10),
            proxy=Proxy(url=f"http://{self.PROXY_USERNAME}:{self.PROXY_PASSWORD}@{self.PROXY_TUNNEL}")
        )
        self.x1 = x1
        self.y1 = y1
        self.x2 = x2
        self.y2 = y2
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
        http_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        }
        api_url = "https://yncx.mnr.gov.cn/dist-app-yn/map/queryResults.json"

        api_params = {
            "returnContent": "true",
            'token': 'Whe67hpdoYaBsTRmdzFkEfWcUyHkFuwQOuKgXDHEOv2deNvj0VbufUWA2w0297kDBDa5T_1V6__VvI1lHY7_fMwl'
        }
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
            api_url,
            headers=http_headers,
            params=api_params,
            json=form_data
        )
        return response.raise_for_status().json().get('data', '')

    async def decrypt_geojson(self) -> bytes:
        """
        :return:          解密后原始字节串
        """
        cipher_hex = await self.get_features()
        logger.info(f"已获取到加密的 GeoJSON 数据, 解密中...")
        if cipher_hex.startswith("04"):  # JS 里先做的那一步
            cipher_hex = cipher_hex[2:]

        content = sm2.CryptSM2(public_key="", private_key=self.SM2_PRIVATE_KEY, mode=1)
        content = content.decrypt(bytes.fromhex(cipher_hex))
        record_sets = json.loads(content.decode("utf-8")).get('recordsets', [])
        for recordset in record_sets:
            if not (feature_list := recordset.get('features')):
                continue
            self.feature_list.extend(feature_list)
        logger.info(f"解密完成，共获取到 {len(self.feature_list)} 个地块数据")

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

    async def download_geojson(self, file_path: str, batch_size: int = 100) -> None:
        """
        下载GeoJSON数据到文件，支持批量处理和并行计算
        Args:
            file_path: 目标文件路径
            batch_size: 批处理大小
        Returns:
            None
        """
        await self.decrypt_geojson()
        logger.info(f"共查询到 {len(self.feature_list)} 个地块数据")
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

    @staticmethod
    def convert_geojson_to_shapefile(input_geojson: str, output_shapefile: str) -> None:

        """
        将GeoJSON格式转换为Shapefile格式

        Args:
            input_geojson: GeoJSON文件路径
            output_shapefile: 输出的Shapefile文件路径

        Returns:
            None
        """
        with fiona.open(input_geojson, 'r') as source:
            with fiona.open(
                    output_shapefile, 'w',
                    driver='ESRI Shapefile',
                    crs=from_epsg(4490),  # 设置坐标系，根据实际情况修改
                    schema=source.schema
            ) as sink:
                for feature in source:
                    sink.write(feature)


async def main():
    # 设置日志级别
    logging.getLogger().setLevel(logging.INFO)
    x1, y1 = 102.0, 24.0  # 左上角经度和纬度
    x2, y2 = 109.0, 20.0  # 右下角经度和纬度
    count = 1000  # 期望返回的结果数量
    geo_output = 'output.geojson'  # 输出的GeoJSON文件路径
    # shp_output = 'artifact'  # 输出的Shapefile文件路径

    async with PermanentBasicFarmlandSpider(x1, y1, x2, y2, count) as spider:
        try:
            logger.info(f"开始查询区域: ({x1}, {y1}) - ({x2}, {y2}) 的永久基本农田数据...")
            await spider.download_geojson(geo_output)
            # 如果指定了Shapefile输出路径，则进行转换
            # if args.shp:
            #     logger.info(f"转换为Shapefile格式: {args.shp}")
            #     spider.convert_geojson_to_shapefile(args.output, args.shp)
            logger.info("处理完成")
        except KeyboardInterrupt:
            logger.info("用户中断操作")
        except HTTPError as e:
            logger.error(f"HTTP请求错误: {str(e)}")
        except Exception as e:
            logger.error(f"处理过程中出现错误: {str(e)}")
            import traceback

            logger.debug(traceback.format_exc())
            raise


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
