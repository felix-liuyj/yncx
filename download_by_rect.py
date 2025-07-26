"""
永久基本农田数据查询与下载工具

此模块提供了基于矩形区域查询并下载永久基本农田数据的功能。
支持将数据保存为GeoJSON格式以及转换为Shapefile格式。
"""
import json
import os
import logging
from typing import Dict, List, Union, Tuple, Optional, Any

import fiona
from fiona.crs import from_epsg
from gmssl import sm2
from httpx import Client, HTTPError
from shapely.geometry import Polygon
from shapely.geometry import mapping

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# HTTP请求配置
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
}
API_URL = "https://yncx.mnr.gov.cn/dist-app-yn/map/queryResults.json"

API_PARAMS = {
    "returnContent": "true",
    'token': 'Whe67hpdoYaBsTRmdzFkEfWcUyHkFuwQOuKgXDHEOv2deNvj0VbufUWA2w0297kDBDa5T_1V6__VvI1lHY7_fMwl'
}


def get_features(x1: float, y1: float, x2: float, y2: float, n: int) -> str:
    """
    根据矩形范围查询永久基本农田数据
    
    Args:
        x1: 左上角经度
        y1: 左上角纬度
        x2: 右下角经度
        y2: 右下角纬度
        n: 期望返回的结果数量
    
    Returns:
        加密的GeoJSON数据字符串
        
    Raises:
        HTTPError: 当HTTP请求失败时抛出
    """
    form_data = {
        'queryMode': 'SpatialQuery',
        'queryParameters': {
            'prjCoordSys': {'epsgCode': 4490},
            'expectCount': n,
            'queryParams': [{'name': "pro31@yndk", 'attributeFilter': "1=1", 'fields': ['yjjbntmj', 'yjjbnttbbh']}],
            'startRecord': 0,
        },
        'geometry': {
            'id': None,
            'style': None,
            'parts': [5],
            'points': [
                {'CLASS_NAME': 'SuperMap.Geometry.Point', 'id': "SuperMap.Geometry_1", 'bounds': None, 'SRID': None,
                 'x': x1, 'y': y1, 'tag': None, 'type': "Point", 'geometryType': "Point"},
                {'CLASS_NAME': 'SuperMap.Geometry.Point', 'id': "SuperMap.Geometry_2", 'bounds': None, 'SRID': None,
                 'x': x2, 'y': y1, 'tag': None, 'type': "Point", 'geometryType': "Point"},
                {'CLASS_NAME': 'SuperMap.Geometry.Point', 'id': "SuperMap.Geometry_3", 'bounds': None, 'SRID': None,
                 'x': x2, 'y': y2, 'tag': None, 'type': "Point", 'geometryType': "Point"},
                {'CLASS_NAME': 'SuperMap.Geometry.Point', 'id': "SuperMap.Geometry_4", 'bounds': None, 'SRID': None,
                 'x': x1, 'y': y2, 'tag': None, 'type': "Point", 'geometryType': "Point"},
                {'CLASS_NAME': 'SuperMap.Geometry.Point', 'id': "SuperMap.Geometry_5", 'bounds': None, 'SRID': None,
                 'x': x1, 'y': y1, 'tag': None, 'type': "Point", 'geometryType': "Point"}
            ],
            'type': "REGION",
            'prjCoordSys': {'epsgCode': 4490}
        },
        'spatialQueryMode': "INTERSECT"
    }
    with Client() as client:
        try:
            # 使用httpx.Client发送POST请求
            response = client.post(
                API_URL, 
                headers=HTTP_HEADERS, 
                params=API_PARAMS, 
                json=form_data
            ).raise_for_status().json()
            return response.get('data', '')
        except HTTPError as e:
            logger.error(f"请求失败: {str(e)}")
            raise


def decrypt_geojson(cipher_hex: str, key: str, mode: int = 1) -> bytes:
    """
    :param key:     16 进制 SM2 私钥
    :param cipher_hex: 16 进制密文（可能前缀 '04'）
    :param mode:      1 = C1C2C3（gmssl 默认），0 = C1C3C2
    :return:          解密后原始字节串
    """
    if cipher_hex.startswith("04"):  # JS 里先做的那一步
        cipher_hex = cipher_hex[2:]

    content = sm2.CryptSM2(public_key="", private_key=key, mode=mode)
    content = content.decrypt(bytes.fromhex(cipher_hex))
    return json.loads(content.decode("utf-8"))


def download_geojson(file_path: str, features: List[Dict[str, Any]]) -> None:
    """
    下载GeoJSON数据到文件

    Args:
        file_path: 目标文件路径
        features: GeoJSON特征列表

    Returns:
        None
    """
    def create_polygon(feature: Dict[str, Any]) -> Polygon:  # 处理单个几何
        parts = feature['geometry']['parts']
        points = feature['geometry']['points']
        start_index = 0
        exterior_ring = []
        interior_rings = []

        for part in parts:
            end_index = start_index + part
            ring = [(points[i]['x'], points[i]['y']) for i in range(start_index, end_index)]

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

    # 检查文件是否存在，如果不存在则创建文件并写入初始数据
    if not os.path.exists(file_path):
        with fiona.open(file_path, 'w', driver='GeoJSON', crs='EPSG:4490',
                        schema={'geometry': 'Polygon', 'properties': {'ID': 'int'}}) as file:
            for feature in features:
                file.write({
                    'properties': {'ID': feature['ID']},
                    'geometry': mapping(create_polygon(feature))
                })

    # 如果文件已经存在则增量保存，通过ID去重
    else:
        existing_ids = []
        with fiona.open(file_path, 'r') as file:
            for feature in file:
                existing_ids.append(feature['properties']['ID'])
        with fiona.open(file_path, 'a') as file:
            for feature in features:
                if feature['ID'] not in existing_ids:
                    file.write({
                        'properties': {'ID': feature['ID']},
                        'geometry': mapping(create_polygon(feature))
                    })


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


if __name__ == "__main__":
    # 默认最大查询1000片，建议不要修改
    expectCount = 1000

    # 保存文件路径
    file_path_geojson = '2.geojson'

    # 默认按矩形查找，传入左上与右下坐标，建议查询范围不要太大，爱护服务器
    x1 = 120.63954
    y1 = 32.43703
    x2 = 120.6456
    y2 = 32.43089

    key = '3c444bddc35163466de81734157430c459d77624333d1daa61f5c22abd890dd0'  # 请自行网页中查找, index js 使用 generateKeyPairHex 查询(function dw)

    try:
        logger.info("开始查询永久基本农田数据...")
        encrypted_data = get_features(x1, y1, x2, y2, expectCount)
        
        logger.info("解密数据...")
        features = decrypt_geojson(encrypted_data, key)
        
        logger.info(f"下载数据到 {file_path_geojson}...")
        download_geojson(file_path_geojson, features)
        
        logger.info(f"共处理 {len(features)} 个地块数据")
        
        # 输出为shp格式
        # file_path_shp='1.shp'
        # logger.info(f"转换为Shapefile格式: {file_path_shp}")
        # convert_geojson_to_shapefile(file_path_geojson, file_path_shp)
        
        logger.info("处理完成")
    except Exception as e:
        logger.error(f"处理过程中出现错误: {str(e)}")
        raise
