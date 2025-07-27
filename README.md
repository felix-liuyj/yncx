# 永久基本农田数据查询与下载工具

## 功能说明

此工具用于查询并下载指定矩形区域内的永久基本农田空间数据，支持保存为GeoJSON格式和Shapefile格式。

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

### 基本用法

```bash
python rect_spider.py --x1 120.63954 --y1 32.43703 --x2 120.6456 --y2 32.43089 --output data.geojson
```

### 同时输出为Shapefile格式

```bash
python rect_spider.py --x1 120.63954 --y1 32.43703 --x2 120.6456 --y2 32.43089 --output data.geojson --shp data.shp
```

### 调整批处理大小

```bash
python rect_spider.py --batch-size 200
```

### 查看调试日志

```bash
python rect_spider.py --log-level DEBUG
```

## 参数说明

| 参数 | 类型 | 默认值 | 描述 |
|------|------|--------|------|
| --x1 | float | 120.63954 | 左上角经度 |
| --y1 | float | 32.43703 | 左上角纬度 |
| --x2 | float | 120.6456 | 右下角经度 |
| --y2 | float | 32.43089 | 右下角纬度 |
| --output, -o | string | output.geojson | 输出的GeoJSON文件路径 |
| --shp | string | 无 | 输出的Shapefile文件路径，如不提供则不转换 |
| --key | string | (默认值) | SM2解密私钥 |
| --count | int | 1000 | 期望返回的结果数量 |
| --batch-size | int | 100 | 批处理大小 |
| --log-level | string | INFO | 日志级别 (DEBUG, INFO, WARNING, ERROR) |