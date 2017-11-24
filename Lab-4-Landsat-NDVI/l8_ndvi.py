"""remotepixel.l8_ndvi"""

import os.path
import base64
from io import BytesIO

import numpy as np
from PIL import Image
import boto3
import botocore

import rasterio as rio
from rasterio import warp
from rasterio.enums import Resampling
from rio_toa.reflectance import reflectance


import rputils
from pywren import wrenutil


np.seterr(divide='ignore', invalid='ignore')

LANDSAT_BUCKET = 's3://landsat-pds'


def thumb(scene, coord):
    """
    """
    try:
        scene_params = rputils.landsat_parse_scene_id(scene)
        band_address = 'http://landsat-pds.s3.amazonaws.com/' + \
            scene_params["key"] + '_thumb_small.jpg'
        return band_address
    except:
        return ''


def point(scene, coord):
    """
    """

    try:
        scene_params = rputils.landsat_parse_scene_id(scene)
        meta_data = rputils.landsat_get_mtl(scene).get('L1_METADATA_FILE')

        sun_elev = meta_data['IMAGE_ATTRIBUTES']['SUN_ELEVATION']

        multi_reflect = meta_data['RADIOMETRIC_RESCALING']['REFLECTANCE_MULT_BAND_4']
        add_reflect = meta_data['RADIOMETRIC_RESCALING']['REFLECTANCE_ADD_BAND_4']
        band_address = scene_params["key"] + '_B4.TIF'
        s3 = boto3.resource('s3', 'us-west-2')
        # if not os.path.exists('/tmp/'+scene+'_B4.TIF'):
        #    s3.Bucket('landsat-pds').download_file(band_address, '/tmp/'+scene+'_B4.TIF')

        with rio.open('s3://landsat-pds/' + band_address) as band:
            lon_srs, lat_srs = warp.transform(
                'EPSG:4326', band.crs, [coord[0]], [coord[1]])
            b4 = list(band.sample([(lon_srs[0], lat_srs[0])]))[0]
            b4 = reflectance(b4, multi_reflect, add_reflect,
                             sun_elev, src_nodata=0)[0]

        multi_reflect = meta_data['RADIOMETRIC_RESCALING']['REFLECTANCE_MULT_BAND_5']
        add_reflect = meta_data['RADIOMETRIC_RESCALING']['REFLECTANCE_ADD_BAND_5']
        band_address = scene_params["key"] + '_B5.TIF'

        #s3_object = s3.get_object(Bucket='landsat-pds', Key=band_address)
        #f = s3_object['Body'].read()
        # if  not os.path.exists('/tmp/'+scene+'_B5.TIF'):
        #    s3.Bucket('landsat-pds').download_file(band_address, '/tmp/'+scene+'_B5.TIF')

        with rio.open('s3://landsat-pds/' + band_address) as band:
            lon_srs, lat_srs = warp.transform(
                'EPSG:4326', band.crs, [coord[0]], [coord[1]])
            b5 = list(band.sample([(lon_srs[0], lat_srs[0])]))[0]
            b5 = reflectance(b5, multi_reflect, add_reflect,
                             sun_elev, src_nodata=0)[0]

        ratio = np.nan_to_num((b5 - b4) / (b5 + b4)) if (b4 * b5) > 0 else 0.

        out = {
            'scene': scene,
            'ndvi': ratio,
            'date': scene_params['date'],
            'cloud': meta_data['IMAGE_ATTRIBUTES']['CLOUD_COVER']}

        return out
    except:
        return {}


def area(scene, bbox):
    """
    """

    max_width = 512
    max_height = 512

    scene_params = rputils.landsat_parse_scene_id(scene)
    meta_data = rputils.landsat_get_mtl(scene).get('L1_METADATA_FILE')

    sun_elev = meta_data['IMAGE_ATTRIBUTES']['SUN_ELEVATION']

    multi_reflect = meta_data['RADIOMETRIC_RESCALING']['REFLECTANCE_MULT_BAND_4']
    add_reflect = meta_data['RADIOMETRIC_RESCALING']['REFLECTANCE_ADD_BAND_4']
    band_address = scene_params["key"] + '_B4.TIF'

    s3 = boto3.resource('s3', 'us-west-2')

    if not os.path.exists('/tmp/' + scene + '_B4.TIF'):
        s3.Bucket('landsat-pds').download_file(band_address,
                                               '/tmp/' + scene + '_B4.TIF')

    with rio.open('/tmp/' + scene + '_B4.TIF') as band:
        crs_bounds = warp.transform_bounds('EPSG:4326', band.crs, *bbox)
        window = band.window(*crs_bounds)

        width = round(window.width) if window.width < max_width else max_width
        height = round(
            window.height) if window.height < max_height else max_height

        b4 = band.read(window=window, out_shape=(height, width),
                       indexes=1, resampling=Resampling.bilinear, boundless=True)
        b4 = reflectance(b4, multi_reflect, add_reflect,
                         sun_elev, src_nodata=0)

    multi_reflect = meta_data['RADIOMETRIC_RESCALING']['REFLECTANCE_MULT_BAND_5']
    add_reflect = meta_data['RADIOMETRIC_RESCALING']['REFLECTANCE_ADD_BAND_5']
    band_address = scene_params["key"] + '_B5.TIF'

    if not os.path.exists('/tmp/' + scene + '_B5.TIF'):
        s3.Bucket('landsat-pds').download_file(band_address,
                                               '/tmp/' + scene + '_B5.TIF')

    with rio.open('/tmp/' + scene + '_B5.TIF') as band:
        crs_bounds = warp.transform_bounds('EPSG:4326', band.crs, *bbox)
        window = band.window(*crs_bounds)

        width = round(window.width) if window.width < max_width else max_width
        height = round(
            window.height) if window.height < max_height else max_height

        b5 = band.read(window=window, out_shape=(height, width),
                       indexes=1, resampling=Resampling.bilinear, boundless=True)
        b5 = reflectance(b5, multi_reflect, add_reflect,
                         sun_elev, src_nodata=0)

    ratio = np.where(
        (b5 * b4) > 0, np.nan_to_num((b5 - b4) / (b5 + b4)), -9999)
    ratio = np.where(ratio > -9999, rputils.linear_rescale(ratio,
                                                           in_range=[-1, 1], out_range=[1, 255]), 0).astype(np.uint8)

    cmap = list(np.array(rputils.get_colormap()).flatten())
    img = Image.fromarray(ratio, 'P')
    img.putpalette(cmap)
    img = img.convert('RGB')

    sio = BytesIO()
    img.save(sio, 'jpeg', subsampling=0, quality=100)
    sio.seek(0)

    return base64.b64encode(sio.getvalue()).decode()
