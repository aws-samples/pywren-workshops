import os
import re
import datetime

from urllib2 import urlopen

import numpy as np

import rasterio as rio
from rasterio.enums import Resampling

from rio_toa import toa_utils


def get_colormap():
    """
    """
    cmap_file = os.path.join(os.path.dirname(__file__), 'cmap.txt')
    with open(cmap_file) as cmap:
        lines = cmap.read().splitlines()
        colormap = [list(map(int, line.split()))
                    for line in lines if not line.startswith('#')][1:]

    return colormap


def get_overview(address, ovrSize):
    """
    """
    with rio.open(address) as src:
        matrix = src.read(indexes=1, out_shape=(
            ovrSize, ovrSize), resampling=Resampling.bilinear).astype(src.profile['dtype'])

    return matrix


def linear_rescale(image, in_range=[0, 16000], out_range=[1, 255]):
    """
    Linear rescaling
    """

    imin, imax = in_range
    omin, omax = out_range
    image = np.clip(image, imin, imax) - imin
    image = image / float(imax - imin)

    return (image * (omax - omin) + omin)


def landsat_get_mtl(sceneid):
    """Get Landsat-8 MTL metadata

    Attributes
    ----------

    sceneid : str
        Landsat sceneid. For scenes after May 2017,
        sceneid have to be LANDSAT_PRODUCT_ID.

    Returns
    -------
    out : dict
        returns a JSON like object with the metadata.
    """

    try:
        scene_params = landsat_parse_scene_id(sceneid)
        meta_file = 'http://landsat-pds.s3.amazonaws.com/{}_MTL.txt'.format(
            scene_params['key'])
        metadata = str(urlopen(meta_file).read().decode())
        return toa_utils._parse_mtl_txt(metadata)
    except:
        raise Exception('Could not retrieve {} metadata'.format(sceneid))


def landsat_parse_scene_id(sceneid):
    """
    Author @perrygeo - http://www.perrygeo.com

    parse scene id
    """

    if not re.match('^(L[COTEM]8\d{6}\d{7}[A-Z]{3}\d{2})|(L[COTEM]08_L\d{1}[A-Z]{2}_\d{6}_\d{8}_\d{8}_\d{2}_(T1|T2|RT))$', sceneid):
        raise ValueError('Could not match' + sceneid)

    precollection_pattern = (
        r'^L'
        r'(?P<sensor>\w{1})'
        r'(?P<satellite>\w{1})'
        r'(?P<path>[0-9]{3})'
        r'(?P<row>[0-9]{3})'
        r'(?P<acquisitionYear>[0-9]{4})'
        r'(?P<acquisitionJulianDay>[0-9]{3})'
        r'(?P<groundStationIdentifier>\w{3})'
        r'(?P<archiveVersion>[0-9]{2})$')

    collection_pattern = (
        r'^L'
        r'(?P<sensor>\w{1})'
        r'(?P<satellite>\w{2})'
        r'_'
        r'(?P<processingCorrectionLevel>\w{4})'
        r'_'
        r'(?P<path>[0-9]{3})'
        r'(?P<row>[0-9]{3})'
        r'_'
        r'(?P<acquisitionYear>[0-9]{4})'
        r'(?P<acquisitionMonth>[0-9]{2})'
        r'(?P<acquisitionDay>[0-9]{2})'
        r'_'
        r'(?P<processingYear>[0-9]{4})'
        r'(?P<processingMonth>[0-9]{2})'
        r'(?P<processingDay>[0-9]{2})'
        r'_'
        r'(?P<collectionNumber>\w{2})'
        r'_'
        r'(?P<collectionCategory>\w{2})$')

    meta = None
    for pattern in [collection_pattern, precollection_pattern]:
        match = re.match(pattern, sceneid, re.IGNORECASE)
        if match:
            meta = match.groupdict()
            break

    if not meta:
        raise ValueError('Could not match' + sceneid)

    if meta.get('acquisitionJulianDay'):
        date = datetime.datetime(int(meta['acquisitionYear']), 1, 1) \
            + datetime.timedelta(int(meta['acquisitionJulianDay']) - 1)

        meta['date'] = date.strftime('%Y-%m-%d')
    else:
        meta['date'] = meta.get("acquisitionYear") + '-' + \
            meta.get("acquisitionMonth") + '-' + meta.get("acquisitionDay")

    collection = meta.get('collectionNumber', '')
    if collection != '':
        collection = 'c'+str(int(collection))

    meta['key'] = os.path.join(
        collection, 'L8', meta['path'], meta['row'], sceneid, sceneid)
    meta['scene'] = sceneid

    return meta


def sentinel_parse_scene_id(sceneid):
    """
    parse scene id
    """

    if not re.match('^S2[AB]_tile_[0-9]{8}_[0-9]{2}[A-Z]{3}_[0-9]$', sceneid):
        raise ValueError('Could not match ' + sceneid)

    sentinel_pattern = (
        r'^S'
        r'(?P<sensor>\w{1})'
        r'(?P<satellite>[AB]{1})'
        r'_tile_'
        r'(?P<acquisitionYear>[0-9]{4})'
        r'(?P<acquisitionMonth>[0-9]{2})'
        r'(?P<acquisitionDay>[0-9]{2})'
        r'_'
        r'(?P<utm>[0-9]{2})'
        r'(?P<sq>\w{1})'
        r'(?P<lat>\w{2})'
        r'_'
        r'(?P<num>[0-9]{1})$')

    meta = None
    match = re.match(sentinel_pattern, sceneid, re.IGNORECASE)
    if match:
        meta = match.groupdict()

    if not meta:
        raise ValueError('Could not match {sceneid}')

    utm = meta['utm']
    sq = meta['sq']
    lat = meta['lat']
    year = meta['acquisitionYear']
    m = meta['acquisitionMonth'].lstrip("0")
    d = meta['acquisitionDay'].lstrip("0")
    n = meta['num']

    meta['key'] = 'tiles/' + utm + '/' + sq + '/' + \
        lat + '/' + year + '/' + m + '/' + d + '/' + n

    return meta
