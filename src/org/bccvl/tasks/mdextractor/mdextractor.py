import io
import json
import struct
import logging
import mimetypes
import os
import os.path
import uuid
import zipfile

from osgeo import ogr

from .utils import safe_unicode, UnicodeCSVReader


LOG = logging.getLogger(__name__)


# @implementer(IMetadataExtractor)
class MetadataExtractor(object):

    extractors = {}

    def from_string(self, data, mime_type):
        md = None
        # TODO: catch all exceptions here?
        if mime_type in self.extractors:
            md = self.extractors[mime_type].from_string(data)
        return md

    def from_file(self, path, mime_type):
        md = None
        # TODO: catch all exceptions here?
        extractor = self.extractors.get(mime_type)
        if extractor:
            md = extractor.from_file(path)
        else:
            md = {}
        return md

    def from_archive(self, archivepath, path, mime_type):
        md = None
        # TODO: catch all exceptions here?
        if mime_type in self.extractors:
            md = self.extractors[mime_type].from_archive(archivepath, path)
        return md


# TODO: FilesystemExtractor .... size, etc....
class ZipExtractor(object):

    def from_fileob(self, fileob):
        ret = {}
        with zipfile.ZipFile(fileob, 'r') as zipf:
            # TODO: zipfile itself may have some metadata, e.g. comment
            for zipinfo in zipf.infolist():
                if zipinfo.filename.endswith('/'):
                    # skip directories
                    continue
                if zipinfo.filename.startswith('__MACOSX/'):
                    # skip OSX resource forks in zip files
                    continue
                # Is this our speciel bccvl metadata file?
                if zipinfo.filename.endswith('/bccvl/metadata.json'):
                    ret['_bccvlmetadata.json'] = \
                        json.load(zipf.open(zipinfo.filename, 'r'))
                    # skip the rest
                    continue

                # interesting attributes on zipinfo
                #     compress_size, compress_type
                #     comment,
                md = {
                    'filename': zipinfo.filename,
                    'file_size': zipinfo.file_size,
                    'date_time': zipinfo.date_time,  # last mod?
                }
                ret[md['filename']] = md

                # all zip metadata collected, let's look at the data itself
                extractor = MetadataExtractor()
                # TODO: detect mime_type if possible first
                # FIXME: this may be fragile; e.g. if mailcap package is not present, then new zip package ala import will fail
                #        also: should we restrict ourselves to bag it format? (only files inside /data/ will be inspected
                # metadata.json or whatever could then be used to find out more
                # about /data/ files before trying to extract metadata
                mime, enc = mimetypes.guess_type(zipinfo.filename)
                if mime:
                    if mime in ('application/zip', ):
                        # zip in zip ... don't recurse
                        continue
                    ret[md['filename']]['metadata'] = \
                        extractor.from_archive(
                            fileob.name, md['filename'], mime)
                elif zipinfo.filename.endswith('.dbf'):
                    # This handle the shape attribute file to extract the min/max value for each column.
                    # Unzip file so that it can be read
                    # TODO: what about gdb file, or spartial dataset?
                    tmpdir = os.path.split(zipf.filename)[0]
                    zipf.extractall(tmpdir)

                    filepath = os.path.join(tmpdir, zipinfo.filename)
                    # extract min and max from file for each column
                    ds = ogr.Open(filepath)
                    dl = ds.GetLayer(0)
                    ld = dl.GetLayerDefn()
                    layername = ld.GetName()

                    layer_metadata = {}
                    for i in range(ld.GetFieldCount()):
                        fieldname = ld.GetFieldDefn(i).GetName()
                        if fieldname == 'segmentno':
                            continue
                        sql = "select min({name}), max({name}) from {layer}".format(
                            name=fieldname, layer=layername)
                        result = ds.ExecuteSQL(sql)
                        row = result.next()
                        layer_metadata[fieldname] = {
                            'min': row.GetField(0), 'max': row.GetField(1)}
                    ret[md['filename']]['metadata'] = layer_metadata
        return ret

    def from_file(self, path):
        fileob = io.open(path, 'rb')
        return self.from_fileob(fileob)

    def from_string(self, data):
        bytesio = io.BytesIO(data)
        return self.from_fileob(bytesio)


class TiffExtractor(object):

    def _traverseXMP(self, xmp, _schema=u'', name=u''):
        """Generator which yields interesting values."""
        from libxmp.core import XMPIterator
        for xmpitem in XMPIterator(xmp, _schema, name, iter_justchildren=True):
            (schema, name, value, options) = xmpitem
            if options['IS_SCHEMA']:
                if schema == _schema:
                    # we asked for this nod, so skip it.
                    continue
                for subitem in self._traverseXMP(xmp, schema, name):
                    yield subitem
            yield xmpitem

    def from_file(self, path):
        return self._get_gdal_metadata(path)

    def from_string(self, data):
        from osgeo import gdal
        # FIXME: need to add .aux.xml to vsimem as well
        memname = '/vsimem/{0}'.format(str(uuid.uuid4()))
        try:
            gdal.FileFromMemBuffer(memname, data)

            ret = self._get_gdal_metadata(memname)
        finally:
            gdal.Unlink(memname)
        return ret

    def from_archive(self, archivepath, path):
        # use gdals VSI infrastructure to read files from zip file
        vsiname = '/vsizip/{0}/{1}'.format(archivepath, path)
        ret = self._get_gdal_metadata(vsiname)
        return ret

    def _geotransform(self, x, y, geotransform):
        if geotransform:
            geox = geotransform[0] + geotransform[1] * x + geotransform[2] * y
            geoy = geotransform[3] + geotransform[4] * x + geotransform[5] * y
        else:
            geox, geoy = x, y
        return geox, geoy

    def _get_gdal_metadata(self, filename):
        # let's do GDAL here ? if it fails do Hachoir
        vsizip = filename.startswith('/vsizip')
        from osgeo import gdal, osr, gdalconst
        ds = gdal.Open(filename, gdal.GA_ReadOnly if vsizip else gdal.GA_Update)

        # TODO: get bounding box ... Geotransform used to convert from pixel to SRS
        #       geotransform may be None?
        geotransform = ds.GetGeoTransform()
        projref = ds.GetProjectionRef()
        if not projref:
            # default to WGS84
            projref = osr.GetWellKnownGeogCSAsWKT('EPSG:4326')
        spref = osr.SpatialReference(projref)  # SRS
        # extract bbox
        #       see http://svn.osgeo.org/gdal/trunk/gdal/swig/python/samples/gdalinfo.py
        #       GDALInfoReportCorner
        # bbox = left,bottom,right,top
        # bbox = min Longitude , min Latitude , max Longitude , max Latitude
        # bbox in srs units
        # transform points into georeferenced coordinates
        left, top = self._geotransform(0.0, 0.0, geotransform)
        right, bottom = self._geotransform(
            ds.RasterXSize, ds.RasterYSize, geotransform)
        srs = (spref.GetAuthorityName(None),  # 'PROJCS', 'GEOGCS', 'GEOGCS|UNIT', None
               spref.GetAuthorityCode(None))
        if None in srs:
            srs = None
        else:
            srs = '{0}:{1}'.format(*srs)

        # build metadata struct
        data = {
            'size': (ds.RasterXSize, ds.RasterYSize),
            'bands': ds.RasterCount,
            'projection': projref,  # WKT
            'srs': srs,
            'origin': (geotransform[0], geotransform[3]),
            'Pxiel Size': (geotransform[1], geotransform[5]),
            'bounds': {'left': left, 'bottom': bottom,
                       'right': right, 'top': top}
        }
        data.update(ds.GetMetadata_Dict())
        data.update(ds.GetMetadata_Dict('EXIF'))
        from libxmp.core import XMPMeta
        xmp = ds.GetMetadata('xml:XMP') or []
        if xmp:
            data['xmp'] = {}
        for xmpentry in xmp:
            xmpmd = XMPMeta()
            xmpmd.parse_from_str(xmpentry)
            for xmpitem in self._traverseXMP(xmpmd):
                (schema, name, value, options) = xmpitem
                if options['IS_SCHEMA']:
                    continue
                if options['ARRAY_IS_ALT']:
                    # pick first element and move on
                    data['xmp'][name] = xmpmd.get_array_item(schema, name, 1)
                    continue
                # current item

                # ARRAY_IS_ALT .. ARRAY_IS_ALT_TEXT, pick first one (value is
                # array + array is ordered)

                # -> array elements don't have special markers :(

                if options['ARRAY_IS_ALT']:
                    pass
                if options['HAS_LANG']:
                    pass
                if options['VALUE_IS_STRUCT']:
                    pass
                if options['ARRAY_IS_ALTTEXT']:
                    pass
                if options['VALUE_IS_ARRAY']:
                    pass
                if options['ARRAY_IS_ORDERED']:
                    pass

                #     -> ALT ARRAY_VALUE???
                # if options['VALUE_IS_ARRAY']:

                # else:
                data['xmp'][name] = value

        # EXIF could provide at least:
        #   width, height, bistpersample, compression, planarconfiguration,
        #   sampleformat, xmp-metadata (already parsed)

        # TODO: get driver metadata?
        #     ds.GetDriver().getMetadata()
        #     ds.GetDriver().ds.GetMetadataItem(gdal.DMD_XXX)

        # Extract GDAL metadata
        for numband in range(1, ds.RasterCount + 1):
            band = ds.GetRasterBand(numband)
            try:
                if gdal.GetDataTypeName(band.DataType) == 'Float32':
                    # Convert nodata value to float32, and update statistic and save it.
                    nodatavalue = band.GetNoDataValue()
                    if nodatavalue:
                        nodatavalue, = struct.unpack('f', struct.pack('f', nodatavalue))
                        band.SetNoDataValue(nodatavalue)
                        (min_, max_, mean, stddev) = band.ComputeStatistics(False)
                        band.SetStatistics(min_, max_, mean, stddev)
                        # Can save the file only if it is not a vsizip file
                        if not vsizip:
                            ds.FlushCache()
            except Exception:
                pass

            (min_, max_, mean, stddev) = band.ComputeStatistics(False)
            banddata = {
                'data type': gdal.GetDataTypeName(band.DataType),
                # band.GetRasterColorTable().GetCount() ... color table with
                # count entries
                'min': min_,
                'max': max_,
                'mean': mean,
                'stddev': stddev,
                'color interpretation': gdal.GetColorInterpretationName(band.GetColorInterpretation()),
                'description': band.GetDescription(),
                'nodata': band.GetNoDataValue(),
                'size': (band.XSize, band.YSize),
                'index': band.GetBand(),
                # band.GetCategoryNames(), GetRasterCategoryNames() .. ?
                # band.GetScale()
            }
            banddata.update(band.GetMetadata())
            if 'band' not in data:
                data['band'] = []
            data['band'].append(banddata)

            # extract Raster Attribute table (if any)
            rat = band.GetDefaultRAT()

            def _getColValue(rat, row, col):
                valtype = rat.GetTypeOfCol(col)
                if valtype == gdalconst.GFT_Integer:
                    return rat.GetValueAsInt(row, col)
                if valtype == gdalconst.GFT_Real:
                    return rat.GetValueAsDouble(row, col)
                if valtype == gdalconst.GFT_String:
                    return rat.GetValueAsString(row, col)
                return None

            GFU_MAP = {
                gdalconst.GFU_Generic: 'Generic',
                gdalconst.GFU_Max: 'Max',
                gdalconst.GFU_MaxCount: 'MaxCount',
                gdalconst.GFU_Min: 'Min',
                gdalconst.GFU_MinMax: 'MinMax',
                gdalconst.GFU_Name: 'Name',
                gdalconst.GFU_PixelCount: 'PixelCount',
                gdalconst.GFU_Red: 'Red',
                gdalconst.GFU_Green: 'Green',
                gdalconst.GFU_Blue: 'Blue',
                gdalconst.GFU_Alpha: 'Alpha',
                gdalconst.GFU_RedMin: 'RedMin',
                gdalconst.GFU_GreenMin: 'GreenMin',
                gdalconst.GFU_BlueMin: 'BlueMin',
                gdalconst.GFU_AlphaMax: 'AlphaMin',
                gdalconst.GFU_RedMax: 'RedMax',
                gdalconst.GFU_GreenMax: 'GreenMax',
                gdalconst.GFU_BlueMin: 'BlueMax',
                gdalconst.GFU_AlphaMax: 'AlphaMax',
            }

            GFT_MAP = {
                gdalconst.GFT_Integer: 'Integer',
                gdalconst.GFT_Real: 'Real',
                gdalconst.GFT_String: 'String',
            }

            if rat:
                banddata['rat'] = {
                    'rows': [
                        [_getColValue(rat, rowidx, colidx)
                         for colidx in range(0, rat.GetColumnCount())]
                        for rowidx in range(0, rat.GetRowCount())
                    ],
                    'cols': [
                        {'name': rat.GetNameOfCol(idx),
                         'type': GFT_MAP[rat.GetTypeOfCol(idx)],
                         'usage': GFU_MAP[rat.GetUsageOfCol(idx)],
                         'idx': idx} for idx in range(0, rat.GetColumnCount())],
                }
                # Assume if there is a RAT we have categorical data
                banddata['type'] = 'categorical'
            else:
                banddata['type'] = 'continuous'

        ds = None

        # HACHOIR Tif extractor:
        # ret = {}
        # for field in parser:
        #     if field.name.startswith('ifd'):
        #         data = {
        #           'img_height': field['img_height']['value'].value,
        #           'img_width': field['img_width']['value'].value,
        #           'bits_per_sample': field['bits_per_sample']['value'].value,
        #           'compression': field['compression']['value'].display
        #         }
        #         ret = data
        return data


class CSVExtractor(object):

    def from_fileob(self, stringio):
        # CSV always expects ifle object that returns unicode???
        csvreader = UnicodeCSVReader(stringio)
        headers = csvreader.next()
        bounds = {
            'bottom': float('Inf'),
            'left': float('Inf'),
            'top': float("-Inf"),
            'right': float("-Inf"),
        }
        species = set()
        data = {}
        count = 0
        if 'lat' in headers and 'lon' in headers:
            # extract md about occurrences -> generate bounding box
            lonidx = headers.index('lon') if 'lon' in headers else None
            latidx = headers.index('lat')
            speciesidx = None
            if 'species' in headers:
                speciesidx = headers.index('species')
            for row in csvreader:
                if not row:
                    # there could be an empty row at end of file
                    continue
                # we count only rows with data
                count += 1
                try:
                    lat, lon = float(row[latidx]), float(row[lonidx])
                    bounds.update(
                        bottom=min(lat, bounds['bottom']),
                        left=min(lon, bounds['left']),
                        top=max(lat, bounds['top']),
                        right=max(lon, bounds['right'])
                    )
                except Exception as e:
                    LOG.error(e, exc_info=True)
                    raise Exception(
                        "Invalid lat/lon value at line {}".format(count))
                if speciesidx is not None:
                    species.add(row[speciesidx])

            data['rows'] = count
            data['species'] = list(species)
            data['bounds'] = bounds

        data.update({
            'headers': [safe_unicode(h) for h in headers],
        })

        return data

    def from_file(self, path):
        csvfile = io.open(path, 'r', encoding='utf-8', errors='ignore')
        return self.from_fileob(csvfile)

    def from_string(self, data):
        # collect header names, and number of rows.
        # assume we have occurrence / absence file.
        # and find bounding box coordinates
        csvfile = io.StringIO(data.decode('utf-8'))
        return self.from_fileob(csvfile)

    def from_archive(self, archivepath, path):
        with zipfile.ZipFile(archivepath, mode='r') as zf:
            fileobj = io.TextIOWrapper(io.BufferedReader(
                zf.open(path, mode='rU')), encoding='utf-8', errors='ignore')
            return self.from_fileob(fileobj)


class HachoirExtractor(object):

    def from_string(self, data):
        from hachoir_parser import guessParser
        from hachoir_core.stream import StringInputStream
        stream = StringInputStream(data)
        parser = guessParser(stream)
        from hachoir_metadata import extractMetadata
        ret = extractMetadata(parser)
        # formated = md.exportPlaintext(line_prefix=u"")
        return ret


MetadataExtractor.extractors = {
    'application/octet-stream': None,
    # basic file system extractor or use Hachoir to auto-detect?
    'application/zip': ZipExtractor(),
    'image/tiff': TiffExtractor(),
    'image/geotiff': TiffExtractor(),
    'text/csv': CSVExtractor(),
    'text/comma-separated-values': CSVExtractor(),
}
