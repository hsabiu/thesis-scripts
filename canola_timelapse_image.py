# Original Author: Javier Garcia
# Modified: Habib Sabiu
# Date: August 24, 2017

# Description: Class implementations for imageFlowerCounterNew.py

import cv2
import operator
import numpy as np

from time import time
from io import StringIO, BytesIO
from PIL import Image, ImageFile
from skimage.feature import blob_doh
from sklearn.cluster import KMeans as skKMeans


class CanolaObject:
    def __str__(self):
        strObject = ''
        for k, v in self.__dict__.items():
            strObject += '{} : {}\n'.format(k, v)
        return strObject


class CanolaPlotRegionObject(CanolaObject):
    def __init__(self):
        self.__id = None
        self.__plot = None
        self.__year = None
        self.__corners = None
        self.__allocatedPlotMask = None
        self.__allocatedRegionMask = None

    def setPlot(self, plot):
        assert self.__plot is None
        self.__plot = plot

    def setYear(self, year):
        assert self.__year is None
        self.__year = year

    def setCorners(self, corners):
        assert self.__corners is None
        self.__corners = corners

    def setId(self, id):
        assert self.__id is None
        self.__id = id

    def setPlotMask(self, plotMask):
        assert self.__allocatedPlotMask is None
        self.__allocatedPlotMask = plotMask

    def setRegionMask(self, regionMask):
        assert self.__allocatedRegionMask is None
        self.__allocatedRegionMask = regionMask


    def getPlot(self):
        return self.__plot

    def getYear(self):
        return self.__year

    def getCorners(self):
        return self.__corners

    def getId(self):
        return self.__id

    def getPlotMask(self):
        return self.__allocatedPlotMask

    def getRegionMask(self):
        return self.__allocatedRegionMask


class CanolaImageClassifierObject(CanolaObject):
    def __init__(self):
        self.__id = None
        self.__imageId = None
        self.__cluster = None
        self.__numberOfYellowPixels = None
        self.__corrupted = False
        self.__regionObject = None

    def setCluster(self, cluster):
        self.__cluster = cluster

    def setRegionObject(self, regionObject):
        self.__regionObject = regionObject

    def setNumberOfYellowPixels(self, numberOfYellowPixels):
        self.__numberOfYellowPixels = numberOfYellowPixels

    def setId(self, id):
        self.__id = id

    def setImageId(self, imageId):
        self.__imageId = imageId

    def flagCorrupted(self):
        self.__corrupted = True


    def getCluster(self):
        return self.__cluster

    def getRegionObject(self):
        return self.__regionObject

    def getNumberOfYellowPixels(self):
        return self.__numberOfYellowPixels

    def getId(self):
        return self.__id

    def getImageId(self):
        return self.__imageId

    def isCorrupted(self):
        return self.__corrupted


class HistogramObject(CanolaObject):
    def __init__(self):
        self.__id = None
        self.__imageId = None
        self.__histogramB = None
        self.__histogramG = None
        self.__histogramBShift = None
        self.__regionObject = None

    def setId(self, id):
        assert self.__id is None
        self.__id = id

    def setImageId(self, imageId):
        assert self.__imageId is None
        self.__imageId = imageId

    def setHistogramB(self, histogramB):
        assert self.__histogramB is None
        self.__histogramB = histogramB

    def setHistogramG(self, histogramG):
        assert self.__histogramG is None
        self.__histogramG = histogramG

    def setHistogramBShift(self, histogramBShift):
        assert self.__histogramBShift is None
        self.__histogramBShift = histogramBShift

    def setRegionObject(self, regionObject):
        assert self.__regionObject is None
        self.__regionObject = regionObject


    def getId(self):
        return self.__id

    def getImageId(self):
        return self.__imageId

    def getHistogramB(self):
        return self.__histogramB

    def getHistogramG(self):
        return self.__histogramG

    def getHistogramBShift(self):
        return self.__histogramBShift

    def getRegionObject(self):
        return self.__regionObject


class PipelineObject(CanolaObject):
    def __init__(self, pipeline=None):
        self.__id = None
        self.__pipeline = pipeline

    def setPipeline(self, pipeline):
        assert self.__pipeline is None
        self.__pipeline = pipeline

    def setId(self, id):
        assert self.__id is None
        self.__id = id


    def getPipeline(self):
        return self.__pipeline

    def getId(self):
        return self.__id


class FlowerCountObject(CanolaObject):
    def __init__(self):
        self.__id = None
        self.__imageId = None
        self.__blobs = None
        self.__regionObject = None
        self.__pipelineObject = PipelineObject(CanolaObject)

    def setBlobs(self, blobs):
        assert self.__blobs is None
        self.__blobs = blobs

    def setRegionObject(self, regionObject):
        assert self.__regionObject is None
        self.__regionObject = regionObject

    def setId(self, id):
        assert self.__id is None
        self.__id = id

    def setImageId(self, imageId):
        assert self.__imageId is None
        self.__imageId = imageId


    def getPipelineObject(self):
        return self.__pipelineObject

    def getBlobs(self):
        return self.__blobs

    def getRegionObject(self):
        return self.__regionObject

    def getId(self):
        return self.__id

    def getImageId(self):
        return self.__imageId


class CanolaTimelapseImage(CanolaObject):
    def __init__(self):
        self.__id = None
        self.__imageId = None
        self.__path = None
        self.__timestamp = None
        self.__imageSize = None
        self.__plot = None
        self.__allocatedImageArray = None
        self.__corrupted = False
        self.__classifierObject = CanolaImageClassifierObject()
        self.__histogramObject = HistogramObject()
        self.__flowerCountObject = FlowerCountObject()

    def readImage(self):
        """
        if self.__allocatedImageArray is None:
            ImageFile.LOAD_TRUNCATED_IMAGES = True
            pil_image = Image.open(self.__path).convert('RGB')
            open_cv_image = np.array(pil_image)
            self.__allocatedImageArray = open_cv_image[:, :, ::-1].copy()
        """
        return self.__allocatedImageArray

    def showDetectedBlobs(self):
        blobs = self.__flowerCountObject.getBlobs()
        assert blobs is not None
        img = np.copy( self.readImage() )
        for x, y, r in blobs:
            cv2.circle(img,center=(int(y), int(x)),radius=int(r),color=(0, 255, 255),thickness=2)
        cv2.imshow("{} {}".format( self.__plot, self.__timestamp ),img )
        cv2.waitKey( 0 )
        cv2.destroyAllWindows()

    def getPlotMask(self):
        regionObject = self.getRegionObject()
        plotMask = regionObject.getPlotMask()

        if plotMask is None:
            regionObject.setPlotMask(Mask.generate(self.__imageSize,regionObject.getCorners()))
        return regionObject.getPlotMask()

    def getRegionMask(self):
        regionObject = self.getRegionObject()
        regionMask = regionObject.getRegionMask()
        if regionMask is None:
            regionManager = RegionManager()
            regionObject.setRegionMask(regionManager.getRegionMask(self.__imageSize,regionObject.getCorners()))
        return regionObject.getRegionMask()

    # SETTERS

    # I added this method for setting image data using Spark
    def setImageArray(self, arrayData):
        assert self.__allocatedImageArray is None
        self.__allocatedImageArray = arrayData       

    def setPath(self, path):
        assert self.__path is None
        self.__path = path

    def setTimestamp(self, timestamp):
        assert self.__timestamp is None
        if isinstance( timestamp, str ):
            timestamp = stringToDatetime( timestamp )
        self.__timestamp = timestamp

    def setImageId(self, imageId):
        assert self.__imageId is None
        self.__imageId = imageId
        self.__classifierObject.setImageId( imageId )
        self.__histogramObject.setImageId( imageId )
        self.__flowerCountObject.setImageId( imageId )

    def setImageSize(self, imageSize):
        assert self.__imageSize is None
        if isinstance( imageSize, str ):
            imageSize = tuple([int(k) for k in imageSize.split('x')[::-1]])
        self.__imageSize = imageSize

    def setPlot(self, plot):
        assert self.__plot is None
        regionObject = self.getRegionObject()
        if regionObject is not None:
            regionPlot = regionObject.getPlot()
            if regionPlot is not None:
                assert plot == regionPlot
        self.__plot = plot

    def setId(self, id):
        assert self.__id is None
        self.__id = id

    def setRegionObject(self, regionObject):
        self.__classifierObject.setRegionObject( regionObject )
        self.__histogramObject.setRegionObject( regionObject )
        self.__flowerCountObject.setRegionObject( regionObject )


    # GETTERS

    def getPath(self):
        return self.__path

    def getTimestamp(self):
        return self.__timestamp

    def getImageId(self):
        return self.__imageId

    def getImageSize(self):
        return self.__imageSize

    def getPlot(self):
        return self.__plot

    def getRegionObject(self):
        return self.__histogramObject.getRegionObject()

    def getPipelineObject(self):
        return self.__flowerCountObject.getPipelineObject()

    def getClassifierObject(self):
        return self.__classifierObject

    def getHistogramObject(self):
        return self.__histogramObject

    def getFlowerCountObject(self):
        return self.__flowerCountObject

    def getId(self):
        return self.__id

    def isCorrupted(self):
        return self.__classifierObject.isCorrupted()


class ImageProcessingModule:

    def processImage( self, imageArray, *argv ):
        assert self._isValidImage(imageArray), self.__class__.__name__
        return self._process( imageArray, *argv )

    def getParamsFromCanolaTimelapseImage(self, canolaTimelapseImage):
        return None

    def _process( self, imageArray, *argv ):
        pass

    def _isValidImage(self, imageArray):
        return isinstance( imageArray, np.ndarray ) and imageArray.dtype == np.uint8


class ImageProcessingPipeline:
    def __init__(self):
        self.__modules = []

    def addModule(self, moduleInstance):
        assert isinstance( moduleInstance, ImageProcessingModule )
        self.__modules.append( moduleInstance )

    def run(self, canolaTimelapseImage):
        res = canolaTimelapseImage.readImage()
        for module in self.__modules:
            res = module.processImage( res,module.getParamsFromCanolaTimelapseImage(canolaTimelapseImage ) )
        return res

    def __str__(self):
        return ','.join([k.__class__.__name__ for k in self.__modules])


class Transformation3D(ImageProcessingModule):
    def _isValidImage(self, imageArray):
        return ImageProcessingModule._isValidImage(self, imageArray) and np.ndim( imageArray ) == 3


class Transformation2D(ImageProcessingModule):
    def _isValidImage(self, imageArray):
        return ImageProcessingModule._isValidImage(self, imageArray) and np.ndim( imageArray ) == 2


class CIELabColorspaceModule(Transformation3D):
    def _process( self, imageArray, *argv ):
        """
        Shift image from BGR to CIELab colorspace and return channel B
        """
        colorspace = cv2.cvtColor( imageArray, cv2.COLOR_BGR2Lab )
        return colorspace[:,:,2]


class OtsuThreshold(Transformation2D):
    def _process( self, imageArray, *argv ):
        """
        Apply Otsu threshold. Output image has zero on pixels with value
        lower than threshold, and their original value otherwise
        """
        blur = cv2.GaussianBlur( imageArray, (5,5), 0 )
        return cv2.threshold( blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU )


class SigmoidMapping(Transformation2D):
    def getParamsFromCanolaTimelapseImage(self, canolaTimelapseImage):
        return canolaTimelapseImage.getHistogramObject().getHistogramBShift()

    def _process( self, imageArray, *argv ):
        """
        Map every pixel's intensity to an output value given by a sigmoid function.
        f(x) = K / ( 1 + e^(t-d*x) )
        """
        yellowThreshMapValue = 0.99
        yellowThreshFromZero = 6
        yellowThreshold = 155

        hist_b_shift = argv[0]
        imageArrayFloat = np.array( imageArray, dtype=np.float32 )
        t_exp = (yellowThreshold + hist_b_shift - yellowThreshFromZero) % 256
        k_exp = np.log(1 / yellowThreshMapValue - 1) / yellowThreshFromZero
        imgAsFloat = 1 / (1 + np.exp(k_exp * (imageArrayFloat - t_exp)))
        return ( imgAsFloat * 255 ).astype( np.uint8 )


class IntensityMapping(Transformation2D):
    def getParamsFromCanolaTimelapseImage(self, canolaTimelapseImage):
        return canolaTimelapseImage.getHistogramObject().getHistogramBShift()

    def _process( self, imageArray, *argv ):
        """
        Perform mapping on grayscale channel to highlight flowers
        """
        hist_b_shift = argv[0]
        imageArrayFloat = np.array( imageArray, dtype=np.float )
        threshold = ( self._params["flowerIntensityThreshold"] + hist_b_shift ) % 256
        imageArrayFloat = np.clip( imageArrayFloat, 0, threshold + 10 )
        imageArrayFloat -= threshold - 8
        imageArrayFloat /= threshold + 10
        imageArrayFloat = np.clip( imageArrayFloat, 0, 1 )
        return np.array( imageArrayFloat, dtype=np.uint8 )


class BlobDetection(Transformation2D):
    def getParamsFromCanolaTimelapseImage(self, canolaTimelapseImage):
        return [canolaTimelapseImage.getPlotMask(),canolaTimelapseImage.getClassifierObject().getNumberOfYellowPixels()]

    def _process( self, imageArray, *argv ):
        """
        Count blobs
        :param *argv: Mask
        :return: List of tuples with (x, y, size) information of every blob detected
        """
        mask = argv[0][0]
        yellowPixels = argv[0][1]
        if yellowPixels == 0:
            return []
        mask = mask > 0
        maskedImage = np.copy( imageArray )
        maskedImage[ mask == 0 ] = 0
        blobs = blob_doh(maskedImage,max_sigma=8,min_sigma=3 )
        return [[int(x), int(y), float(z)] for x, y, z in blobs]


class Mask:
    @staticmethod
    def generate( imageShape, boundCorners ):
        if len( imageShape ) == 3:
            modShape = imageShape[:-1]
        else:
            modShape = imageShape

        def __crossProduct( p1, p2, p3 ):
            v1 = [p2[0] - p1[0], p2[1] - p1[1]]
            v2 = [p3[0] - p2[0], p3[1] - p2[1]]
            return v1[0] * v2[1] - v1[1] * v2[0]

        mask = np.zeros( modShape )
        minX = max( [min( [x[0] for x in boundCorners] ), 0] )
        minY = max( [min( [y[1] for y in boundCorners] ), 0] )
        maxX = min( max( [x[0] for x in boundCorners] ), modShape[1] )
        maxY = min( max( [y[1] for y in boundCorners] ), modShape[0] )

        # Iterate through the containing-square and eliminate points
        # that are out of the ROI
        for x in range( minX, maxX ):
            for y in range( minY, maxY ):
                h1 = __crossProduct( boundCorners[2], boundCorners[0], (x, y) )
                h2 = __crossProduct( boundCorners[3], boundCorners[1], (x, y) )
                v1 = __crossProduct( boundCorners[0], boundCorners[1], (x, y) )
                v2 = __crossProduct( boundCorners[2], boundCorners[3], (x, y) )
                if h1 > 0 > h2 and v1 > 0 > v2:
                    mask[y, x] = 255
        return mask

