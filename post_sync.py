import arcpy


def checkSync(src, dst):
    srcList = [f[0] for f in arcpy.da.SearchCursor(src, 'GUID')]
    if dst.startswith("https://"):
        dstList = [f[0].upper() for f in arcpy.da.SearchCursor(dst, 'GUID')]
    else:
        dstList = [f[0] for f in arcpy.da.SearchCursor(dst, 'GUID')]
    sortedSrc = sorted(srcList)
    sortedDst = sorted(dstList)
    if sortedSrc != sortedDst:
        return False
    else:
        return True