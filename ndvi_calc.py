import rasterio
from matplotlib import pyplot as plt
import numpy as np
from time import time


with rasterio.open("LC09_L1TP_190024_20211225_20220123_02_T1_B4.TIF") as tif:
    band_red = tif.read(1)
    plt.imshow(band_red, cmap='pink')
    plt.show()

with rasterio.open("LC09_L1TP_190024_20211225_20220123_02_T1_B5.TIF") as tif:
    band_nir = tif.read(1)
    plt.imshow(band_nir, cmap='pink')
    plt.show()

# Allow division by zero
np.seterr(divide='ignore', invalid='ignore')

# Calculate NDVI
start = time()
ndvi = (band_nir.astype(float) - band_red.astype(float)) / (band_nir + band_red)
end = time()

# print(ndvi)
plt.imshow(ndvi, cmap='summer')
plt.show()
# counter = 0  # ilość pixeli, które nie są None, NaN, 0 itp.
# summ = 0.0  # suma wartości każdego pixela (suma ndvi)
# for row in ndvi:
#     for el in row:
#         if el > 0.0:
#             counter += 1
#             summ += el
# print(summ, counter)
# print(np.shape(ndvi)[0] * np.shape(ndvi)[1])  # ilość pixeli w zdjęciu
print(end - start)

