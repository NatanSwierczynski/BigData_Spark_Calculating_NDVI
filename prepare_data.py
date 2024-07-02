from osgeo.gdalnumeric import *
import json
import numpy as np
import os


def get_paths(dspath: str, yrs: list) -> dict:
    files_per_year = {}
    for year in yrs:
        all_filepaths = os.listdir(f"{dspath}/{year}")
        bnd2 = f'{dspath}/{year}/{[fp for fp in all_filepaths if "B2.TIF" in fp][0]}'
        bnd3 = f'{dspath}/{year}/{[fp for fp in all_filepaths if "B3.TIF" in fp][0]}'
        bnd4 = f'{dspath}/{year}/{[fp for fp in all_filepaths if "B4.TIF" in fp][0]}'
        bnd5 = f'{dspath}/{year}/{[fp for fp in all_filepaths if "B5.TIF" in fp][0]}'
        confs = f'{dspath}/{year}/{[fp for fp in all_filepaths if "MTL.json" in fp][0]}'
        files_per_year[year] = {
            "band2": bnd2,
            "band3": bnd3,
            "band4": bnd4,
            "band5": bnd5,
            "confs": confs
        }
    return files_per_year


def normalize(band: np.ndarray) -> np.ndarray:
    band_min = band.min()
    band_max = band.max()
    return (band-band_min)/(band_max - band_min)


dataset_path = "D:/SatelliteImagesBIGDATA"

years = os.listdir(dataset_path)
filepaths_per_year = get_paths(dspath=dataset_path, yrs=years)

for year in filepaths_per_year.keys():
    band2 = gdal.Open(filepaths_per_year[year]["band2"])
    band3 = gdal.Open(filepaths_per_year[year]["band3"])
    band4 = gdal.Open(filepaths_per_year[year]["band4"])
    band5 = gdal.Open(filepaths_per_year[year]["band5"])

    # Miniaturki
    band2_gdal = gdal.Warp(f"warp/band2_small{year}.tif", band2, xRes=128, yRes=128)
    band3_gdal = gdal.Warp(f"warp/band3_small{year}.tif", band3, xRes=128, yRes=128)
    band4_gdal = gdal.Warp(f"warp/band4_small{year}.tif", band4, xRes=128, yRes=128)

    # Podstawowe dane o warstwie
    xsize = band3.RasterXSize
    ysize = band3.RasterYSize

    # Zamiana pliku tif na tablice numpy array
    band5_arr = band5.ReadAsArray().astype('float32')
    band4_arr = band4.ReadAsArray().astype('float32')
    blue_arr = band2_gdal.GetRasterBand(1).ReadAsArray()
    green_arr = band3_gdal.GetRasterBand(1).ReadAsArray()
    red_arr = band4_gdal.GetRasterBand(1).ReadAsArray()

    # Ostateczna warstwa
    r_normalized = normalize(red_arr)
    g_normalized = normalize(green_arr)
    b_normalized = normalize(blue_arr)

    # spłaszczenie tablic
    band5_flat = band5_arr.flatten()
    band4_flat = band4_arr.flatten()

    variables_dict = {
        "xsize": xsize,
        "ysize": ysize,
    }
    variables_json = json.dumps(variables_dict)
    try:
        os.mkdir(f"first-stage-output/{year}")
    except FileExistsError as err:
        print(err)
    with open(f"first-stage-output/{year}/variables.json", "w") as f:
        f.write(variables_json)

    np.save(f"first-stage-output/{year}/band5_flat.npy", band5_flat)
    np.save(f"first-stage-output/{year}/band4_flat.npy", band4_flat)

    np.save(f"first-stage-output/{year}/blue_band.npy", b_normalized)
    np.save(f"first-stage-output/{year}/green_band.npy", g_normalized)
    np.save(f"first-stage-output/{year}/red_band.npy", r_normalized)

    # hdfs_client = InsecureClient("http://localhost:9000", root="/")
    # tf = TemporaryFile()
    # numpy.save(tf, band5_flat)
    # tf.seek(0)
    # hdfs_client.write(f"/first-stage-output/{year}/band5_flat.npy", tf.read(), overwrite=True)
    # tf = TemporaryFile()
    # numpy.save(tf, band4_flat)
    # tf.seek(0)
    # hdfs_client.write(f"/first-stage-output/{year}/band4_flat.npy", tf.read(), overwrite=True)


