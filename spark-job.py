import json
import findspark
import numpy as np
from PIL import Image
import os
from pyspark.sql import SparkSession
findspark.init()

spark = SparkSession.builder.master("spark://192.168.0.199:7077").appName('Big_data')\
        .config("spark.executor.memory", "6g")\
        .config("spark.driver.memory", "10g")\
        .config("spark.driver.host", "192.168.0.199")\
        .getOrCreate()


def create_ndvi_map(pixel_map, X, Y):
    """ Zamiana wektora NDVI na prostokątną mapę NDVI. """
    make_array = np.asarray(pixel_map)
    data_reshape = np.reshape(make_array, (X, Y), order='F')
    data_rot = np.rot90(data_reshape, k=1, axes=(0, 1))
    data_flip = np.flipud(data_rot)
    return ((data_flip + 1) / 2 * 255).astype("int")


years = [y for y in os.listdir("first-stage-output") if y != "combined"]  # y not in os.listdir("spark-output") and

for i, year in enumerate(years):
    with open(f"first-stage-output/{year}/variables.json") as f:
        variables = json.loads(f.read())

    band5_flat = np.load(f"first-stage-output/{year}/band5_flat.npy")
    band4_flat = np.load(f"first-stage-output/{year}/band4_flat.npy")

    # Spark
    # Przypisanie tablic numpy do RDD
    band5_rdd = spark.sparkContext.parallelize(band5_flat, 50)
    band4_rdd = spark.sparkContext.parallelize(band4_flat, 50)
    # Złączenie list obu wektorów w celu umożliwienia operacji map
    ndvi_rdd = band5_rdd.zip(band4_rdd)
    # Sprawdzanie ilości pixeli, które mają wartość
    ndvi_rdd = ndvi_rdd.map(lambda x: (x[0] - x[1]) / (x[0] + x[1]) if (x[0] + x[1]) != 0 else -1)
    # Konwersja RDD do np.array
    ndvi = ndvi_rdd.collect()
    # Zapis RDD do HDFS
    # ndvi_rdd.saveAsPickleFile(f"hdfs://localhost:9000/spark-output/{year}/ndvi_rdd.pkl")
    # Usuwanie RDD
    band5_rdd.unpersist()
    band4_rdd.unpersist()
    ndvi_rdd.unpersist()
    # Spark

    # Utworzenie prostokątnej mapy NDVI z wektora i zapis
    ndvi_map = create_ndvi_map(ndvi, variables["xsize"], variables["ysize"])
    try:
        os.mkdir(f"spark-output/{year}")
    except FileExistsError as err:
        print(err)
    np.save(f"spark-output/{year}/ndvi.npy", np.array(ndvi_map))

    # Tworzenie miniaturki z mapy NDVI i zapis
    rgb = np.array([
        np.load(f"first-stage-output/{year}/green_band.npy"),
        np.load(f"first-stage-output/{year}/red_band.npy"),
        np.load(f"first-stage-output/{year}/blue_band.npy")
    ])
    rgb = np.swapaxes(rgb, 0, 2)
    rgb = np.rot90(rgb)
    img = Image.fromarray(ndvi_map)
    img = img.resize((rgb.shape[1], rgb.shape[0]))
    ndvi_map = np.array(img)
    np.save(f"spark-output/{year}/ndvi_min.npy", np.array(ndvi_map))

    # Wyznaczenie średniego NDVI i zapis
    ndvi = np.array(ndvi)
    mean_ndvi = sum(ndvi[ndvi > -1])/len(ndvi[ndvi > -1])
    with open(f"spark-output/{year}/mean_ndvi.txt", "w") as f:
        f.write(str(mean_ndvi))

