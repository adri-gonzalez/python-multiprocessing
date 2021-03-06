# -*- coding: utf-8 -*-
import io
import random
import sys
from multiprocessing.pool import ThreadPool
import pathlib

import requests
from PIL import Image
import time

start = time.time()


def get_download_location():
    try:
        url_input = sys.argv[1]
    except IndexError:
        print('ERROR: Proporcione el archivo txt \n $python 01_image_downloader.py cats.txt')
    name = url_input.split('.')[0]
    pathlib.Path(name).mkdir(parents=True, exist_ok=True)
    return name


def get_urls():
    """
    Devuelve una lista de direcciones URL al leer el archivo txt proporcionado como argumento en la terminal
    """
    try:
        url_input = sys.argv[1]
    except IndexError:
        print('ERROR: Proporcione el archivo txt \n Example $python 01_image_downloader.py dogs.txt \n\n')
        sys.exit()
    with open(url_input, 'r') as f:
        images_url = f.read().splitlines()

    print('{} Imagenes detectadas'.format(len(images_url)))
    return images_url


def image_downloader(img_url: str):
    """
    Input:
    param: img_url  str (url de la imagen)
    Intenta descargar la URL de la imagen y usar el nombre proporcionado en los encabezados.
    De lo contrario, elige un nombre al azar
    """
    print(f'Descargando: {img_url}')
    res = requests.get(img_url, stream=True)
    count = 1
    while res.status_code != 200 and count <= 5:
        res = requests.get(img_url, stream=True)
        print(f'Reintentando: {count} {img_url}')
        count += 1
    # checking the type for image
    if 'image' not in res.headers.get("content-type", ''):
        print('ERROR: a URL no parece ser la de una imagen')
        return False
    # Trying to red image name from response headers
    try:
        image_name = str(img_url[(img_url.rfind('/')) + 1:])
        if '?' in image_name:
            image_name = image_name[:image_name.find('?')]
    except Exception as ex_:
        print(ex_)
        image_name = str(random.randint(11111, 99999)) + '.jpg'

    i = Image.open(io.BytesIO(res.content))
    download_location = get_download_location()
    i.save(download_location + '/' + image_name)
    return f'Descarga completada: {img_url}'


def run_downloader(process: int, images_url: list):
    """
    Entradas:
        proceso: (int) n??mero de proceso a ejecutar
        images_url: (lista) lista de URL de im??genes
    """
    print(f'MENSAJE: Corriendo proceso {process}')
    results = ThreadPool(process).imap_unordered(image_downloader, images_url)
    for r in results:
        print(r)


try:
    num_process = int(sys.argv[2])
except Exception as ex:
    print(ex)
    num_process = 10

images_url = get_urls()
run_downloader(num_process, images_url)

end = time.time()
print('Tiempo necesario para descargar {}'.format(len(get_urls())))
print(end - start)
